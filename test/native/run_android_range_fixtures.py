#!/usr/bin/env python3
"""Run the real Android plugin in a disposable Flutter application.

The runner never starts an emulator, changes an AVD, or contacts a provider.
Pass an explicitly selected test Android device. APK production includes the
workspace's final native-artifact gate before Flutter is allowed to install it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import zipfile


WORKSPACE = Path(__file__).resolve().parents[4]
PLUGIN = WORKSPACE / "third_party/background_downloader"
TEST = Path(__file__).with_name("android_range_resume_test.dart")
PACKAGE = "app.iptv.tests.android_range_fixtures"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def production_hashes() -> dict[str, str]:
    paths = sorted((PLUGIN / "android/src/main").rglob("*.kt"))
    paths.extend(sorted((PLUGIN / "lib").rglob("*.dart")))
    return {str(path.relative_to(PLUGIN)): digest(path) for path in paths}


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def extract_journal(artifacts: Path) -> int:
    # Flutter test removes its temporary package at completion, so ADB may no
    # longer be able to read the app's journal. Every journal entry is also sent
    # in full over the test protocol. Retain the raw logs and extract that copy.
    marker = "ANDROID_RANGE_FIXTURE "
    lines = []
    for line in (artifacts / "connected-tests.log").read_text().splitlines():
        position = line.find(marker)
        if position < 0:
            continue
        value = line[position + len(marker):]
        json.loads(value)  # Refuse a truncated event instead of silently losing it.
        lines.append(value)
    (artifacts / "fixture-journal.jsonl").write_text("\n".join(lines) + "\n")
    return len(lines)


def command(
    arguments: list[str],
    *,
    cwd: Path,
    log: Path,
    environment: dict[str, str],
    timeout: int = 1200,
) -> subprocess.CompletedProcess:
    print(f"Running {arguments[0]} {arguments[1]} — {log}", flush=True)
    with log.open("w") as output:
        output.write(json.dumps({"command": arguments, "cwd": str(cwd)}) + "\n")
        output.flush()
        result = subprocess.run(
            arguments,
            cwd=cwd,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}); full log: {log}")
    return result


def prepare(host: Path, artifacts: Path, environment: dict[str, str]) -> None:
    if host.exists():
        raise RuntimeError(f"Refusing to overwrite existing host: {host}")
    command(
        [
            "fvm", "flutter", "create", "--empty", "--platforms=android",
            "--project-name", "android_range_fixtures", "--org", "app.iptv.tests",
            str(host),
        ],
        cwd=WORKSPACE,
        log=artifacts / "host-create.log",
        environment=environment,
    )
    shutil.copyfile(WORKSPACE / ".fvmrc", host / ".fvmrc")
    (host / "pubspec.yaml").write_text(
        "name: android_range_fixtures\n"
        "publish_to: none\n"
        "version: 1.0.0+1\n"
        "environment:\n  sdk: '>=3.8.0 <4.0.0'\n"
        "dependencies:\n"
        "  flutter:\n    sdk: flutter\n"
        f"  background_downloader:\n    path: {json.dumps(str(PLUGIN))}\n"
        "  path_provider: ^2.0.2\n"
        "  crypto: ^3.0.0\n"
        "dev_dependencies:\n"
        "  flutter_test:\n    sdk: flutter\n"
        "  integration_test:\n    sdk: flutter\n"
        "flutter:\n  uses-material-design: true\n"
    )
    # The generated lints dependency is irrelevant to this standalone host.
    (host / "analysis_options.yaml").write_text(
        "analyzer:\n  exclude:\n    - build/**\n"
    )
    (host / "integration_test").mkdir()
    shutil.copyfile(TEST, host / "integration_test" / TEST.name)
    generated_test = host / "test"
    if generated_test.exists():
        shutil.rmtree(generated_test)
    manifest = host / "android/app/src/main/AndroidManifest.xml"
    xml = manifest.read_text()
    xml = xml.replace(
        '<application\n', '<application\n        android:usesCleartextTraffic="true"\n'
    )
    xml = xml.replace(
        '    <application',
        '    <uses-permission android:name="android.permission.INTERNET" />\n'
        '    <application',
        1,
    )
    manifest.write_text(xml)
    build = host / "android/app/build.gradle.kts"
    text = build.read_text().replace("minSdk = flutter.minSdkVersion", "minSdk = 23")
    gate = WORKSPACE / "scripts/validate_android_native.gradle"
    text += f"\n// Validate final packaged native bytes before installation.\napply(from = {json.dumps(str(gate))})\n"
    build.write_text(text)
    # Flutter invokes the same JDK for pub integration/build, and Gradle records it.
    gradle_properties = host / "android/gradle.properties"
    with gradle_properties.open("a") as properties:
        properties.write("\norg.gradle.java.home=" + environment["JAVA_HOME"] + "\n")
    command(
        ["fvm", "flutter", "pub", "get", "--offline"],
        cwd=host,
        log=artifacts / "host-pub-get.log",
        environment=environment,
    )
    command(
        ["fvm", "dart", "analyze", "integration_test/android_range_resume_test.dart"],
        cwd=host,
        log=artifacts / "test-analyze.log",
        environment=environment,
    )


def apk_proof(apk: Path) -> dict[str, object]:
    # The required Gradle gate has already validated every library's ELF ABI.
    # Keep fingerprints of the exact packaged libraries for this test run.
    with zipfile.ZipFile(apk) as archive:
        libraries = {
            entry.filename: {
                "size": entry.file_size,
                "sha256": hashlib.sha256(archive.read(entry)).hexdigest(),
            }
            for entry in archive.infolist()
            if re.fullmatch(r"lib/[^/]+/[^/]+\.so", entry.filename)
        }
    if not libraries:
        raise RuntimeError(f"No native libraries in {apk}")
    if "lib/arm64-v8a/libflutter.so" not in libraries:
        raise RuntimeError(f"Missing Android arm64 Flutter runtime in {apk}")
    return {"apk": str(apk), "sha256": digest(apk), "libraries": libraries}


def run_tests(
    host: Path, artifacts: Path, device: str, environment: dict[str, str]
) -> None:
    sdk = Path(environment["ANDROID_HOME"])
    adb = sdk / "platform-tools/adb"
    before = production_hashes()
    write_json(artifacts / "source-before.json", before)
    command(
        [str(adb), "-s", device, "shell", "getprop"],
        cwd=host,
        log=artifacts / "android-device.log",
        environment=environment,
    )
    command(
        [
            "fvm", "flutter", "build", "apk", "--debug", "--no-pub",
            "--target-platform", "android-arm64",
            "--target", "integration_test/android_range_resume_test.dart", "--verbose",
        ],
        cwd=host,
        log=artifacts / "apk-build.log",
        environment=environment,
    )
    build_log = (artifacts / "apk-build.log").read_text()
    if not re.search(r"Validated \d+ Android native libraries in .*\.apk", build_log):
        raise RuntimeError("Build completed without a successful final native gate")
    apk = host / "build/app/outputs/flutter-apk/app-debug.apk"
    write_json(artifacts / "apk-before-install.json", apk_proof(apk))
    apksigner = sorted((sdk / "build-tools").glob("*/apksigner"))[-1]
    command(
        [str(apksigner), "verify", "--verbose", "--print-certs", str(apk)],
        cwd=host,
        log=artifacts / "apk-signature.log",
        environment=environment,
    )
    if before != production_hashes():
        raise RuntimeError("Plugin production sources changed during the APK build")
    with (artifacts / "logcat.log").open("w") as output:
        logcat = subprocess.Popen(
            [str(adb), "-s", device, "logcat", "-v", "threadtime"],
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            command(
                [
                    "fvm", "flutter", "test", "--no-pub", "--verbose", "-d", device,
                    "integration_test/android_range_resume_test.dart", "--reporter", "expanded",
                    "--file-reporter", f"json:{artifacts / 'flutter-test-events.jsonl'}",
                ],
                cwd=host,
                log=artifacts / "connected-tests.log",
                environment=environment,
            )
        finally:
            logcat.terminate()
            try:
                logcat.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logcat.kill()
                logcat.wait(timeout=10)
            try:
                command(
                    [
                        str(adb), "-s", device, "exec-out", "run-as", PACKAGE,
                        "cat", "files/android-range-fixtures.jsonl",
                    ],
                    cwd=host,
                    log=artifacts / "device-journal-fetch.log",
                    environment=environment,
                    timeout=30,
                )
            except RuntimeError as error:
                print(str(error), file=sys.stderr)
            journal_events = extract_journal(artifacts)
    after = production_hashes()
    write_json(artifacts / "source-after.json", after)
    if before != after:
        raise RuntimeError("Plugin production sources changed during connected tests")
    write_json(artifacts / "apk-after-tests.json", apk_proof(apk))
    events = [
        json.loads(line)
        for line in (artifacts / "flutter-test-events.jsonl").read_text().splitlines()
    ]
    finished = [
        event for event in events
        if event.get("type") == "testDone" and not event.get("hidden", False)
    ]
    summary = {
        "device": device,
        "package": PACKAGE,
        "host": str(host),
        "nativeGatePassed": True,
        "sourcesUnchanged": True,
        "journalEvents": journal_events,
        "passedTestEvents": sum(event.get("result") == "success" for event in finished),
        "failedTestEvents": sum(event.get("result") != "success" for event in finished),
        "completed": events[-1] if events else None,
    }
    write_json(artifacts / "summary.json", summary)
    print(json.dumps(summary, indent=2), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", help="Explicit disposable Android device, e.g. emulator-5580")
    parser.add_argument("--prepare-only", action="store_true", help="Create/analyze host without building/installing")
    parser.add_argument("--prepared-host", type=Path, help="Use a previously prepared host")
    parser.add_argument(
        "--artifacts", type=Path,
        default=Path("/private/tmp") / f"android-range-fixtures-{time.strftime('%Y%m%d-%H%M%S')}",
    )
    arguments = parser.parse_args()
    if not arguments.prepare_only and not arguments.device:
        parser.error("--device is required; the runner never selects or starts an emulator")
    artifacts = arguments.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    flutter_settings = Path.home() / ".config/flutter/settings"
    configured_jdk = json.loads(flutter_settings.read_text()).get("jdk-dir")
    if not configured_jdk:
        raise RuntimeError("Flutter must have its existing JDK 21 configured")
    environment["JAVA_HOME"] = configured_jdk
    version = subprocess.run(
        [str(Path(environment["JAVA_HOME"]) / "bin/java"), "-version"],
        capture_output=True, text=True, check=True,
    )
    (artifacts / "jdk-version.log").write_text(version.stdout + version.stderr)
    if not re.search(r'version "21[.\"]', version.stderr + version.stdout):
        raise RuntimeError("The Android fixture host requires the existing JDK 21")
    environment.setdefault("ANDROID_HOME", str(Path.home() / "Library/Android/sdk"))
    host = arguments.prepared_host.resolve() if arguments.prepared_host else artifacts / "host"
    if arguments.prepared_host:
        if not host.is_dir():
            raise RuntimeError(f"Prepared host missing: {host}")
        shutil.copyfile(TEST, host / "integration_test" / TEST.name)
        properties = host / "android/gradle.properties"
        text = re.sub(r"^org\.gradle\.java\.home=.*$", "", properties.read_text(), flags=re.MULTILINE)
        properties.write_text(text + "\norg.gradle.java.home=" + environment["JAVA_HOME"] + "\n")
        command(
            ["fvm", "dart", "analyze", "integration_test/android_range_resume_test.dart"],
            cwd=host,
            log=artifacts / "test-analyze.log",
            environment=environment,
        )
    else:
        prepare(host, artifacts, environment)
    write_json(artifacts / "host-context.json", {
        "workspace": str(WORKSPACE), "plugin": str(PLUGIN), "host": str(host),
        "package": PACKAGE, "sdk": "Flutter 3.41.9", "testSha256": digest(TEST),
    })
    if arguments.prepare_only:
        print(f"Prepared {host}. No APK built and no app installed.", flush=True)
    else:
        run_tests(host, artifacts, arguments.device, environment)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
