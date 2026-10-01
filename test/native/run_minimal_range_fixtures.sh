#!/usr/bin/env bash
set -euo pipefail
plugin_root="$(cd "$(dirname "$0")/../.." && pwd)"
fixture_build_dir="$(mktemp -d /private/tmp/minimal-range-build.XXXXXX)"
trap 'rm -rf "$fixture_build_dir"' EXIT
swiftc -swift-version 5 -module-cache-path "$fixture_build_dir/module-cache" \
  "$plugin_root/ios/background_downloader/Sources/background_downloader/Task.swift" \
  "$plugin_root/ios/background_downloader/Sources/background_downloader/Auth.swift" \
  "$plugin_root/ios/background_downloader/Sources/background_downloader/RangeDownloader.swift" \
  "$plugin_root/test/native/minimal_range_fixtures.swift" -o "$fixture_build_dir/fixtures"
"$fixture_build_dir/fixtures"
