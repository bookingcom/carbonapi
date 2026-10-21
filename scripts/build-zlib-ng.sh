#!/bin/sh
set -eu

# zlib-ng 2.3.3. Use its native, namespaced API, never replace system zlib.
revision=12731092979c6d07f42da27da673a9f6c7b13586
checksum=a0d2a5d122c84b56a793a1553a9c3327fb2eb7469bf7a86b79e3c7be5d92e8d6
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
prefix="$root/.build/zlib-ng"
if test -f "$prefix/revision" && test "$(cat "$prefix/revision")" = "$revision" &&
    test -f "$prefix/lib/libz-ng.a" && test -f "$prefix/include/zlib-ng.h"; then
    exit 0
fi

mkdir -p "$root/.build"
work=$(mktemp -d "$root/.build/zlib-ng-build.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM
curl --fail --location --retry 3 --connect-timeout 10 --max-time 120 --output "$work/source.tar.gz" \
    "https://codeload.github.com/zlib-ng/zlib-ng/tar.gz/$revision"
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$work/source.tar.gz" | cut -d ' ' -f 1)
else
    actual=$(shasum -a 256 "$work/source.tar.gz" | cut -d ' ' -f 1)
fi
test "$actual" = "$checksum" || { echo 'zlib-ng source checksum mismatch' >&2; exit 1; }
mkdir "$work/source"
tar -xzf "$work/source.tar.gz" -C "$work/source" --strip-components=1
cd "$work/source"
# Upstream's default build uses runtime CPU dispatch, not -march=native.
./configure --static --prefix="$prefix"
make -j "${ZLIBNG_JOBS:-2}"
make check
make install
cp LICENSE.md "$prefix/LICENSE.md"
printf '%s\n' "$revision" > "$prefix/revision"
