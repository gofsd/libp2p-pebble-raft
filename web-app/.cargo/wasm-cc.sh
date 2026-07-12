#!/bin/sh
# C compiler for wasm32-unknown-unknown, used by sqlite-wasm-rs's build
# script (see web-app/README.md's "Building" section). Prefers a real
# clang with wasm32 support already on PATH; falls back to `zig cc`
# (bundles its own clang) if not, since a plain `zig` install is a much
# smaller/easier one-time download than a full LLVM toolchain and doesn't
# need root -- see README.md for how to get either.
#
# cc-rs always emits the literal Cargo target triple
# (--target=wasm32-unknown-unknown), but zig's clang frontend only
# recognizes its own target names (wasm32-freestanding is the LLVM
# wasm32-unknown-unknown equivalent there), so that flag is rewritten only
# on the zig fallback path -- a real standalone clang accepts
# wasm32-unknown-unknown natively and needs no rewriting.
if command -v clang >/dev/null 2>&1; then
  exec clang "$@"
fi

if command -v zig >/dev/null 2>&1; then
  args=""
  for a in "$@"; do
    case "$a" in
      --target=wasm32-unknown-unknown)
        args="$args --target=wasm32-freestanding"
        ;;
      *)
        args="$args \"$a\""
        ;;
    esac
  done
  eval exec zig cc "$args"
fi

echo "wasm-cc.sh: neither clang nor zig found on PATH -- see web-app/README.md's Building section" >&2
exit 1
