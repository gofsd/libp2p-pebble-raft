#!/bin/sh
# Archiver for wasm32-unknown-unknown, paired with wasm-cc.sh -- see that
# script's doc comment.
if command -v llvm-ar >/dev/null 2>&1; then
  exec llvm-ar "$@"
fi

if command -v zig >/dev/null 2>&1; then
  exec zig ar "$@"
fi

echo "wasm-ar.sh: neither llvm-ar nor zig found on PATH -- see web-app/README.md's Building section" >&2
exit 1
