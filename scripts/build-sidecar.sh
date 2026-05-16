#!/usr/bin/env bash
# Builds the Go sidecar and places it in src-tauri/binaries/ with the
# Tauri-required `<basename>-<target-triple>` filename so externalBin
# can pick it up.
#
# Usage: scripts/build-sidecar.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SIDECAR_DIR="$ROOT/sidecar"
OUT_DIR="$ROOT/src-tauri/binaries"
BASENAME="kstack-sidecar"

# Tauri uses the *Rust* host triple, not the Go one. Derive it from rustc,
# or take it from KSTACK_HOST_TRIPLE so CI can skip installing Rust here.
TRIPLE="${KSTACK_HOST_TRIPLE:-$(rustc -vV | sed -n 's/^host: //p')}"
EXT=""
case "$TRIPLE" in
  *windows*) EXT=".exe" ;;
esac

mkdir -p "$OUT_DIR"
OUT="$OUT_DIR/$BASENAME-$TRIPLE$EXT"

echo "→ building $OUT"
(cd "$SIDECAR_DIR" && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o "$OUT" .)
echo "✓ $OUT"
