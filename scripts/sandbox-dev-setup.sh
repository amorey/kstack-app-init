#!/usr/bin/env bash
# Set up a Linux-native dev environment for kstack-app inside the sandbox,
# without letting its build artifacts collide with the macOS host's.
#
# The repo is bind-mounted from the macOS host, so every build output path is
# shared. Two mechanisms keep the two platforms apart:
#
#   1. Configurable output paths (Cargo, Go, pnpm store) are pointed at
#      $BUILD_ROOT via environment variables.
#   2. Fixed paths that tooling can't relocate (node_modules, dist) get a
#      sandbox-only bind mount from $BUILD_ROOT on top of them. A bind mount is
#      invisible to the host: macOS keeps its own node_modules underneath,
#      untouched, while the sandbox sees the Linux one.
#
# BUILD_ROOT lives *inside* the repo (on the host's disk) so artifacts survive
# sandbox restarts and aren't capped by the 20G overlay. Set
# KSTACK_SANDBOX_BUILD_ROOT=/home/agent/kstack-build to use the overlay instead
# (faster I/O, smaller, wiped on sandbox recreate).
#
# Cargo is the exception: its target/ goes on the overlay ($CARGO_ROOT), because
# the host mount's directory metadata is not coherent enough for it. Cargo gets
# EEXIST from mkdir for directories it has just created and reads fingerprint
# stamps back missing, so a build there never converges. Nothing else the
# sandbox builds touches the tree densely enough to hit it.
#
# Idempotent — safe to re-run. Run it once per sandbox boot: bind mounts do not
# survive a restart, the env vars (in /etc/sandbox-persistent.sh) do.

set -euo pipefail

# No-op outside the sandbox. This runs from a SessionStart hook, and the repo is
# shared with the macOS host, so the host's own agents fire this same hook —
# where every redirection here would be wrong. $SANDBOX_VM_ID is set only inside
# the sandbox; the Linux check guards against a stray export.
if [ -z "${SANDBOX_VM_ID:-}" ] || [ "$(uname -s)" != "Linux" ]; then
  exit 0
fi

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_ROOT="${KSTACK_SANDBOX_BUILD_ROOT:-$REPO/.sandbox-linux}"
CARGO_ROOT="${KSTACK_SANDBOX_CARGO_ROOT:-/home/agent/kstack-build}"
PERSIST="/etc/sandbox-persistent.sh"

echo "→ build root: $BUILD_ROOT"
echo "→ cargo root: $CARGO_ROOT"
# Go's caches are deliberately absent here: they default to $HOME, which is
# already sandbox-local (not shared with the host), so they need no
# redirection. Keeping them out of the repo also keeps the module cache's
# vendored *.ts fixtures out of vitest's file discovery, which — unlike
# eslint — does not honor .gitignore.
mkdir -p "$BUILD_ROOT"/{node_modules,dist,pnpm-store}
mkdir -p "$CARGO_ROOT/target"

# --- 1. bind mounts for paths the tooling can't relocate -------------------
for dir in node_modules dist; do
  target="$REPO/$dir"
  mkdir -p "$target"
  if mountpoint -q "$target"; then
    echo "✓ $dir already bind-mounted"
  else
    sudo mount --bind "$BUILD_ROOT/$dir" "$target"
    echo "✓ $dir bind-mounted from $BUILD_ROOT/$dir"
  fi
done

# --- 2. env vars for the paths that are configurable -----------------------
# Written to the persistent env file, which is sourced before every command.
# Every line this script owns is dropped first, so a re-run rewrites the values
# rather than leaving an older copy above them.
#
# pnpm takes settings from PNPM_CONFIG_* or pnpm-workspace.yaml, and the yaml is
# committed — shared with the host, so it cannot hold a Linux path. Unset, the
# store defaults to the root of node_modules' filesystem: $REPO/.pnpm-store, the
# same path the host's macOS pnpm picks, mixing both platforms' native builds.
sudo touch "$PERSIST"
sudo sed -i \
  -e '/KSTACK_SANDBOX_ENV/d' \
  -e '/^export CARGO_TARGET_DIR=/d' \
  -e '/^export PNPM_CONFIG_STORE_DIR=/d' \
  -e '/^export PATH=.*\.cargo\/bin/d' \
  "$PERSIST"
sudo tee -a "$PERSIST" >/dev/null <<EOF
# KSTACK_SANDBOX_ENV — Linux build outputs, kept out of the macOS host's paths.
export CARGO_TARGET_DIR="$CARGO_ROOT/target"
export PNPM_CONFIG_STORE_DIR="$BUILD_ROOT/pnpm-store"
export PATH="\$HOME/.cargo/bin:\$HOME/.local/share/pnpm:\$PATH"
EOF
echo "✓ env vars written to $PERSIST"

echo
echo "Done. Open a new shell (or 'source $PERSIST') to pick up the env vars."
