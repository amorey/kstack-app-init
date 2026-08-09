# Tauri + React + Typescript

This template should help get you started developing with Tauri, React and Typescript in Vite.

## Dev Environment

Start sandbox:

```
# Build template image
docker build -f Dockerfile.sbx -t claude-kstack-app .

# Save your locally-built image to a tar
docker image save claude-kstack-app -o .sbx/claude-kstack-app.tar

# Load it into the sandbox runtime's image store
sbx template load .sbx/claude-kstack-app.tar

# Now you can use it as a template
sbx run claude --template claude-kstack-app
```

Start server inside sandbox:

```
sbx exec -it claude-kstack-app bash
```

Port forward:

```
./scripts/expose-dev.sh
```

### Building inside the sandbox (Linux) with a macOS host

The sandbox bind-mounts the repo from the macOS host, so both platforms share
every build-output path. Run this once per sandbox boot to keep the Linux
artifacts off the host's:

```
./scripts/sandbox-dev-setup.sh
```

It points the relocatable outputs (`CARGO_TARGET_DIR`, the pnpm store) at
`.sandbox-linux/` via `/etc/sandbox-persistent.sh`, and bind-mounts
`.sandbox-linux/{node_modules,dist}` over the fixed paths that tooling can't
relocate. A bind mount is sandbox-only: the host's macOS `node_modules` sits
untouched underneath. Go's caches need no handling — they default to `$HOME`,
which isn't shared. Neither does `src-tauri/binaries/`, since the sidecar's
filename carries the target triple, so the two platforms' builds coexist.

The sandbox needs the Linux toolchain the host doesn't have:

```
sudo apt-get install -y build-essential pkg-config libssl-dev \
  libwebkit2gtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev \
  librsvg2-dev libsoup-3.0-dev libjavascriptcoregtk-4.1-dev patchelf
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
npm install -g pnpm@11.0.8   # corepack can't run pnpm 11 on the bundled Node
```

## Recommended IDE Setup

- [VS Code](https://code.visualstudio.com/) + [Tauri](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode) + [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
