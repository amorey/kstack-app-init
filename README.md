## Architecture

- **`src/`** — the React webview (React 19, TanStack Router, urql, Tailwind v4).
- **`src-tauri/`** — the Tauri 2 / Rust native host: windows, tray, menus, and the sidecar's
  lifecycle. It bridges all webview GraphQL over a Unix socket — the webview has no network
  access.
- **`sidecar/`** — a Go binary owning the GraphQL API and all Kubernetes/cloud logic. It
  mirrors each cluster into a per-cluster SQLite cache and streams changes to the UI.
- **`proto/`** — the shared host↔sidecar gRPC contract.

Design rationale lives in [`docs/adr/`](docs/adr/README.md). Working conventions live in the
per-area `CLAUDE.md` files.

## Toolchain

Rust (rustup), Go, Node + `pnpm`. `rust-toolchain.toml` pins the Rust compiler, so rustup
installs and selects it for you on the first `cargo` call — don't override it with `+stable`.
On Linux, Tauri needs the usual native deps:

```
sudo apt-get install -y build-essential pkg-config libssl-dev \
  libwebkit2gtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev \
  librsvg2-dev libsoup-3.0-dev libjavascriptcoregtk-4.1-dev patchelf
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
npm install -g pnpm@11.0.8   # corepack can't run pnpm 11 on the bundled Node
```

## Develop

```
pnpm install
pnpm tauri dev      # full app (or `pnpm dev` for the webview alone)
```

The `Makefile` is the polyglot entry point:

```
make test    # JS + Rust + Go
make lint
make vet
make proto   # regenerate the gRPC bindings after editing proto/
```

If you develop inside a Linux sandbox against a macOS host checkout, run
`./scripts/sandbox-dev-setup.sh` first — see the root `CLAUDE.md` for details.

### Sandbox environment (optional)

```
docker build -f Dockerfile.sbx -t claude-kstack-app .
docker image save claude-kstack-app -o .sbx/claude-kstack-app.tar
sbx template load .sbx/claude-kstack-app.tar
sbx run claude --template claude-kstack-app
sbx exec -it claude-kstack-app bash     # shell inside
./scripts/expose-dev.sh                 # port forward
```

## Recommended IDE setup

[VS Code](https://code.visualstudio.com/) + [Tauri](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode) + [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
