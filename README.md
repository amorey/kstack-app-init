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

## Recommended IDE Setup

- [VS Code](https://code.visualstudio.com/) + [Tauri](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode) + [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
