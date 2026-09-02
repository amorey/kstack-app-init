# Polyglot task entry point. The Go sidecar lives outside the JS world,
# so it doesn't belong in package.json scripts; this Makefile is the
# place where Go, Rust, and JS commands meet.

.PHONY: sidecar sidecar-dev proto proto-go proto-rust test test-go test-rust test-js lint lint-go lint-rust lint-js vet vet-go vet-rust clean

# Build the Go sidecar into src-tauri/binaries/ with the Tauri-required
# `<name>-<rust-host-triple>` filename. Tauri's externalBin picks it up.
sidecar:
	bash scripts/build-sidecar.sh

# The dev build, and the only one that lets KSTACK_CLOUD_API_URL and friends
# redirect the sidecar. `tauri dev` calls this; every release path calls
# `sidecar` and so builds untagged.
sidecar-dev:
	KSTACK_SIDECAR_TAGS=debug bash scripts/build-sidecar.sh

# Regenerate the gRPC bindings from the shared repo-root proto/ for both
# languages. proto/ is the single source of truth (host <-> sidecar wire
# format); the Go bindings are committed, the Rust ones are produced by
# src-tauri/build.rs at compile time (using a vendored protoc, so plain Rust
# builds need no system install).
proto: proto-go proto-rust

# Go: protoc-gen-go + protoc-gen-go-grpc via the //go:generate directives in
# sidecar/grpc/authpb and sidecar/grpc/pokepb. One-time tooling for regenerating the committed
# Go bindings: install `protoc`, then `go install
# google.golang.org/protobuf/cmd/protoc-gen-go` and `go install
# google.golang.org/grpc/cmd/protoc-gen-go-grpc` (versions pinned in
# sidecar/tools.go). The Rust side needs none of this — its protoc is vendored.
proto-go:
	cd sidecar && go generate ./grpc/...

# Rust: bindings are emitted by src-tauri/build.rs on build; this forces a
# rebuild so any proto/codegen error surfaces from `make proto`.
proto-rust:
	cd src-tauri && cargo build

# Run every test suite in the repo.
test: test-go test-rust test-js

test-go:
	cd sidecar && go test ./...

# Rust integration test spawns the real sidecar binary, so build it first.
test-rust: sidecar
	cd src-tauri && cargo test

test-js:
	pnpm test --run

# Run every linter in the repo.
lint: lint-go lint-rust lint-js

# `gofmt -l` lists unformatted files; non-empty = lint failure.
lint-go:
	@cd sidecar && unformatted=$$(gofmt -l .); \
		if [ -n "$$unformatted" ]; then \
			echo "gofmt: files need formatting:"; echo "$$unformatted"; exit 1; \
		fi

lint-rust:
	cd src-tauri && cargo fmt --check

lint-js:
	pnpm lint

# Static analysis (separate from formatting checks in `lint`).
vet: vet-go vet-rust

vet-go:
	cd sidecar && go vet ./...

vet-rust:
	cd src-tauri && cargo clippy --all-targets -- -D warnings

clean:
	rm -rf src-tauri/binaries
	rm -rf src-tauri/target
	cd sidecar && go clean -testcache
