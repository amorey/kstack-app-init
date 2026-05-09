# Polyglot task entry point. The Go sidecar lives outside the JS world,
# so it doesn't belong in package.json scripts; this Makefile is the
# place where Go, Rust, and JS commands meet.

.PHONY: sidecar test test-go test-rust test-js lint lint-go lint-rust lint-js vet vet-go vet-rust clean

# Build the Go sidecar into src-tauri/binaries/ with the Tauri-required
# `<name>-<rust-host-triple>` filename. Tauri's externalBin picks it up.
sidecar:
	bash scripts/build-sidecar.sh

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
