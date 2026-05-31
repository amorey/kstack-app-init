//go:build tools
// +build tools

// Pin codegen tooling here so `go mod tidy` keeps it (and its transitive
// `golang.org/x/tools` deps) in go.sum. Without this, `go run
// github.com/99designs/gqlgen generate` fails on a fresh checkout.
//
// The protoc plugins are pinned too so `make proto` (which runs
// `go generate ./grpc/...` → protoc) resolves the same versions on a fresh
// checkout. Install them with `go install google.golang.org/protobuf/cmd/protoc-gen-go`
// and `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc`.
package tools

import (
	_ "github.com/99designs/gqlgen"
	_ "github.com/99designs/gqlgen/graphql/introspection"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
