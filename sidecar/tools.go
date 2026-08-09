//go:build tools
// +build tools

// Pins codegen tooling (gqlgen, protoc plugins) in go.sum so `gqlgen generate`
// and `make proto` work on a fresh checkout.
package tools

import (
	_ "github.com/99designs/gqlgen"
	_ "github.com/99designs/gqlgen/graphql/introspection"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
