//go:build tools
// +build tools

// Pin codegen tooling here so `go mod tidy` keeps it (and its transitive
// `golang.org/x/tools` deps) in go.sum. Without this, `go run
// github.com/99designs/gqlgen generate` fails on a fresh checkout.
package tools

import (
	_ "github.com/99designs/gqlgen"
	_ "github.com/99designs/gqlgen/graphql/introspection"
)
