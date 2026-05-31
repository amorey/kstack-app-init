// Package kubecontextpb holds the generated gRPC bindings for the
// KubeContextService defined in the repo-root proto/ directory.
//
// The bindings are produced by protoc; regenerate with `make proto` (or
// `go generate ./grpc/...` from the sidecar module) after editing
// ../../../proto/kubecontext.proto. The generated *.pb.go files are
// committed. Never hand-edit them.
package kubecontextpb

//go:generate protoc --proto_path=../../../proto --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ../../../proto/kubecontext.proto
