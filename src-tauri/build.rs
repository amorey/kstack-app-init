fn main() {
    // Use the vendored protoc so codegen never depends on a system install —
    // keeps `cargo build` hermetic on clean CI runners and fresh dev machines.
    std::env::set_var(
        "PROTOC",
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc"),
    );

    // Compile the shared repo-root proto/ into Rust gRPC client bindings.
    // `../proto` resolves from this crate root (src-tauri/) up to the repo
    // root, where the single source-of-truth proto lives (also consumed by the
    // Go sidecar). The host is a client only, so skip server codegen.
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &["../proto/auth.proto", "../proto/poke.proto"],
            &["../proto"],
        )
        .expect("compile proto");
    println!("cargo:rerun-if-changed=../proto/auth.proto");
    println!("cargo:rerun-if-changed=../proto/poke.proto");

    tauri_build::build();
}
