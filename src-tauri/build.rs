fn main() {
    // Vendored protoc: keeps `cargo build` hermetic (no system install).
    std::env::set_var(
        "PROTOC",
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc"),
    );

    // Repo-root proto/ is the shared source of truth (also compiled for the Go
    // sidecar). The host is a client only, so skip server codegen.
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
