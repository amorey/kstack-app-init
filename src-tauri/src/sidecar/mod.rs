//! Everything sidecar-related: the wire transport, the process lifecycle,
//! and the Tauri command that bridges the webview to the running binary.
//!
//! Public surface is intentionally small — `spawn`/`shutdown` for the host
//! lifecycle, `graphql_query` for the invoke handler, and the bits of the
//! transport that integration tests need.

// `command` is `pub` because `tauri::generate_handler!` resolves the
// command path against this module tree and needs to reach the
// macro-generated `__cmd__graphql_query` helper next to the function.
pub mod command;
mod lifecycle;
mod transport;

pub use lifecycle::{shutdown, spawn};
pub use transport::{query_uds, SidecarError, READY_PREFIX};
