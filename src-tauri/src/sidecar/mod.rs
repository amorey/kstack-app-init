// Copyright 2026 The Kubetail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
mod credentials;
mod lifecycle;
mod subscribe;
mod transport;
mod wake_poster;

pub use credentials::spawn_credential_pusher;
pub use lifecycle::{shutdown, spawn};
pub use transport::{query_uds, SidecarError, READY_PREFIX};
pub use wake_poster::spawn_wake_poster;
