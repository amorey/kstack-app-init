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

//! gRPC client the host uses for the sidecar's host-internal control channel
//! (today: the kube-context watch + set that drives the tray).
//!
//! gRPC needs HTTP/2; GraphQL stays HTTP/1.1. Both share the one socket via
//! **h2c** — the sidecar's `NewH2CHandler` routes HTTP/2 `application/grpc`
//! requests to its gRPC server and everything else to GraphQL. tonic dials our
//! cross-platform [`ipc::Stream`] (UDS / named pipe, *not* TCP) through a
//! custom [`tower::service_fn`] connector, speaking HTTP/2 with prior
//! knowledge (no TLS/ALPN — the socket is already user-restricted). The
//! connector reuses [`ipc::connect`], so it inherits the same capped-backoff
//! dial retry the GraphQL client gets.
//!
//! Unlike the per-call HTTP/1 GraphQL client, a tonic [`Channel`] is cheap to
//! clone and multiplexes many RPCs over one h2 connection, so [`GrpcClient`]
//! holds a lazily-established channel and re-dials only if it's lost.

use hyper_util::rt::TokioIo;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint as TonicEndpoint, Uri};
use tower::service_fn;

use super::ipc::{self, Endpoint};
use crate::error::{AppError, Result};

// The generated bindings for proto/kubecontext.proto (see build.rs). The
// module name matches the proto `package`.
pub mod kubecontext {
    tonic::include_proto!("kubecontext");
}

use kubecontext::kube_context_service_client::KubeContextServiceClient;
pub use kubecontext::KubeContextState;
use kubecontext::{SetCurrentContextRequest, WatchRequest};

/// A server-streamed `Watch` response: each item is a full kube-context
/// snapshot, or a transport error that ends the stream.
pub type WatchStream = tonic::Streaming<KubeContextState>;

/// Holds the sidecar's gRPC channel, dialing lazily on first use and re-dialing
/// if the connection was lost (e.g. a sidecar restart). Construct via
/// [`GrpcClient::new`]; reach the RPCs through [`SidecarService`](super::SidecarService).
pub struct GrpcClient {
    endpoint: Endpoint,
    // Cached across calls; `None` until the first successful dial. Guarded so
    // concurrent callers share one channel instead of each opening their own.
    channel: Mutex<Option<Channel>>,
}

impl GrpcClient {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            channel: Mutex::new(None),
        }
    }

    /// Returns a cloned, ready channel — reusing the cached one when present,
    /// otherwise dialing a fresh connection over the IPC socket.
    async fn channel(&self) -> Result<Channel> {
        let mut guard = self.channel.lock().await;
        if let Some(ch) = guard.as_ref() {
            return Ok(ch.clone());
        }
        let ch = connect(self.endpoint.clone()).await?;
        *guard = Some(ch.clone());
        Ok(ch)
    }

    /// Drops the cached channel so the next call re-dials. Called when an RPC
    /// fails at the transport level — the h2 connection is likely dead (sidecar
    /// restart) and a stale `Channel` would keep failing.
    async fn reset(&self) {
        *self.channel.lock().await = None;
    }

    /// Persists `name` as the kubeconfig current-context (unary RPC). The
    /// change is observed by any active [`Self::watch`] stream and by the
    /// webview's GraphQL subscription (shared watcher).
    pub async fn set_current_context(&self, name: String) -> Result<()> {
        let mut client = KubeContextServiceClient::new(self.channel().await?);
        match client
            .set_current_context(SetCurrentContextRequest { name })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) => {
                self.reset().await;
                Err(status_to_err(status))
            }
        }
    }

    /// Opens the kube-context watch stream: a snapshot first, then a fresh
    /// snapshot on every kubeconfig change. The caller drives it with
    /// `stream.message().await`; a returned error / `None` ends the stream and
    /// the cached channel is reset so the next attempt re-dials.
    pub async fn watch(&self) -> Result<WatchStream> {
        let mut client = KubeContextServiceClient::new(self.channel().await?);
        match client.watch(WatchRequest {}).await {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) => {
                self.reset().await;
                Err(status_to_err(status))
            }
        }
    }
}

/// Dials the sidecar over its IPC socket and completes the HTTP/2 (h2c)
/// handshake, returning a multiplexing [`Channel`].
///
/// The `http://` origin is a placeholder: tonic needs a syntactically valid
/// URI to form the `:authority` pseudo-header, but the connector ignores it and
/// dials the real socket via [`ipc::connect`].
async fn connect(endpoint: Endpoint) -> Result<Channel> {
    TonicEndpoint::from_static("http://kstack.local")
        .connect_with_connector(service_fn(move |_: Uri| {
            let endpoint = endpoint.clone();
            async move {
                let stream = ipc::connect(&endpoint)
                    .await
                    .map_err(|err| std::io::Error::other(err.to_string()))?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|err| AppError::Io(std::io::Error::other(err.to_string())))
}

/// Maps a gRPC `Status` to the host's `AppError::Io`, matching how the GraphQL
/// transport surfaces failures across the command boundary. The status code +
/// message are preserved in the `Io` error's text for diagnostics.
fn status_to_err(status: tonic::Status) -> AppError {
    AppError::Io(std::io::Error::other(format!(
        "grpc {}: {}",
        status.code(),
        status.message()
    )))
}
