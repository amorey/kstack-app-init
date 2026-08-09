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

//! gRPC client for the sidecar's host-internal control channel (auth-state
//! watch + login/logout, resync poke). Shares the one socket with GraphQL via
//! h2c — tonic dials [`ipc::Stream`] with HTTP/2 prior knowledge (no TLS/ALPN;
//! the socket is user-restricted) through a `tower::service_fn` connector that
//! reuses [`ipc::connect`]'s retry. One cached [`Channel`], re-dialed on loss.
//! See docs/adr/2026-08-09-single-socket-h2c.md.

use hyper_util::rt::TokioIo;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint as TonicEndpoint, Uri};
use tower::service_fn;

use super::ipc::{self, Endpoint};
use crate::error::{AppError, Result};

// Generated bindings for proto/auth.proto (see build.rs); module name matches
// the proto `package`.
pub mod auth {
    tonic::include_proto!("auth");
}

// Generated bindings for proto/poke.proto.
pub mod poke {
    tonic::include_proto!("poke");
}

use auth::auth_service_client::AuthServiceClient;
pub use auth::AuthState;
#[cfg(test)]
pub use auth::Identity;
use auth::{AuthStateWatchRequest, LogoutRequest, StartLoginRequest};

use poke::poke_service_client::PokeServiceClient;
use poke::PokeRequest;

/// Server-streamed `AuthStateWatch`: each item a full snapshot, or a transport
/// error ending the stream.
pub type AuthStateStream = tonic::Streaming<AuthState>;

/// Lazily-dialed, cached gRPC channel; re-dials if lost (sidecar restart).
/// Reach the RPCs through [`SidecarService`](super::SidecarService).
pub struct GrpcClient {
    endpoint: Endpoint,
    // Guarded so concurrent callers share one channel.
    channel: Mutex<Option<Channel>>,
}

impl GrpcClient {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            channel: Mutex::new(None),
        }
    }

    /// Cached channel, or a fresh dial over the IPC socket.
    async fn channel(&self) -> Result<Channel> {
        let mut guard = self.channel.lock().await;
        if let Some(ch) = guard.as_ref() {
            return Ok(ch.clone());
        }
        let ch = connect(self.endpoint.clone()).await?;
        *guard = Some(ch.clone());
        Ok(ch)
    }

    /// Drops the cached channel so the next call re-dials — a stale `Channel`
    /// after a transport failure would keep failing.
    async fn reset(&self) {
        *self.channel.lock().await = None;
    }

    /// Runs the sidecar's synchronous login setup (loopback bind + browser
    /// open); the async sign-in tail reports via [`Self::watch_auth_state`].
    pub async fn start_login(&self) -> Result<()> {
        let mut client = AuthServiceClient::new(self.channel().await?);
        match client.start_login(StartLoginRequest {}).await {
            Ok(_) => Ok(()),
            Err(status) => {
                self.reset().await;
                Err(status_to_err(status))
            }
        }
    }

    /// Clears local credentials, revokes the refresh token (fire-and-forget).
    pub async fn logout(&self) -> Result<()> {
        let mut client = AuthServiceClient::new(self.channel().await?);
        match client.logout(LogoutRequest {}).await {
            Ok(_) => Ok(()),
            Err(status) => {
                self.reset().await;
                Err(status_to_err(status))
            }
        }
    }

    /// Best-effort resync nudge (unary), driven by [`crate::wake`]. See
    /// docs/adr/2026-08-09-poke-resync-fanout.md.
    pub async fn poke(&self) -> Result<()> {
        let mut client = PokeServiceClient::new(self.channel().await?);
        match client.poke(PokeRequest {}).await {
            Ok(_) => Ok(()),
            Err(status) => {
                self.reset().await;
                Err(status_to_err(status))
            }
        }
    }

    /// Opens the auth-state watch stream (current snapshot first, then one per
    /// session change); an error resets the cached channel.
    pub async fn watch_auth_state(&self) -> Result<AuthStateStream> {
        let mut client = AuthServiceClient::new(self.channel().await?);
        match client.auth_state_watch(AuthStateWatchRequest {}).await {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) => {
                self.reset().await;
                Err(status_to_err(status))
            }
        }
    }
}

/// Dials the IPC socket and completes the h2c handshake. The `http://` origin
/// is a placeholder — tonic needs a valid URI for `:authority`, but the
/// connector ignores it and dials via [`ipc::connect`].
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

/// Maps a gRPC `Status` to `AppError::Io` (matching the GraphQL transport's
/// error shape), preserving code + message in the text.
fn status_to_err(status: tonic::Status) -> AppError {
    AppError::Io(std::io::Error::other(format!(
        "grpc {}: {}",
        status.code(),
        status.message()
    )))
}
