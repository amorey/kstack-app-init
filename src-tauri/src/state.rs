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

use tokio_util::sync::CancellationToken;

use crate::services::sidecar::SidecarService;
use crate::window_manager::WindowManager;

pub struct AppState {
    pub sidecar: SidecarService,
    pub window_manager: WindowManager,
    /// App-wide graceful-shutdown signal. Cancelled once on Quit (see
    /// `lib.rs`'s `RunEvent::ExitRequested`), which is *before* the sidecar is
    /// torn down. The long-lived background tasks spawned at setup — the tray
    /// kube-context supervisor (`tray::spawn_tray_subscription`) and the Unix
    /// signal handler (`lib::spawn_signal_handler`) — select on
    /// [`CancellationToken::cancelled`] so they stop their reconnect/retry
    /// loops and pending sleeps instead of churning against a sidecar that's
    /// going away. Cheap to clone; each task holds its own clone.
    pub shutdown: CancellationToken,
}
