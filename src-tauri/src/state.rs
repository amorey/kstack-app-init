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

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::services::sidecar::SidecarService;
use crate::tray::TraySnapshots;
use crate::window_manager::WindowManager;

pub struct AppState {
    pub sidecar: SidecarService,
    pub window_manager: WindowManager,
    /// Latest-state holder for the tray's account watch stream, written by
    /// `spawn_authstate_subscription`.
    pub tray: Arc<Mutex<TraySnapshots>>,
    /// App-wide graceful-shutdown signal, cancelled once on Quit *before* the
    /// sidecar is torn down. Every app-lifetime background task (tray/wake
    /// supervisors, signal handler) must hold a clone and select on
    /// [`CancellationToken::cancelled`] so it doesn't retry against a dying
    /// sidecar.
    pub shutdown: CancellationToken,
}
