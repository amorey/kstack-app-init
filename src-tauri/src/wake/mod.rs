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

//! Host-side driver that fires the sidecar's `Poke` on OS wake-from-sleep and
//! network-return, so long-lived connections resync promptly instead of waiting
//! on the sidecar's own ~30s wall-clock backstop.
//!
//! - [`core`] is the platform-agnostic, unit-tested heart: edge detection +
//!   trailing-edge debounce ([`core::classify`], [`core::run_coalescer`]).
//! - `supervisor` wires it to Tauri: it owns the event channel, spawns the
//!   per-platform native sources, runs the coalescer, and calls
//!   `SidecarService::poke` on each emitted poke.
//! - `macos` / `windows` / `linux` are the thin, `#[cfg]`-gated native sources
//!   that translate OS callbacks into [`core::RawEvent`]s.

mod core;
mod supervisor;

// Platform-native event sources. Each module exposes the same
// `spawn_sources(tx, shutdown)` entry point; `platform` aliases the one for the
// current target so `supervisor` stays platform-agnostic.
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos as platform;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as platform;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;

// Other targets (e.g. the BSDs) get no native sources — the coalescer simply
// never fires, and the sidecar's wall-clock detector remains the backstop.
#[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
mod platform {
    use tauri::AppHandle;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    use super::core::RawEvent;

    pub fn spawn_sources(_app: &AppHandle, _tx: Sender<RawEvent>, _shutdown: CancellationToken) {}
}

pub use supervisor::spawn_wake_poke_supervisor;
