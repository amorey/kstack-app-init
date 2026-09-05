// Copyright 2026 The Kstack Authors
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

//! Reads the process on the far end of a connected [`Stream`]. The kernel
//! stamps that identity at connect time, so a server cannot claim another's —
//! the same property the sidecar's own listener relies on
//! (`sidecar/internal/ipc/peer_*.go`), pointed the other way.

#[cfg(not(target_vendor = "apple"))]
use interprocess::local_socket::traits::StreamCommon;

use super::ipc::Stream;
use crate::error::{AppError, Result};

/// Reports the pid of the process serving `stream`.
///
/// `interprocess` answers this on Linux (`SO_PEERCRED`) and Windows
/// (`GetNamedPipeServerProcessId`). Darwin is `xucred`-based and its
/// `LOCAL_PEERCRED` carries no pid at all, so macOS asks the socket itself.
#[cfg(not(target_vendor = "apple"))]
pub fn peer_pid(stream: &Stream) -> Result<u32> {
    let creds = stream.peer_creds().map_err(AppError::Io)?;
    let pid = creds.pid().ok_or_else(|| {
        AppError::Io(std::io::Error::other(
            "peer credentials carry no process id",
        ))
    })?;
    // `pid()` is `i32` on Linux and already `u32` on Windows.
    #[allow(clippy::unnecessary_cast)]
    Ok(pid as u32)
}

#[cfg(target_vendor = "apple")]
pub fn peer_pid(stream: &Stream) -> Result<u32> {
    use std::os::fd::AsRawFd;

    let Stream::UdSocket(uds) = stream;
    let fd = uds.inner().as_raw_fd();

    let mut pid: libc::pid_t = 0;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: `fd` is borrowed from a live stream, and the out-params match
    // LOCAL_PEERPID's documented type and width.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            std::ptr::from_mut(&mut pid).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(AppError::Io(std::io::Error::last_os_error()));
    }
    Ok(pid as u32)
}

#[cfg(test)]
mod tests {
    use super::super::ipc::{bind_listener, cleanup_path, Endpoint, Target};
    use std::time::Duration;

    /// The one fact no unit test can fake: the pid the kernel reports for the
    /// far end of a connection is the process that actually bound it. Here
    /// that is the test process itself.
    #[tokio::test]
    async fn peer_pid_reports_the_listening_process() {
        let endpoint = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let _listener = bind_listener(endpoint.as_arg());
        let target = Target::expecting(endpoint.clone(), Some(std::process::id()));
        let stream = target
            .connect_with_budget(Duration::from_secs(1))
            .await
            .expect("connect");

        assert_eq!(
            super::peer_pid(&stream).expect("peer pid"),
            std::process::id()
        );

        cleanup_path(endpoint.as_arg());
    }
}
