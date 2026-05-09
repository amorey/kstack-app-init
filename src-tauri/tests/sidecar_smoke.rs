// Integration test: spawn the real Go sidecar binary, parse its READY
// line, query it over UDS, assert the canned `{ ping }` response.
//
// This is the Rust-side counterpart to sidecar/server/server_test.go —
// it locks in the contract between the Tauri host and the sidecar
// (transport + framing + shutdown), independent of any Tauri runtime.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

fn sidecar_binary() -> PathBuf {
    // `scripts/build-sidecar.sh` puts the binary here under the host triple.
    let triple = std::env::var("TARGET").unwrap_or_else(|_| {
        // Fallback: derive from rustc at test time.
        let out = std::process::Command::new("rustc")
            .arg("-vV")
            .output()
            .expect("rustc -vV");
        let s = String::from_utf8_lossy(&out.stdout);
        s.lines()
            .find_map(|l| l.strip_prefix("host: "))
            .expect("rustc host line")
            .trim()
            .to_string()
    });
    let ext = if triple.contains("windows") {
        ".exe"
    } else {
        ""
    };
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("binaries");
    p.push(format!("kstack-sidecar-{triple}{ext}"));
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn sidecar_answers_ping_over_uds() {
    let bin = sidecar_binary();
    assert!(
        bin.exists(),
        "sidecar binary missing at {bin:?} — run `make sidecar` first"
    );

    let mut child = Command::new(&bin)
        .stdin(Stdio::piped()) // keep stdin open so EOF-shutdown doesn't fire
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sidecar");

    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read READY line");
    let socket = line
        .strip_prefix(kstack_app_lib::sidecar::READY_PREFIX)
        .expect("READY prefix")
        .trim()
        .to_string();

    // Socket access is gated by filesystem perms on Unix. If the chmod
    // call regresses, every other user on the box can speak GraphQL to us.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&socket)
            .expect("stat socket")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "socket should be 0600, got {:o}",
            mode & 0o777
        );
    }

    let body = br#"{"query":"{ ping }"}"#;
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        kstack_app_lib::sidecar::query_uds(socket.as_ref(), body),
    )
    .await
    .expect("timeout")
    .expect("query_uds");

    let got = String::from_utf8_lossy(&resp);
    assert!(got.contains(r#""ping":"pong""#), "unexpected body: {got}");

    // Closing stdin triggers the sidecar's EOF-shutdown path.
    drop(child.stdin.take());
    let _ = child.wait();
}
