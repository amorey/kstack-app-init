---
title: The host authenticates the sidecar
scope: src-tauri, sidecar
status: Planned
---

# The host authenticates the sidecar

**Needs:** nothing. **Hands on:** nothing. Closes R-01 and H-5.

## Goal

Make the trust between host and sidecar mutual. Today it runs one way.

The sidecar checks who is calling it: `ipc.Authenticated` rejects any peer whose uid is not ours
and, when the host supplied a pid, any process that is not the host
(`sidecar/internal/ipc/authlistener.go`). The host checks nothing. `ipc::connect_with_budget`
returns the first connection that succeeds, and the three things built on it — the GraphQL query
client, the SSE subscription reader, the gRPC channel — take it as the sidecar.

The address makes that worth exploiting. `Endpoint::pick(&std::env::temp_dir())` names the socket
`kstack-sidecar-<host pid>-<n>.sock` in the shared temp directory, and the sidecar's `Listen`
removes an existing path before binding. On Windows the name is in the flat `\\.\pipe\` namespace.
So another process of the same user can predict the address, and — with the right timing — be the
thing on the other end.

## The threat, stated plainly

This defends against **another process running as the same user** impersonating the sidecar: seeing
every GraphQL request the app makes, and answering with whatever it likes. It is not a defence
against an attacker who is already inside the host process, and it is not a defence against one who
can simply read the cache files (that is S-3). Saying so keeps the protection from reading as
bigger than it is.

## What is true now

- Host side: `Endpoint::pick` (`src-tauri/src/services/sidecar/ipc.rs`) and **two** dial entry
  points — `ipc::connect` and `ipc::connect_with_budget`. Of the three call sites, only
  `graphql/subscribe.rs:213` uses the budgeted one; `graphql/query.rs:111` and `grpc.rs:147` call
  `ipc::connect`.
- The host holds the child pid in `State.pid` — but **`State.pid` outlives the child**. The drain
  task's `Terminated` arm sets only `exited = true`; `pid` is cleared inside `kill`. Its own comment
  says the pid may be reused, which is exactly the value a naive check would compare against.
- **gRPC re-dials.** `GrpcClient::channel` caches a `Channel` and `reset()` drops it after any
  failure, so a check that runs once at startup would not cover the reconnect.
- Sidecar side: the Unix socket is bound 0600 under a tightened umask; the Windows pipe carries an
  owner-only DACL (`D:P(A;;GA;;;OW)`). Both keep *other users* out. Neither keeps out another
  process of this user, which is the whole gap.
- The host already knows how to make a private directory: `ensure_data_dir` creates 0700 and
  re-tightens, pinned by `ensure_data_dir_creates_and_tightens_to_0700`.

## Settle this first

**Does `interprocess`'s `tokio::Stream` hand us the underlying fd or handle?** Everything below
assumes it does. If it does not, the choice is a small `impl` on our side of the crate boundary or
dropping to `tokio::net::UnixStream` / `tokio::net::windows::named_pipe` directly, and that changes
the shape of `ipc::Stream`. Answer it before writing anything else — it is an afternoon, and it
decides whether this spec is a day or a week.

## Design

### One private directory, created by the host

The endpoint moves out of the shared temp directory into a per-user runtime directory the host owns:

- **Linux:** `$XDG_RUNTIME_DIR/kstack/` when it is set and usable, else a `0700` directory under
  the temp dir, created and re-tightened by the existing `ensure_data_dir`. This is where the gain
  is: `/tmp` is shared and sticky-bit only.
- **macOS:** `$TMPDIR` is already a per-user directory the system creates `0700`, so the change is
  a subdirectory for tidiness, not a security fix. Say so rather than claiming a win.

The socket path must still fit `sun_path`, which `Endpoint::pick` already checks — the check stays,
and matters more now that the base is longer.
- **Windows:** the pipe namespace is flat and there is no directory to make private. The DACL and
  the peer check below are the whole policy there.

The directory is created before spawn and removed on clean shutdown, by the same code that owns the
child. A stale directory from a crashed run is **reused, not refused** — `ensure_data_dir`
re-tightens its mode first — and any socket left inside it is removed at that point, by the host,
before the new sidecar is told to bind. Nothing sweeps another instance's directory: a second window
of the running app shares this one, and a concurrently running copy owns its own.

### The check lives in the dialer — in both of them

**Both entry points take the expected pid**, not just the budgeted one. If only
`connect_with_budget` is checked, `ipc::connect` is the unchecked back door — and two of the three
call sites are already on it.

```rust
/// Dials `endpoint` and refuses any peer that is not `expect_pid`. The kernel
/// supplies the peer's identity, so a server cannot claim another's — the same
/// property the sidecar's own listener relies on, pointed the other way.
pub async fn connect_with_budget(
    endpoint: &Endpoint,
    expect_pid: ExpectedPid,
    budget: Duration,
) -> Result<Stream>
```

Making the pid a required argument is what forces every call site to have one — a defaulted or
optional pid is how the gRPC connector quietly keeps dialing unchecked.

### How the pid reaches the dialers

This is the part that touches three files, so the spec picks the shape rather than leaving it to
the first person to open the code. `QueryClient`, the SSE reader and the gRPC `service_fn` connector
each hold an `Endpoint` and nothing else; none can see `SidecarService`'s state.

**Hand each one a `watch::Receiver<Option<u32>>`** — cheap to clone, `Send + Sync`, already a
tokio dependency, and a read is not a lock the connector can be poisoned by. `spawn` owns the
sender: it publishes `Some(pid)` at spawn and **`None` from the drain task's `Terminated` arm**,
which is the fix to the premise above and belongs in the same commit. `ExpectedPid` is a thin
newtype over the receiver so the call sites read plainly and a test can construct one from a
constant.

`None` means refuse: there is no sidecar, so there is nothing legitimate to connect to, and a pid
the OS has since handed to someone else is precisely what must not be accepted.

**Per platform, the peer's identity comes from the OS:**

| Platform | Call |
| --- | --- |
| Linux | `getsockopt(SO_PEERCRED)` — pid and uid together |
| macOS | `getsockopt(LOCAL_PEERPID)`, plus `LOCAL_PEERCRED` for the uid |
| Windows | `GetNamedPipeServerProcessId` on the pipe handle |

The Windows row has its own version of the question above: the call needs the raw `HANDLE` out of
the named-pipe stream, which `interprocess` may or may not surrender even if it hands over a Unix
fd. Answer both together.

**Fail closed.** A peer we cannot read is refused, exactly as the sidecar refuses one whose
credentials are unavailable. An unreadable peer is not an unusual-but-fine case; it is the case an
attacker would arrange.

## Rules

- **Every connection is checked, not every process.** gRPC re-dials; SSE reconnects.
- **Both dial entry points take the pid.** An unchecked `connect` is the whole hole.
- **Fail closed** on an unreadable peer.
- **The expected pid is read live** from the service state.
- **The endpoint lives in a directory only this user can enter**, on the platforms that have
  directories.

## Build order

Each step is one red/green cycle and one commit.

1. Settle the fd/handle question above.
2. `peer_pid(&Stream) -> Result<u32>` per platform, with a test that connects to a listener in the
   test process and reads back its own pid. This is the piece that cannot be faked in a unit test,
   so it is worth its own commit.
3. `connect_with_budget` takes `expect_pid` and refuses a mismatch. The test is the point of the
   whole spec: bind a listener in the test process, dial it with a *wrong* pid, assert refusal —
   a hostile listener, in a unit test.
4. Publish the pid on a `watch` channel, clear it in the `Terminated` arm, and thread it through
   all three call sites. The test that earns this step is the one asserting a dial after
   `Terminated` is refused rather than compared against a reusable pid.
5. The private runtime directory, with a test that the created directory is 0700 on Unix.

## Not in this pass

- **Authenticating a principal rather than a process.** The host trusts its own process; anyone
  inside it inherits everything. `security-model.md` already says this and it does not change.
- **A cryptographic handshake over the socket.** A shared secret passed at spawn would authenticate
  the *channel* rather than the process, and would need somewhere to live that is not the
  environment or the command line. The kernel already knows the answer; ask it.
- **Windows pipe pre-creation defence beyond the peer check.** `FILE_FLAG_FIRST_PIPE_INSTANCE` on
  the sidecar's listener is worth investigating and belongs to whoever runs the native test; it is
  not blocking the check above.
- **Sandboxing the sidecar.** A different project.

## When it lands

- `security-model.md`: the *Host → sidecar* boundary drops "Authentication is one-way: the host does
  not verify the server PID" and "not necessarily a private directory". A new row —
  *The host refuses any IPC peer that is not the sidecar it spawned* — lands as **Enforced** by the
  hostile-listener test, noting that the Windows path needs a native run.
- The **R-01 / H-5** bullet leaves `TODO.md`; the register's H-5 row is answered by this spec.
- Delete this spec.

## Done when

A test process that binds the endpoint before the sidecar does gets refused by every one of the
three transports, on all three platforms. The app still starts, signs in, and syncs a cluster. On
Linux and macOS the socket sits in a directory no other user can enter.
