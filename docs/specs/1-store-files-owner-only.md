---
title: Cache files are owner-only on disk
scope: sidecar · host
status: Planned
---

# Cache files are owner-only on disk

**Needs:** nothing. **Hands on:** [12-decide-what-the-cache-is.md](12-decide-what-the-cache-is.md)
assumes this has landed.

## Goal

Make every SQLite file the sidecar writes readable only by the user who owns it.

Today the mode comes from whatever umask the app inherits, so a cache file can land 0644.
`atomicjson` already gets this right for `host.json`; the databases do not.

**And the directory does not save them.** The sidecar's `os.MkdirAll(dataDir, 0o700)`
(`clustersvc/service.go:405`) is a no-op in a shipped build, because the host has already created
the directory: `std::fs::create_dir_all` in `src-tauri/src/services/sidecar/service.rs:91` takes the
umask, so the directory is 0755. The comment above that `MkdirAll` — "nothing else has made dataDir
yet" — is wrong, and fixing it is part of this spec.

Files in scope: `<data-dir>/app.db` and every `<data-dir>/clusters/<id>.db`, plus each one's
`-wal` and `-shm` siblings.

## How the three layers fit

Three changes, each covering what the others cannot:

- **The umask** (step 2) is what makes a *new* file land 0600 the instant it is created. Nothing
  else closes the window between a create and a later chmod.
- **The chmod** (step 3) is for files an older build already wrote at 0644. It also covers the
  siblings for good: SQLite creates `-wal` and `-shm` with the database file's own mode, so once
  the database is 0600 every sibling it creates afterwards is too.
- **The directory** (step 1) is the outer wall, and the only one Windows has.

## What to build

**1. Create the data directory 0700 on the host side.** In `service.rs`, replace
`std::fs::create_dir_all(&data_dir)` with a Unix arm using
`std::fs::DirBuilder::new().recursive(true).mode(0o700)` (behind `#[cfg(unix)]`, via
`std::os::unix::fs::DirBuilderExt`), and the plain call on Windows, where the per-user
`%LOCALAPPDATA%` ACL already restricts it.

**And fix the directories that already exist.** A mode on `DirBuilder` applies only to directories
it creates, so every current install keeps its 0755 after the upgrade. On Unix, follow the create
with `std::fs::set_permissions(&data_dir, Permissions::from_mode(0o700))` unconditionally.

**Give it a seam.** This sits inside `SidecarService::spawn`, which spawns a real sidecar, so a test
cannot reach it. Extract `fn ensure_data_dir(path: &Path) -> Result<()>` and call that; the test in
the section below tests the function.

Then correct the comment on `MkdirAll` in `clustersvc/service.go` — it is a fallback for a
standalone run, not the first creation.

**2. Set the process umask at startup.** In `sidecar/main.go`, first thing in `main`, call a helper
that sets the umask to `0o077` on Unix and does nothing on Windows: two files beside `main.go`,
split by build tag (`umask_unix.go` with `syscall.Umask(0o077)`, `umask_windows.go` with an empty
body). `ipc.Listen` saves and restores the umask around its own bind, so it is unaffected.

The umask is inherited by every process the sidecar starts, which today means kubeconfig `exec`
credential plugins. A plugin's own cache files land tighter than they otherwise would; nothing
reads them across users, so that is harmless.

**3. Chmod the file after opening it.** In `sqlitemigrate.OpenPool`, after the pool is open, force
the mode on the three paths:

```go
// sql.Open is lazy — the file does not exist until a connection runs. Ping first,
// then fix the mode: a file written by an older build kept the umask it was born with.
```

So: `db.Ping()`, then `os.Chmod` on `path`, `path+"-wal"` and `path+"-shm"`, ignoring
`os.IsNotExist` for the two siblings — on a fresh open they do not exist yet, and SQLite creates
them from the database's mode once that is fixed. On Windows `os.Chmod` cannot set POSIX bits and
is effectively a no-op; the data directory's ACL carries it there.

Do it in `OpenPool` only. `OpenReadPool` opens a file `OpenPool` already created.

## Tests

Beside the code, and skipped on Windows (`runtime.GOOS == "windows"`), the way
`atomicjson_test.go` already does it:

- In `sqlitemigrate`: open a pool in a temp dir, write a row, and assert `path`, `path-wal` and
  `path-shm` are each `0600`.
- In `kubestore`: open a manager in a temp dir with a permissive umask (`syscall.Umask(0o022)` for
  the duration of the test, restored after), sync something into it, and assert the cache file is
  `0600`. The test binary never runs `main`, so this pins step 3 — the chmod — on the real path,
  shaped like `TestListen_IsOwnerOnly`. **`syscall.Umask` is process-global**, so this test must
  not be `t.Parallel()` — nothing in that package is today, and it must stay that way.
- In `main`'s package: the umask helper leaves the process at `0o077` on Unix. This is the only
  test step 2 gets, since the helper is one line.
- In `src-tauri`: `ensure_data_dir` creates a missing directory 0700, and tightens an existing
  0755 one to 0700. Unix only.

## When it lands

In [`docs/security-model.md`](../security-model.md), move the row *"Data directories created 0700;
`host.json` and the sync files written 0600"* from **Held by review** to **Enforced**, naming the
new tests, and drop the "the SQLite files themselves take the umask" caveat. Add `service.rs` to
that row's *Where* column — the directory is created by the host, which the row does not currently
say.
