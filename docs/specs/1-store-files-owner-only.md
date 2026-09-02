---
title: Cache files are owner-only on disk
scope: sidecar
status: Planned
---

# Cache files are owner-only on disk

**Needs:** nothing. **Hands on:** [2-dependency-scanning-in-ci.md](2-dependency-scanning-in-ci.md)
is independent; [12-decide-what-the-cache-is.md](12-decide-what-the-cache-is.md) assumes this has
landed.

## Goal

Make every SQLite file the sidecar writes readable only by the user who owns it.

Today the mode comes from whatever umask the app inherits, so a cache file can land 0644. The only
thing keeping it private is the `0700` on its parent directory — one bit, in one place, protecting
full object bodies for every kind the cluster mirrors. `atomicjson` already gets this right for
`host.json`; the databases do not.

Files in scope: `<data-dir>/app.db` and every `<data-dir>/clusters/<id>.db`, plus each one's
`-wal` and `-shm` siblings.

## What to build

**1. Set the process umask at startup.** In `sidecar/main.go`, before anything opens a file, call a
small helper that sets the umask to `0o077` on Unix and does nothing on Windows. Put it in
`sidecar/internal/ipc`'s neighbour style: a new tiny package or two files in `main`'s directory
split by build tag (`umask_unix.go` with `syscall.Umask(0o077)`, `umask_windows.go` with an empty
body). This is what covers a `-wal` file SQLite recreates later, which no one-time chmod can.

**2. Chmod the file after opening it.** In `sqlitemigrate.OpenPool`, after the pool is open, force
the mode on the three paths:

```go
// sql.Open is lazy — the file does not exist until a connection runs. Ping first,
// then fix the mode: a file written by an older build kept the umask it was born with.
```

So: `db.Ping()`, then `os.Chmod` on `path`, `path+"-wal"` and `path+"-shm"`, ignoring
`os.IsNotExist` for the two siblings. On Windows `os.Chmod` cannot set POSIX bits and is
effectively a no-op — that is fine, the data directory's ACL carries it there.

Do it in `OpenPool` only. `OpenReadPool` opens a file `OpenPool` already created.

## Tests

Beside the code, and skipped on Windows (`runtime.GOOS == "windows"`), the way
`atomicjson_test.go` already does it:

- In `sqlitemigrate`: open a pool in a temp dir, write a row, and assert `path`, `path-wal` and
  `path-shm` are each `0600`.
- In `kubestore`: open a manager in a temp dir with a permissive umask (`syscall.Umask(0o022)` for
  the duration of the test), sync something into it, and assert the cache file is `0600`. This is
  the one that pins the real path, shaped like `TestListen_IsOwnerOnly`.

## When it lands

In [`docs/security-model.md`](../security-model.md), move the row *"Data directories created 0700;
`host.json` and the sync files written 0600"* from **Held by review** to **Enforced**, naming the
new test, and drop the "the SQLite files themselves take the umask" caveat.
