# Repository Locking

How REQ-MAINTENANCE-004 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md)'s
cross-process "only one repository-mutating operation at a time" is actually enforced.

## DESIGN-MAINTENANCE-001: An OS advisory lock file in `meta/`, held for the whole session
Status: implemented (crates/db/src/lock.rs)

A process that wants to mutate a repository (a read-write mount for as long as it stays mounted -
DESIGN-MOUNT-008 in [`mount-write-path.md`](mount-write-path.md); a future directed import,
reclaim, or compaction run) takes an exclusive OS advisory lock (`flock` on Unix, `LockFileEx` on
Windows) on a dedicated file inside `meta/`, non-blocking: if another process already holds it, the
attempt fails immediately rather than waiting (REQ-MAINTENANCE-004's "refused rather than allowed
to proceed" - REQ-MAINTENANCE-006's wait-instead-of-refuse option is not addressed here). The lock
is held for as long as the process that acquired it keeps running, released automatically by the
OS when its holding file descriptor/handle closes - on a clean exit, but also on a crash or a hard
kill, with no separate cleanup step and no stale-lock state left behind to reconcile on a later
run.

This is a separate concern from `crate::connection`'s per-process `Mutex<Connection>`: that guards
this *process's own threads* against racing each other on the one shared connection; this guards
against a wholly *different process* opening the same repository for writing at the same time.

### Why SQLite's own locking is not enough on its own

SQLite already serializes write *transactions* against each other across processes - one write
transaction commits before another can begin, regardless of how many connections attempt to write.
This is real, and is exactly what REQ-MAINTENANCE-004's own rationale already credits it for:
protecting against corruption from an operation that only ever writes metadata. It does not,
however, extend to what this decision actually needs: exclusivity across a whole *session's* worth
of many separate transactions, not just within any one of them. Nothing stops SQLite from letting a
second process's transaction interleave between two transactions belonging to a session that
still considers itself "in progress" - a background write job that reserves and records a new
chunk's byte range in one short transaction, then writes those bytes through `store` as a separate
step outside any transaction at all (DESIGN-STORE-003 in [`byte-store.md`](byte-store.md)),
depends on nothing else being able to reclaim that same range in between; that guarantee holds only
because reclaim itself cannot start while the write session is still running, which is precisely
what this decision provides and SQLite's own per-transaction locking does not.

SQLite's locking also has no awareness at all of `crates/store`'s separate `data/` directory
(DESIGN-STORE-002) - two processes physically relocating or overwriting byte ranges there at the
same time is a repository-corrupting race SQLite's own locking cannot see, let alone prevent.

### Known limitation: not guaranteed on a network-mounted repository

Advisory locking is enforced by the operating system for a *locally* mounted filesystem, including
one with no native locking support of its own - the FAT family, in particular, since the OS
enforces the lock in memory against the local mount, never asking the underlying filesystem driver
to cooperate. A network-mounted repository is a different case: reliability then depends on the
network filesystem protocol's own locking support and the client/server configuration actually in
use (NFS's lock manager, or an SMB client's own settings) - support ranges from solid to silently
absent depending on that configuration, in a way this project has no way to detect or verify from
inside a lock request that reports success either way. This is a real, currently accepted
limitation for a network-mounted repository specifically, not a property of local or removable
storage (an external/USB drive, in any filesystem format) - documented for the user in
[`../../README.md`](../../README.md) rather than silently assumed away.

### Alternative considered and rejected: a manually-written PID/lock file

Writing a plain file recording the holding process's PID, checked and removed by hand, was
considered and rejected: unlike an OS advisory lock, nothing releases it automatically if the
holding process crashes or is killed, leaving a stale lock file a later run must detect (e.g. by
checking whether the recorded PID is still running - itself unreliable across a reboot, where PIDs
get reused) and clean up before it can proceed. An OS advisory lock needs none of that: the same
mechanism that grants exclusivity is exactly what releases it, unconditionally, the moment the
holding process's file descriptor/handle goes away.

### Alternative considered and rejected: relying on `busy_timeout` to smooth over write contention

SQLite's `busy_timeout` connection setting (`connection.rs`) already makes a connection wait
instead of immediately failing with `SQLITE_BUSY` on transient contention. Read as a substitute for
this decision and rejected for the same reason SQLite's own transaction-level locking is not
enough: it only ever smooths over momentary contention between individual transactions, never
providing the session-spanning exclusivity a whole mutating session actually needs.
