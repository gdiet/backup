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

Acquisition failing for any reason other than the lock already being held - most plausibly the
"Known limitation" below actually manifesting - gets its own distinct, actionable error rather
than falling back to a bare OS error code (REQ-OPERABILITY-004's "a repository that ... is already
in use" is named there as exactly this kind of foreseeable failure).

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

## DESIGN-MAINTENANCE-002: Exclusive creation narrows the acquisition race further; a diagnostic marker records who holds it
Status: implemented (crates/db/src/lock.rs)

DESIGN-MAINTENANCE-001's "Known limitation" is that `flock`'s own cross-process exclusivity is not
guaranteed on a network-mounted repository, in a way this project cannot detect from inside a lock
request that reports success either way. Acquisition adds one more gate ahead of `flock` that
narrows this on a real, common class of such filesystems: exclusive file creation (`O_CREAT|O_EXCL`
- `create_new` in Rust's `OpenOptions`) is one of the most widely and correctly implemented atomic
primitives across network filesystems, including several that do not correctly propagate `flock`
across machines - it needs no server-side lock state held across a session, only a single atomic
"create this name, or tell me it already exists" operation.

Acquisition tries `create_new` on the lock file first. On success, nothing else could have created
that name first, so this session is provably the only one that just started acquiring it. On
`AlreadyExists`, the existing file is opened normally (no `O_EXCL`) and acquisition continues into
`flock` exactly as DESIGN-MAINTENANCE-001 already describes - this fallback is what preserves that
decision's crash-recovery property unchanged: a file left behind by a process that exited without
releasing (a crash, a hard kill) is not itself proof of an active holder, and `flock` is still what
tells the two apart, since the OS already released that process's own advisory lock the moment its
file descriptor/handle went away. `flock` is acquired unconditionally on both paths, including right
after a fresh `create_new` success - without it, a second session's own fallback-and-`flock` attempt
would find nothing held and incorrectly treat the first session's still-active file as a stale
leftover.

For this same-name creation race to be narrowed on *every* acquisition, not only the very first one
ever made against a given repository, a clean release now deletes the lock file, not only releases
`flock` - done while still holding the lock, immediately before the `flock` guard itself is dropped,
never after. Releasing in the other order (drop `flock` first, delete second) would leave a window
where a different process's fallback-and-`flock` attempt already succeeds and considers itself the
new holder, and this session's later delete of the *same name* would then remove the new holder's
own lock file out from under it. Deleting first means a later acquirer's `create_new` only ever
succeeds once this session no longer references that path at all - by the time that happens, this
session's own lingering hold on the (already unnamed) file is unreachable by any future acquirer and
irrelevant to correctness, gone entirely once its `flock` guard finishes dropping immediately after.

Once acquired (via either path), the process writes one line identifying itself - hostname, process
id, and acquisition time - into the file, for a human diagnosing an "already locked" report to see
who actually holds it. This is diagnostic only, never consulted by the locking logic itself - see
the rejected alternative below for why using it as a second gate was considered and specifically not
adopted.

### Known limitation: Windows delete-pending semantics not yet verified against a real mount

Deleting a file while a handle to it is still open succeeds on Windows the same way it does on
Unix - Rust's default share mode already includes `FILE_SHARE_DELETE` - but NTFS keeps the directory
entry in a "pending delete" state until every handle referencing it actually closes, which here
follows only microseconds later as part of the same release (the `flock` guard dropping right after
the delete call above). Whether a competing acquirer's `create_new` in that narrow window reports
`AlreadyExists` and needs a caller-visible retry, or is transparently handled by the OS, has not been
checked against a real Windows/NTFS mount - `agent-todos/` tracks this the same way it already does
for `write_cache.rs`'s sparse-file behavior.

### Alternative considered and rejected: using the diagnostic marker's own presence as a second locking gate

Treating the marker file's content as itself a signal to gate on - refusing whenever it is
non-empty, regardless of what `flock` reports - was considered and specifically rejected: it
reintroduces exactly the failure mode DESIGN-MAINTENANCE-001's own "a manually-written PID/lock
file" alternative already rejected, since a marker left behind by a crashed process is
indistinguishable from an active one without a `flock`-equivalent auto-release property, which
content alone does not have. Concretely, it would make the *reliable* case strictly worse: on local
or removable storage, where `flock` already recovers correctly and automatically from an unclean
shutdown, a stale marker would now block every future acquisition attempt permanently, requiring
manual cleanup - a regression on the one case that works fully today - while only inconsistently
improving the network-filesystem case it targets, since most such filesystems cache reads (an NFS
attribute cache, an SMB client cache, a cloud-sync mount's own caching), so a just-written marker
may not even be visible to a second machine for long enough to matter, in a way this project has no
way to detect any more reliably than it already cannot detect `flock`'s own silent failure
(DESIGN-MOUNT-009's "Alternative considered and rejected: attempting transient/permanent detection
for systemic failures" rejects the same kind of heuristic for the same underlying reason).
