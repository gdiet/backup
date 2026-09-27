# User-Facing Timezone Display

## DESIGN-CLI-007: Local time by default, resolved fresh per timestamp via `time`'s `local-offset` feature
Status: implemented (crates/cli/src/time_format.rs)

REQ-OPERABILITY-008 (`requirements/non-functional/operability.md`) needs the operator's own local
timezone for user-facing timestamps, reflecting whatever daylight-saving rule applied at the
specific instant a given timestamp names - not just whatever rule happens to apply "now". `time`'s
`local-offset` feature (`UtcOffset::local_offset_at`) provides exactly this, called fresh for every
timestamp rather than cached once at startup: a fixed, cached offset would render a timestamp from
before a daylight-saving transition under the wrong rule for as long as the process kept running
past it.

### Verified safe to call from a mount session's own worker/dispatch threads

Older versions of the `time` crate had a well-known soundness hazard (RUSTSEC-2020-0071): calling
into its local-offset machinery from a multithreaded process could race a `tzset()`-driven
environment-variable read. The pinned version here (`0.3.55`) no longer routes
`UtcOffset::local_offset_at`/`OffsetDateTime::now_local` through that path at all - confirmed by
reading the vendored source directly, not assumed from the historical advisory alone - and verified
empirically, live, on both platforms this project mounts on: calling it single-threaded, with eight
concurrently running worker threads, and from within one of those workers while the others stayed
alive, all succeeded identically on Windows and on WSL2/Debian Linux. `dfs mount`'s own
worker/dispatch pools (DESIGN-MOUNT-006 in `mount-write-path.md`) are exactly this kind of
concurrent caller, so this needed direct verification rather than trusting the crate's own docs on
faith.

### Known caveat: a musl/Alpine build without its own timezone database

`local_offset_at` falls back to UTC (REQ-OPERABILITY-008's own documented fallback, marked with the
same "Z" a genuine UTC choice gets, never silently mislabeled as local) whenever the platform cannot
determine an offset at all. The concrete case likely to matter here is a musl-based container (e.g.
Alpine) missing the `tzdata` package, which does not ship `/usr/share/zoneinfo` by default. Not a
bug, and not currently relevant: this project's own Docker image
(`docker/samba-mount/Dockerfile`) builds on Debian (glibc), and no musl target exists anywhere in
this project as of this writing. Worth revisiting if a musl-based image is ever added - installing
`tzdata` there is the standard fix.
