# Implement the `store` (backup) subcommand

## Context

The Rust workspace (`cdc`, `store`, `db`, `cli`) has all the individual building blocks for a deduplicating backup tool, but `cli store` currently only walks source files, chunks/hashes them, and prints the result — nothing is written to the SQLite metadata DB or to the on-disk byte store. This plan wires those pieces together into a working `store` command.

Design reference was gathered from the Scala tool (mature, whole-file MD5 dedup, single-threaded traversal + one dedicated persist thread, purely additive backups, no `--concurrency` flag) and the Go project (architecturally closer — CDC + chunk-level dedup + bounded worker pool — but unfinished/non-compiling, useful only to validate the chosen architecture). `.backupignore`, `reference`, and `restore` are explicitly out of scope for this round, as is space reclamation (GC) of the byte store — the existing `ref_count` triggers only clean up metadata rows, not physical store bytes, and that stays true after this change.

Four design decisions were clarified with the user and are locked in below (not re-litigated during implementation):
1. Add `tree_entries.kind` to disambiguate directories from (possibly empty) files.
2. Add `contents.hash` (dedup over the ordered chunk-hash sequence) so identical files share one `contents` row — restores parity with Scala's whole-file dedup at the metadata level.
3. `--target-exists` requires the full target path to already exist; `--create-dirs` creates all missing target path components; with neither flag, at most the last path component may be created (`mkdir` vs `mkdir -p` vs plain `mkdir` semantics). The two flags are mutually exclusive.
4. Store-write or DB errors abort the whole run (systemic problem, not per-file); source-read errors remain per-file skip-and-continue, as today.

---

## 1. `db` crate: schema changes

Edit `SCHEMA_V1` in `db/src/migrations.rs` in place (no released data exists yet — single migration so far):

- `tree_entries`: add `kind TEXT NOT NULL CHECK (kind IN ('dir','file'))`. The seeded root row becomes `kind='dir'`. `content_id` stays nullable (`NULL` for directories and empty files alike; `kind` is now the sole authority for dir-vs-file).
- `contents`: add `hash BLOB NOT NULL, UNIQUE(hash)`. Hash is computed by the caller (not by SQLite) as `blake3_20( concat_over_chunks( length_le_u64_bytes || chunk_hash_20_bytes ) )` over the file's ordered chunk sequence; an empty file hashes the empty sequence, so all empty files converge on one shared `contents` row with zero `content_chunks`.

Update the existing raw-SQL test inserts in `db/src/lib.rs` (`tree_entries`/`contents` rows in `tests::*`) to include the new columns.

## 2. `db` crate: new public API

- `pub struct Repository { .. }`, `pub fn open_repository(repo_root: &Path) -> Result<Repository, Error>` — opens `meta/repository.db`, reads `repository_settings` back. Methods: `.settings() -> RepositorySettings`, `.data_dir() -> PathBuf`, `.open_read_connection()` / `.open_write_connection()` (thin wrappers around the existing private `open_connection`, now used both for many short-lived readers and the one dedicated writer).
- `pub fn find_tree_entry(conn, parent_id, name) -> Result<Option<TreeEntryRow>, Error>` and `pub fn insert_directory(conn, parent_id, name, time_millis) -> Result<i64, Error>` (idempotent via `INSERT ... ON CONFLICT (parent_id, name) WHERE deleted_at IS NULL DO NOTHING` against the existing partial unique index, then re-`SELECT`).
- `pub fn find_chunk(conn, length, hash) -> Result<Option<i64>, Error>` — dedup lookup, used from worker read connections.
- `pub enum ChunkRef { Existing(i64), New { length: u64, hash: Vec<u8>, position: u64 } }`, `pub struct FileBackupRecord { parent_id: i64, name: String, time_millis: i64, chunks: Vec<ChunkRef>, content_hash: Vec<u8> }`, `pub fn apply_backup_batch(conn: &mut Connection, batch: &[FileBackupRecord]) -> Result<(), Error>` — the single function the writer thread calls per flush. One transaction per call; per record: insert-or-get each new chunk (`ON CONFLICT (length, hash) DO NOTHING` + re-`SELECT`, resolving races between workers that hashed the same new chunk independently), insert-or-get the `contents` row by `content_hash` (`ON CONFLICT (hash) DO NOTHING` + re-`SELECT`; insert `content_chunks` only when the row was newly created), insert the `tree_entries` row (`kind='file'`). Chunk bytes are never touched here — only metadata.

## 3. `cdc` crate: buffering wrapper

`HashingChunker` hashes chunks but discards the raw bytes, so it can't be used as-is when a dedup-missed chunk's bytes need to be written to the store. Add `BufferingHashingChunker<H, C>` next to `HashingChunker` (same file), returning `ChunkWithBytes { length_hash: LengthHash, bytes: Vec<u8> }`: it owns its own `chunker`/`hasher` and re-implements the same short slicing loop `HashingChunker::next` already has, additionally buffering each slice before hashing it. This means every file is read exactly once regardless of hit/miss ratio (a dedup hit just drops the buffered bytes) — strictly better than a re-read-on-miss design, which would double I/O on a first/full backup where almost everything misses. Duplicating the ~15-line slicing loop is preferred over refactoring `HashingChunker` to share it, to keep this change small and low-risk to already-tested code.

Add unit tests reusing `HashingChunker`'s existing test vectors, asserting buffered bytes match the original input per chunk.

## 4. `cli` crate: wiring it together in `store.rs`

- **Repo settings**: replace the hardcoded `CDC_TARGET_SIZE_BITS`/CDC-only chunker with `db::open_repository(repo)?.settings()`, and a small `enum RepoChunker { Cdc(CdcChunker), Single(SingleChunkChunker) }` implementing `cdc::Chunker` by delegation, selected from `settings.chunking()` at runtime (avoids `Box<dyn Chunker>`).
- **CLI flags**: mark `create_dirs`/`target_exists` `conflicts_with` each other in the `clap::Args` derive. Implement the three-tier semantics from Context §3 in the up-front directory pass below.
- **Directory pass (single walk per source)**: for each source, drive `walkdir::WalkDir` on the main thread. On each **directory** entry, resolve-or-create its `tree_entries` row via `insert_directory` (using one connection owned by the main thread) and record `relative_path -> tree_entries_id` in an in-memory map; on each **file** entry, look up its already-resolved parent id and push `(path, parent_id)` into a `Vec`. This keeps traversal single-pass (no separate directory-then-file walk) and guarantees parents exist before children are queued. Target-path resolution/creation (the three-tier flag semantics) happens once, before this pass starts, using the same connection.
- **Store-position cursor**: before spawning workers, query `SELECT COALESCE(MAX(stop), 0) FROM chunks` once and seed an `Arc<AtomicU64>`. New chunks reserve space via `fetch_add(length, Ordering::SeqCst)` (wait-free) and are written directly from the worker thread via `store::LongTermStore::write` — never routed through the writer thread, which stays free to batch cheap metadata transactions instead of becoming an I/O bottleneck. Two workers racing on the same new `(length, hash)` may both write bytes (the loser's bytes become permanently orphaned, never referenced) — accepted per Context §4/§out-of-scope GC, resolved deterministically at the metadata level by `apply_backup_batch`'s `ON CONFLICT` handling.
- **Read connections**: one long-lived connection per rayon worker *thread* (via `thread_local!`), reused across all files that thread processes.
- **Writer thread**: `std::thread::spawn` (outside the rayon pool, unaffected by `--concurrency`), owns the one write connection, drains an `mpsc::Receiver<FileBackupRecord>`, batches (e.g. up to 200 messages or a 200ms idle timeout, whichever first) and calls `apply_backup_batch` once per flush.
- **Worker body** (replacing `chunk_and_print`): for each `(path, parent_id)`, open the file, run it through `BufferingHashingChunker`, resolve each chunk via the thread-local read connection (existing vs. new + position from the atomic cursor + immediate `store::write`), compute `content_hash` alongside, and send one `FileBackupRecord` to the writer channel.
- **Error handling**: source open/read failures and per-file mtime failures: log + skip + continue (existing pattern, extended). Store-write or DB errors (writer thread or any read connection): set a shared `AtomicBool` abort flag, stop dispatching new work, skip committing further batches, print a clear error, exit non-zero. Unreadable subdirectories during the walk: `WalkDir` already yields these as `Err` entries (not a panic) — log + skip that subtree, matching existing `walk_files` behavior, deliberately avoiding the Scala bug where this crashes the whole run.
- **Metadata scope**: store `name`, `parent_id`, a single mtime (`fs::metadata()?.modified()` → epoch-millis), and the resolved content reference — no permissions/ownership/extra timestamps/symlinks-as-symlinks this round (matches Scala's existing scope, no regression).

## Suggested sequencing

1. `db` schema changes (`kind`, `contents.hash`) + update existing tests.
2. `db`: `open_repository`/`Repository`.
3. `db`: `find_tree_entry`, `insert_directory`.
4. `db`: `find_chunk`, `ChunkRef`/`FileBackupRecord`, `apply_backup_batch` (+ unit tests, including a race-simulation test: two `apply_backup_batch` calls inserting the same new chunk resolve to one row).
5. `cdc`: `BufferingHashingChunker`/`ChunkWithBytes` + tests.
6. `cli`: `RepoChunker`, settings wiring, `conflicts_with` flags.
7. `cli`: directory pass + target-path resolution (three-tier semantics).
8. `cli`: atomic store-position cursor.
9. `cli`: writer thread + batching.
10. `cli`: worker body rewrite + channel wiring.
11. `cli`: abort-flag plumbing + run summary (skipped-file count, non-zero exit on abort).

## Verification

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test` across the workspace after each step, per `AGENTS.md`.
- New `db` unit tests: `insert_directory` idempotency, `find_chunk`/`apply_backup_batch` insert-or-get behavior for both chunks and contents, the race-simulation test above.
- New `cdc` unit tests for `BufferingHashingChunker` against existing `HashingChunker` vectors.
- `cli` integration test: run `store` against a small temp source tree into a temp repo (via `db::init_repository` + `run_store`), assert resulting `tree_entries`/`contents`/`chunks`/`content_chunks` rows and that store bytes are readable back via `LongTermStore::read`; re-run the same backup and assert zero new `chunks`/`contents` rows are created (dedup hit path) and that duplicate files/directories don't error.
- Manual smoke test via the `run` skill: `cargo run -p cli -- init` a temp repo, `cargo run -p cli -- store` a small real directory tree, inspect the resulting `meta/repository.db` (e.g. via `sqlite3`) and `data/` layout.
