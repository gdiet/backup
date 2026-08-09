use std::path::Path;
use std::process::ExitCode;

use clap::Args;
use store::{LongTermStore, ReadIntegrity};

use crate::chunk_store::{SpaceAllocator, read_chunk_bytes, write_chunk_bytes};
use crate::progress::Progress;
use crate::repo_lock::RepoLock;

#[derive(Args)]
pub struct CompactStoreArgs {}

/// Defragments the data store: relocates every live chunk so the store's
/// used address space becomes one contiguous block starting at 0, then
/// truncates the backing files down to that new, smaller size - turning
/// gaps `reclaim-space` left behind (`db::free_space_summary`, surfaced by
/// `stats`) into actually reclaimed disk space. See
/// `docs/plans/implemented/compact-store.md`.
///
/// **Invalidates older `db backup` snapshots** for restore purposes - see
/// `docs/plans/implemented/stale-backup-guard.md`: `db restore` warns
/// automatically when this has happened (it bumps `store_generation`
/// unconditionally whenever it actually relocates anything), but take a
/// fresh backup first if you want to be able to restore back to exactly
/// this point in time.
///
/// Exclusive: refuses to run if another command already holds the
/// repository's lock file (see [`crate::repo_lock`]) - relocating chunks
/// while `store`/`mount --read-write`/`reclaim-space` touch the same
/// `chunk_extents` rows and store bytes would race.
///
/// Safe to interrupt (`SIGINT`/`SIGKILL`/power loss) at any point and
/// resume with another run: every relocation commits in one step (the
/// chunk's bytes are written to their new, still-unreferenced location
/// first; a single transaction then switches `chunk_extents` to point at
/// them), and both the target size and which chunk to move next are
/// always freshly recomputed from the database's current state at the
/// start of each run, never a remembered plan - matching
/// `docs/plans/implemented/03-chunk-extents.md`'s own "no persisted
/// free-list" choice.
pub fn run_compact_store(repo: &Path, _args: CompactStoreArgs) -> ExitCode {
    let repository = match db::open_repository(repo) {
        Ok(r) => r,
        Err(err) => {
            eprintln!(
                "error: failed to open repository at {}: {err}",
                repo.display()
            );
            return ExitCode::FAILURE;
        }
    };

    let _lock = match RepoLock::try_acquire(&db::meta_dir(repo)) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            eprintln!(
                "error: another command is already running against this repository \
                 (meta/.lock is held) - try again once it finishes"
            );
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("error: failed to acquire the repository lock: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut conn = match repository.open_write_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };
    let data_store = LongTermStore::new(repository.data_dir(), false);

    let target_size = match db::total_live_bytes(&conn) {
        Ok(n) => n,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };
    let extents = match db::chunk_extents_sorted(&conn) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };
    let to_move = match db::bytes_to_relocate(&conn, target_size) {
        Ok(n) => n,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "Compacting store: target size {target_size} byte(s), {to_move} byte(s) to relocate..."
    );
    // Built once and reused for the whole run, not rebuilt per chunk - a
    // vacated chunk's old position is always at or past target_size (by
    // the selection query below), so it's never needed as a destination
    // for a later move in this same run; the trailing-region/gap state
    // this allocator tracks only ever needs to reflect what's still below
    // target_size, which reserve() alone already maintains correctly
    // across calls. Matches `store`'s own single-allocator-per-run usage.
    let allocator = SpaceAllocator::from_sorted_extents(&extents);
    let mut progress = Progress::new(to_move as u64);
    let mut chunks_moved = 0u64;

    loop {
        let next = match db::next_chunk_to_relocate(&conn, target_size) {
            Ok(n) => n,
            Err(err) => {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        };
        let Some((chunk_id, length)) = next else {
            break;
        };

        let (bytes, integrity) = match read_chunk_bytes(&conn, &data_store, chunk_id, length as u64)
        {
            Ok(result) => result,
            Err(err) => {
                eprintln!("error: failed to read chunk {chunk_id}: {err}");
                return ExitCode::FAILURE;
            }
        };
        // Not this command's job to validate data integrity (`check`
        // already does that) - relocate whatever could be read (missing
        // bytes already zero-filled by `read_chunk_bytes`) rather than
        // getting stuck reprocessing the same chunk forever.
        if let ReadIntegrity::Incomplete { missing_or_short } = &integrity {
            eprintln!(
                "warning: chunk {chunk_id} has missing/short store data ({} file(s) - see \
                 `check`) - relocating whatever could be read anyway",
                missing_or_short.len()
            );
        }

        let new_extents = match write_chunk_bytes(&data_store, &allocator, &bytes) {
            Ok(extents) => extents,
            Err(err) => {
                eprintln!("error: failed to write relocated chunk {chunk_id}: {err}");
                return ExitCode::FAILURE;
            }
        };

        if let Err(err) = db::relocate_chunk(&mut conn, chunk_id, &new_extents) {
            eprintln!("error: failed to record relocated chunk {chunk_id}: {err}");
            return ExitCode::FAILURE;
        }

        chunks_moved += 1;
        progress.add(length as u64);
    }
    progress.finish();

    // Unconditional on having actually moved something, unlike
    // reclaim_space's chunks_purged > 0 guard: a no-op run (already fully
    // packed) genuinely changed nothing, so there's nothing for a later
    // db restore to warn about.
    if chunks_moved > 0
        && let Err(err) = db::bump_store_generation(&conn)
    {
        eprintln!("error: {err}");
        return ExitCode::FAILURE;
    }

    if let Err(err) = data_store.truncate_to(target_size as u64) {
        eprintln!("error: failed to truncate the data store: {err}");
        return ExitCode::FAILURE;
    }

    println!("Store compacted: {chunks_moved} chunk(s) relocated, size now {target_size} byte(s).");
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::path::PathBuf;

    fn init_repo() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        (temp_dir, repo_root)
    }

    /// Writes `bytes` to the physical store at `start` and registers a
    /// matching `chunks`/`chunk_extents` row - bypasses `tree_entries`
    /// entirely, since `compact-store` operates on every row still present
    /// in `chunks` regardless of whether anything currently references it
    /// (that distinction is `reclaim-space`'s job, not this one's).
    fn write_chunk(repo_root: &Path, conn: &Connection, id: i64, start: u64, bytes: &[u8]) {
        let data_store = store::LongTermStore::new(repo_root.join("data"), false);
        data_store.write(start, bytes).unwrap();
        conn.execute(
            "INSERT INTO chunks (id, length, hash) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, bytes.len() as i64, format!("h{id}").into_bytes()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (?1, 0, ?2, ?3)",
            rusqlite::params![id, start as i64, start as i64 + bytes.len() as i64],
        )
        .unwrap();
    }

    #[test]
    fn run_compact_store_succeeds_on_an_empty_repository() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_compact_store(&repo_root, CompactStoreArgs {}),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn run_compact_store_relocates_the_tail_chunk_into_a_gap_and_shrinks_the_store() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();

        write_chunk(&repo_root, &conn, 1, 0, &[1u8; 100]);
        write_chunk(&repo_root, &conn, 2, 100, &[2u8; 100]);
        write_chunk(&repo_root, &conn, 3, 200, &[3u8; 100]);
        // Simulate reclaim-space already having purged chunk 2, leaving a
        // gap at [100, 200) - cascades into its chunk_extents row too.
        conn.execute("DELETE FROM chunks WHERE id = 2", ()).unwrap();
        drop(conn);

        assert_eq!(
            run_compact_store(&repo_root, CompactStoreArgs {}),
            ExitCode::SUCCESS
        );

        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        assert_eq!(db::store_generation(&conn).unwrap(), 1);
        assert_eq!(
            db::chunk_extents(&conn, 3).unwrap(),
            vec![(100, 200)],
            "chunk 3 relocated into the gap chunk 2 left behind"
        );
        assert_eq!(db::chunk_extents(&conn, 1).unwrap(), vec![(0, 100)]);

        let data_store = store::LongTermStore::new(repository.data_dir(), true);
        let mut buf = [0u8; 100];
        data_store.read(100, &mut buf).unwrap();
        assert_eq!(buf, [3u8; 100]);

        let file_len = std::fs::metadata(repository.data_dir().join("00/00/0000000000"))
            .unwrap()
            .len();
        assert_eq!(file_len, 200, "truncated down to the new target size");
    }

    #[test]
    fn run_compact_store_is_a_no_op_and_does_not_bump_generation_when_already_packed() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        write_chunk(&repo_root, &conn, 1, 0, &[1u8; 50]);
        drop(conn);

        assert_eq!(
            run_compact_store(&repo_root, CompactStoreArgs {}),
            ExitCode::SUCCESS
        );

        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        assert_eq!(
            db::store_generation(&conn).unwrap(),
            0,
            "nothing moved - must not bump"
        );
    }

    #[test]
    fn run_compact_store_refuses_when_the_lock_is_already_held() {
        let (_temp_dir, repo_root) = init_repo();
        let _lock = RepoLock::try_acquire(&db::meta_dir(&repo_root))
            .unwrap()
            .unwrap();

        assert_eq!(
            run_compact_store(&repo_root, CompactStoreArgs {}),
            ExitCode::FAILURE
        );
    }
}
