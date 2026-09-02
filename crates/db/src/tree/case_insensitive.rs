//! DESIGN-MOUNT-005's Windows-only case-insensitive lookup fallback (`docs/design/
//! tree-namespace-case-sensitivity.md`) and DESIGN-MOUNT-017's per-directory name cache built on
//! top of it - together, everything [`super::find_child_id`] needs once its own indexed
//! exact-match query misses. A distinct concern from the rest of [`super`]'s ordinary CRUD
//! operations, which is why it lives in its own submodule: [`fold_key`]/[`insert_keeping_highest_id`]
//! decide what "the same name" means case-insensitively, and [`find_child_id_case_insensitive`]/
//! [`note_child_inserted`] are the only two seams the rest of `tree` needs to reach into this
//! module through - a mutation call site only ever says "this inserted a new child, keep the cache
//! in sync" ([`note_child_inserted`]), never the folding/caching mechanics themselves.

use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::Error;
use crate::name_cache::NameCache;

/// DESIGN-MOUNT-005's Unicode case fold for the lookup fallback below - Rust's full case mapping,
/// locale-independent by construction (deterministic regardless of the running system's locale).
/// Not guaranteed to match NTFS's own per-codepoint upcase table in every corner case (e.g. German
/// `ß`, whose uppercase form changes length) - see that design doc's "Known limitations".
fn fold_key(name: &str) -> String {
    name.to_uppercase()
}

/// Inserts `id` under `folded_key` into `candidates`, keeping the higher id if one is already
/// present there - DESIGN-MOUNT-005's deterministic tiebreak (the most recently created entry wins
/// a case-only collision). Used while populating a cache entry from an unordered `SELECT`, where
/// more than one live sibling can fold to the same key (e.g. a pair written from Linux, where
/// case-only collisions are not prevented - REQ-MOUNT-010).
fn insert_keeping_highest_id(candidates: &mut HashMap<String, i64>, folded_key: String, id: i64) {
    candidates
        .entry(folded_key)
        .and_modify(|existing| {
            if id > *existing {
                *existing = id;
            }
        })
        .or_insert(id);
}

/// Folds `name` and passes it on to [`NameCache::note_inserted`] - a mutation call site (`mkdir`,
/// `settle_file`) only needs "this inserted a new child, keep the cache in sync", not `fold_key`'s
/// own case-insensitivity concern or `note_inserted`'s folded-name precondition spelled out inline.
pub(super) fn note_child_inserted(cache: &mut NameCache, parent_id: i64, id: i64, name: &str) {
    cache.note_inserted(parent_id, id, &fold_key(name));
}

/// [`super::find_child_id`]'s Windows-only fallback body, factored out and left unconditionally
/// compiled (not itself `#[cfg(windows)]`) so its actual query-and-match logic can be exercised by
/// tests on any platform, even though [`super::find_child_id`] only ever calls it on a real
/// Windows build.
///
/// Consults `cache` first - DESIGN-MOUNT-017's mitigation for the unindexed full scan
/// below being O(n) in the parent's live child count on every miss. The cached form is keyed by
/// folded name (`HashMap<String, i64>`, not a linearly-scanned list), so a lookup against an
/// already-warm entry is an `O(1)` average hash lookup, not another `O(n)` scan.
pub(super) fn find_child_id_case_insensitive(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
) -> Result<Option<i64>, Error> {
    let target_key = fold_key(name);
    cache.with_cached_or_populate(
        parent_id,
        || {
            // Folds each candidate's name once here, at population time - not the raw `name`, so
            // a lookup against an already-warm cache entry never has to fold again.
            let mut stmt = conn.prepare(
                "SELECT id, name FROM tree_entries \
                 WHERE parent_id = ?1 AND deleted_at IS NULL AND id != 0",
            )?;
            let rows = stmt.query_map(params![parent_id], |row| {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?;
            let mut candidates = HashMap::new();
            for row in rows {
                let (id, name) = row?;
                insert_keeping_highest_id(&mut candidates, fold_key(&name), id);
            }
            Ok(candidates)
        },
        |candidates| candidates.get(&target_key).copied(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RepositorySettings, init_repository, open_repository};

    // Returns the TempDir alongside the Repository - it must outlive every use of the
    // Repository (dropping it deletes the directory the open connection points at).
    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(Some(20), 1_700_000_000_000);
        init_repository(&repo_root, settings).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        (repo, dir)
    }

    /// Inserts a bare `contents` row (no chunks) for tests that only need a valid `content_id` to
    /// point a file entry at, not the dedup bookkeeping itself.
    fn insert_content(repo: &crate::Repository, id: i64, hash_byte: u8) -> i64 {
        repo.with_connection(|conn, _cache| {
            conn.execute(
                "INSERT INTO contents (id, length, hash) VALUES (?1, ?2, ?3)",
                (id, 0, vec![hash_byte; 20]),
            )?;
            Ok(())
        })
        .unwrap();
        id
    }

    // DESIGN-MOUNT-005: the fold/tiebreak logic itself, platform-independent - runs on every
    // platform regardless of which one actually reaches it through `find_child_id`.

    #[test]
    fn fold_key_folds_ascii_case() {
        assert_eq!(fold_key("Foo"), fold_key("foo"));
    }

    #[test]
    fn fold_key_folds_non_ascii_case_too() {
        assert_eq!(fold_key("café"), fold_key("CAFÉ"));
    }

    #[test]
    fn insert_keeping_highest_id_keeps_the_only_id_present() {
        let mut candidates = std::collections::HashMap::new();
        insert_keeping_highest_id(&mut candidates, fold_key("foo"), 1);
        assert_eq!(candidates.get(&fold_key("FOO")), Some(&1));
    }

    #[test]
    fn insert_keeping_highest_id_keeps_the_higher_id_regardless_of_insertion_order() {
        let key = fold_key("foo");
        let mut candidates = std::collections::HashMap::new();
        insert_keeping_highest_id(&mut candidates, key.clone(), 3);
        insert_keeping_highest_id(&mut candidates, key.clone(), 7);
        insert_keeping_highest_id(&mut candidates, key.clone(), 5);
        assert_eq!(candidates.get(&key), Some(&7));
    }

    // DESIGN-MOUNT-005's query-and-match fallback, called directly (bypassing `find_child_id`'s
    // `cfg!(windows)` dispatch) so the real SQLite-backed logic is exercised on every platform,
    // not only a real Windows build.

    #[test]
    fn find_child_id_case_insensitive_finds_a_case_variant_sibling() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "foo", 100).unwrap();

        let mut cache = NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                find_child_id_case_insensitive(conn, &mut cache, 0, "FOO")
            })
            .unwrap();
        assert_eq!(found, Some(id));
    }

    #[test]
    fn find_child_id_case_insensitive_prefers_the_highest_id_among_pre_existing_case_variants() {
        let (repo, _dir) = repo();
        // Two live siblings differing only by case, as if written from Linux (REQ-MOUNT-010 does
        // not prevent that there - only `find_child_id`'s Windows-only fallback prevents
        // *creating* a second one once a build reaches it). Inserted directly via SQL since
        // `mkdir` itself would refuse the second one, even running this test on a non-Windows
        // platform - this exercises the real population path in `find_child_id_case_insensitive`
        // itself, not just the standalone `insert_keeping_highest_id` tiebreak helper above.
        let higher_id = repo
            .with_transaction(|conn, _cache| {
                conn.execute(
                    "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'foo', 100, 0)",
                    [],
                )?;
                conn.execute(
                    "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'Foo', 100, 0)",
                    [],
                )?;
                Ok(conn.last_insert_rowid())
            })
            .unwrap();

        let mut cache = NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                find_child_id_case_insensitive(conn, &mut cache, 0, "FOO")
            })
            .unwrap();
        assert_eq!(found, Some(higher_id));
    }

    #[test]
    fn find_child_id_case_insensitive_returns_none_without_a_match() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "foo", 100).unwrap();

        let mut cache = NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                find_child_id_case_insensitive(conn, &mut cache, 0, "bar")
            })
            .unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn find_child_id_case_insensitive_ignores_a_deleted_entry() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "foo", 100).unwrap();
        repo.rmdir(id, 200).unwrap();

        let mut cache = NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                find_child_id_case_insensitive(conn, &mut cache, 0, "FOO")
            })
            .unwrap();
        assert_eq!(found, None);
    }

    // A regression test for the cache itself: a removal must invalidate the *parent's* cached
    // entry, not the removed id's own - easy to get backwards, and a stale cache would silently
    // resurrect a removed sibling for every case-insensitive lookup against that parent from then
    // on.
    #[test]
    fn find_child_id_case_insensitive_does_not_resurrect_a_sibling_removed_after_the_cache_was_warmed()
     {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);

        let a = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, 0, "a", 100))
            .unwrap();

        let found = repo
            .with_connection(|conn, _cache| {
                find_child_id_case_insensitive(conn, &mut cache, 0, "A")
            })
            .unwrap();
        assert_eq!(
            found,
            Some(a),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| crate::tree::rmdir(conn, &mut cache, a, 200))
            .unwrap();

        let found_after_removal = repo
            .with_connection(|conn, _cache| {
                find_child_id_case_insensitive(conn, &mut cache, 0, "A")
            })
            .unwrap();
        assert_eq!(found_after_removal, None);
    }

    // White-box coverage for every `tree_entries` mutation path's effect on the cache
    // (DESIGN-MOUNT-017 in `tree-namespace-case-sensitivity.md`), not just `mkdir`/`rmdir` above -
    // each test shares one `NameCache` instance across a warming lookup and the mutation, the same
    // way a real `Repository`'s single cache instance would, so a wrong or missing cache update
    // shows up as an incorrect `find_child_id_case_insensitive` result rather than being masked by
    // a fresh cache instance per call.

    #[test]
    fn mkdir_keeps_an_already_warmed_directory_correct_for_a_newly_created_sibling() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);

        let a = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, 0, "a", 100))
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A"
            ))
            .unwrap(),
            Some(a),
            "the cache must be warm for the root directory now"
        );

        let b = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, 0, "b", 100))
            .unwrap();
        // "b" must be found even though the cache was already warm before it was created - proves
        // `note_inserted` actually keeps an already-cached entry current, not just a freshly
        // populated one.
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "B"
            ))
            .unwrap(),
            Some(b)
        );
    }

    #[test]
    fn settle_file_keeps_an_already_warmed_directory_correct_for_a_newly_created_sibling() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let a = repo
            .with_transaction(|conn, _cache| {
                crate::tree::settle_file(conn, &mut cache, 0, "a.txt", 100, content_a)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(a),
            "the cache must be warm for the root directory now"
        );

        let b = repo
            .with_transaction(|conn, _cache| {
                crate::tree::settle_file(conn, &mut cache, 0, "b.txt", 100, content_b)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "B.TXT"
            ))
            .unwrap(),
            Some(b)
        );
    }

    #[test]
    fn settle_file_replacing_an_existing_file_keeps_the_cache_correct() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let first = repo
            .with_transaction(|conn, _cache| {
                crate::tree::settle_file(conn, &mut cache, 0, "a.txt", 100, content_a)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(first),
            "the cache must be warm for the root directory now"
        );

        let second = repo
            .with_transaction(|conn, _cache| {
                crate::tree::settle_file(conn, &mut cache, 0, "a.txt", 200, content_b)
            })
            .unwrap();
        assert_ne!(
            first, second,
            "settle_file creates a new history entry, not an in-place update"
        );

        // Must resolve to the new entry, not the replaced one - proves the invalidate-then-let-the-
        // next-miss-repopulate path actually runs, rather than a stale entry surviving the replace.
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(second)
        );
    }

    #[test]
    fn unlink_file_does_not_resurrect_a_file_removed_after_the_cache_was_warmed() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);
        let content_id = insert_content(&repo, 1, 0xAA);

        let id = repo
            .with_transaction(|conn, _cache| {
                crate::tree::settle_file(conn, &mut cache, 0, "a.txt", 100, content_id)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(id),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| crate::tree::unlink_file(conn, &mut cache, id, 200))
            .unwrap();

        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            None
        );
    }

    #[test]
    fn rename_within_the_same_directory_keeps_the_cache_correct_for_the_new_name() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);

        let a = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, 0, "a", 100))
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A"
            ))
            .unwrap(),
            Some(a),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| {
            crate::tree::rename(conn, &mut cache, 0, "a", 0, "renamed", false, 200)
        })
        .unwrap();

        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "A"
            ))
            .unwrap(),
            None,
            "the old name must no longer resolve"
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "RENAMED"
            ))
            .unwrap(),
            Some(a),
            "the new name must resolve, against the same, previously-warmed cache instance"
        );
    }

    #[test]
    fn rename_across_directories_keeps_the_cache_correct_in_both_parents() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);

        let source = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, 0, "source", 100))
            .unwrap();
        let target_dir = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, 0, "target", 100))
            .unwrap();
        let a = repo
            .with_transaction(|conn, _cache| crate::tree::mkdir(conn, &mut cache, source, "a", 100))
            .unwrap();

        // Warm the cache for both the source and the (empty) target directory before the move.
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, source, "A"
            ))
            .unwrap(),
            Some(a)
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, target_dir, "A"
            ))
            .unwrap(),
            None
        );

        repo.with_transaction(|conn, _cache| {
            crate::tree::rename(conn, &mut cache, source, "a", target_dir, "a", false, 200)
        })
        .unwrap();

        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, source, "A"
            ))
            .unwrap(),
            None,
            "the old parent's cache must no longer show the moved entry"
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, target_dir, "A"
            ))
            .unwrap(),
            Some(a),
            "the new parent's cache must show the moved entry"
        );
    }

    #[test]
    fn rename_replacing_an_existing_file_keeps_the_cache_correct() {
        let (repo, _dir) = repo();
        let mut cache = NameCache::with_active(16, true);
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let old = repo
            .with_transaction(|conn, _cache| {
                crate::tree::settle_file(conn, &mut cache, 0, "old.txt", 100, content_a)
            })
            .unwrap();
        repo.with_transaction(|conn, _cache| {
            crate::tree::settle_file(conn, &mut cache, 0, "new.txt", 100, content_b)
        })
        .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "OLD.TXT"
            ))
            .unwrap(),
            Some(old),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| {
            crate::tree::rename(conn, &mut cache, 0, "old.txt", 0, "new.txt", false, 200)
        })
        .unwrap();

        // `rename` moves `old`'s own row rather than creating a new one, so "new.txt" must now
        // resolve to `old`'s id - not the replaced target's, and not a stale cached miss either.
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "OLD.TXT"
            ))
            .unwrap(),
            None
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| find_child_id_case_insensitive(
                conn, &mut cache, 0, "NEW.TXT"
            ))
            .unwrap(),
            Some(old)
        );
    }

    // The base, case-sensitive behavior (REQ-MOUNT-010) must stay exactly as it was outside the
    // Windows fallback - this would fail if `find_child_id`'s `cfg!(windows)` dispatch above were
    // ever accidentally widened to every platform.

    #[test]
    #[cfg(not(windows))]
    fn mkdir_allows_a_case_only_variant_outside_windows() {
        let (repo, _dir) = repo();
        let foo = repo.mkdir(0, "foo", 100).unwrap();

        let variant = repo
            .mkdir(0, "Foo", 200)
            .expect("a case-only variant must succeed outside the Windows lookup fallback");
        assert_ne!(foo, variant);
    }

    // DESIGN-MOUNT-005's full Windows-only stack, through the public `Repository` API - cannot
    // run on this development platform, but is compiled and checked wherever a Windows build is
    // (the Docker cross-compile check, and real WinFSP via the `julius-winfsp-ssh` skill).

    #[test]
    #[cfg(windows)]
    fn mkdir_refuses_a_case_only_variant_on_windows() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "foo", 100).unwrap();

        let err = repo.mkdir(0, "Foo", 200).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    #[cfg(windows)]
    fn rename_case_only_respelling_of_self_succeeds_and_updates_spelling() {
        // REQ-MOUNT-010's own example (install.txt -> Install.txt), exercised via mkdir for
        // simplicity - the distinction does not matter here, the self-identity check runs before
        // any file/directory-kind logic.
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "install.txt", 100).unwrap();

        repo.rename(0, "install.txt", 0, "Install.txt", false, 200)
            .expect("a case-only respelling of the same entry must succeed");

        let entry = repo.resolve_path("/Install.txt").unwrap().unwrap();
        assert_eq!(entry.id, id);
        // Lookup stays case-insensitive regardless of which spelling is currently stored.
        assert_eq!(repo.resolve_path("/install.txt").unwrap().unwrap().id, id);
        let names: Vec<String> = repo
            .list_children(0)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec!["Install.txt".to_string()]);
    }

    #[test]
    #[cfg(windows)]
    fn rename_refuses_a_different_entrys_case_variant_directory() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        repo.mkdir(0, "B", 100).unwrap();

        // "b" does not exist exactly, but case-insensitively resolves to "B" - REQ-MOUNT-009's
        // directory-collision refusal still applies to that resolved target.
        let err = repo.rename(0, "a", 0, "b", false, 200).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    #[cfg(windows)]
    fn rename_replaces_a_different_entrys_case_variant_file() {
        let (repo, _dir) = repo();
        repo.with_connection(|conn, _cache| {
            conn.execute(
                "INSERT INTO contents (id, length, hash) \
                 VALUES (2, 0, X'0102030405060708090A0B0C0D0E0F1011121314')",
                (),
            )?;
            conn.execute(
                "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) \
                 VALUES (1, 0, 'old.txt', 0, 2, 1)",
                (),
            )?;
            conn.execute(
                "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) \
                 VALUES (2, 0, 'New.txt', 0, 2, 1)",
                (),
            )?;
            Ok(())
        })
        .unwrap();

        // "new.txt" does not exist exactly, but case-insensitively resolves to "New.txt" - the
        // existing file-replaces-file rule (REQ-MOUNT-009) still applies to that resolved target.
        repo.rename(0, "old.txt", 0, "new.txt", false, 200)
            .expect("replacing an existing file's case variant must succeed");
        let replaced = repo.resolve_path("/new.txt").unwrap().unwrap();
        assert_eq!(replaced.id, 1);
    }
}
