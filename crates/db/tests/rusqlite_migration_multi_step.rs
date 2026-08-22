//! Verifies a guarantee `open_repository` depends on but cannot itself exercise yet: pre-release,
//! this project's real schema has exactly one migration ("Pre-release: a single, freely rewritten
//! v1 migration" under DESIGN-METADATA-005 in `docs/design/metadata-storage.md`), so there is no
//! real multi-version history to test against.
//!
//! Reading `rusqlite_migration`'s own `Migrations::goto_up` confirms it applies every intermediate
//! migration's SQL in sequence when jumping more than one version at once, not just the
//! difference between two schema versions - but this is not stated as a plain-English guarantee in
//! its docs, and its own test suite only checks the resulting version-number bookkeeping for a
//! multi-version jump, never actual schema/data changes. A synthetic, throwaway three-version
//! schema here - independent of this crate's real schema - closes that gap with real data.

use rusqlite::Connection;
use rusqlite_migration::{M, Migrations};

#[test]
fn to_latest_applies_every_intermediate_migration_when_jumping_several_versions_at_once() {
    let only_the_first_migration = Migrations::new(vec![M::up("CREATE TABLE t (a INTEGER)")]);
    let all_three_migrations = Migrations::new(vec![
        M::up("CREATE TABLE t (a INTEGER)"),
        M::up("ALTER TABLE t ADD COLUMN b INTEGER"),
        M::up("INSERT INTO t (a, b) VALUES (1, 2)"),
    ]);

    let mut conn = Connection::open_in_memory().unwrap();
    // Stand in for a repository backed up while at schema version 1.
    only_the_first_migration.to_latest(&mut conn).unwrap();
    conn.execute("INSERT INTO t (a) VALUES (0)", ()).unwrap();

    // Stand in for that backup being restored, then opened by code that only knows about the
    // now-current, later schema - jumping two versions in a single to_latest call, not one at a
    // time.
    all_three_migrations.to_latest(&mut conn).unwrap();

    let migration_3_row: (i64, i64) = conn
        .query_row("SELECT a, b FROM t WHERE a = 1", (), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("migration 3's INSERT must have run");
    assert_eq!(migration_3_row, (1, 2));

    // The row inserted between the two to_latest calls must survive migration 2's ALTER TABLE
    // unharmed, with the new column defaulting to NULL rather than losing the row entirely.
    let preexisting_row_b: Option<i64> = conn
        .query_row("SELECT b FROM t WHERE a = 0", (), |row| row.get(0))
        .expect("the pre-existing row must survive migration 2's ALTER TABLE");
    assert_eq!(preexisting_row_b, None);
}
