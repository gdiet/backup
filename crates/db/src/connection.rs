//! Connection-level SQLite pragma configuration - see "SQLite connection pragmas" under
//! DESIGN-METADATA-003 in `docs/design/metadata-storage.md` for the reasoning and the prior-art
//! this was checked against.

use rusqlite::Connection;

use crate::Error;

/// Configures a connection this crate will write through - every connection today, since reads
/// and writes still share one connection per [`crate::Repository`] (see its own doc comment).
/// Sets `foreign_keys`, `synchronous`, `busy_timeout`, `auto_vacuum`, and `journal_mode`, in that
/// specific order: `auto_vacuum` must be set before switching to WAL below, since it can only
/// take effect for free on a still-empty database, and the WAL switch itself forces a header
/// write that closes that window - setting `auto_vacuum` after would silently leave it at `NONE`
/// forever, recoverable only by a full, blocking `VACUUM`.
///
/// A future read-only connection (once reads split off their own, per DESIGN-METADATA-003) needs
/// none of this except `busy_timeout` - the rest either only matters for writes, or (for
/// `journal_mode`) is a persistent, whole-database property already set once here.
pub(crate) fn configure_write_connection(conn: &Connection) -> Result<(), Error> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;

    let journal_mode: String =
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
    if journal_mode != "wal" {
        return Err(Error::WalUnavailable(journal_mode));
    }
    Ok(())
}
