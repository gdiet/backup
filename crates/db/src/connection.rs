//! Connection-level SQLite pragma configuration - see "SQLite connection pragmas" under
//! DESIGN-METADATA-003 in `docs/design/metadata-storage.md` for the reasoning and the prior-art
//! this was checked against.

use rusqlite::{Connection, ErrorCode};

use crate::Error;

/// Configures a connection this crate will write through - every connection today, since reads
/// and writes still share one connection per [`crate::Repository`] (see its own doc comment).
/// Sets `foreign_keys`, `synchronous`, `busy_timeout`, `auto_vacuum`, and `journal_mode`, in that
/// specific order: `auto_vacuum` must be set before switching to WAL below, since it can only
/// take effect for free on a still-empty database, and the WAL switch itself forces a header
/// write that closes that window - setting `auto_vacuum` after would silently leave it at `NONE`
/// forever, recoverable only by a full, blocking `VACUUM`.
///
/// A read-only connection (`connection::configure_read_only_connection` below) needs none of this
/// except `busy_timeout` - the rest either only matters for writes, or (for `journal_mode`) is a
/// persistent, whole-database property already set once here.
pub(crate) fn configure_write_connection(conn: &Connection) -> Result<(), Error> {
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(wrap_unreliable_connection_error)?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(wrap_unreliable_connection_error)?;
    conn.pragma_update(None, "busy_timeout", 5000)
        .map_err(wrap_unreliable_connection_error)?;
    conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")
        .map_err(wrap_unreliable_connection_error)?;

    let journal_mode: String = conn
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
        .map_err(wrap_unreliable_connection_error)?;
    if journal_mode != "wal" {
        return Err(Error::WalUnavailable(journal_mode));
    }
    Ok(())
}

/// Configures a genuinely `SQLITE_OPEN_READ_ONLY` connection (`crate::open_repository_read_only`).
/// Sets only `busy_timeout` - `foreign_keys`/`synchronous` have nothing to enforce on a connection
/// that never writes, and `auto_vacuum`/`journal_mode` are persistent, whole-database properties
/// already set by whichever write connection created the repository (DESIGN-METADATA-005 in
/// `docs/design/metadata-storage.md`); a read-only connection could not change them even if it
/// needed to (no write permission), so there is nothing to check or correct here the way
/// `configure_write_connection` does for `journal_mode` above.
pub(crate) fn configure_read_only_connection(conn: &Connection) -> Result<(), Error> {
    conn.pragma_update(None, "busy_timeout", 5000)
        .map_err(wrap_unreliable_connection_error)?;
    Ok(())
}

/// Distinguishes a filesystem that cannot support WAL's locking requirements at all - a
/// connection-configuring `PRAGMA` hard-failing with a locking- or I/O-category SQLite error
/// (observed over a WSL<->Windows 9p bridge: `SQLITE_BUSY`/`SQLITE_IOERR`) - from any other SQLite
/// error, wrapping the former as [`Error::ConnectionUnreliable`] for an actionable message instead
/// of a bare SQLite error string surfacing as [`Error::Sqlite`].
fn wrap_unreliable_connection_error(err: rusqlite::Error) -> Error {
    let is_locking_or_io_failure = matches!(
        &err,
        rusqlite::Error::SqliteFailure(sqlite_err, _)
            if matches!(
                sqlite_err.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::SystemIoFailure
            )
    );
    if is_locking_or_io_failure {
        Error::ConnectionUnreliable(err)
    } else {
        Error::Sqlite(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlite_failure(code: ErrorCode) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code,
                extended_code: 0,
            },
            Some("simulated for this test".to_string()),
        )
    }

    #[test]
    fn wrap_unreliable_connection_error_catches_database_busy() {
        let err = wrap_unreliable_connection_error(sqlite_failure(ErrorCode::DatabaseBusy));
        assert!(matches!(err, Error::ConnectionUnreliable(_)));
    }

    #[test]
    fn wrap_unreliable_connection_error_catches_database_locked() {
        let err = wrap_unreliable_connection_error(sqlite_failure(ErrorCode::DatabaseLocked));
        assert!(matches!(err, Error::ConnectionUnreliable(_)));
    }

    #[test]
    fn wrap_unreliable_connection_error_catches_system_io_failure() {
        let err = wrap_unreliable_connection_error(sqlite_failure(ErrorCode::SystemIoFailure));
        assert!(matches!(err, Error::ConnectionUnreliable(_)));
    }

    #[test]
    fn wrap_unreliable_connection_error_leaves_other_sqlite_errors_alone() {
        let err = wrap_unreliable_connection_error(sqlite_failure(ErrorCode::ConstraintViolation));
        assert!(matches!(err, Error::Sqlite(_)));
    }

    #[test]
    fn wrap_unreliable_connection_error_leaves_non_sqlite_failure_errors_alone() {
        // Not every rusqlite::Error is a SqliteFailure - e.g. a column-type mismatch reading a
        // pragma result. Those must fall through to Error::Sqlite too, not be miscategorized.
        let err = wrap_unreliable_connection_error(rusqlite::Error::InvalidColumnIndex(0));
        assert!(matches!(err, Error::Sqlite(_)));
    }
}
