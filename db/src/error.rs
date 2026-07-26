use std::fmt;
use std::path::PathBuf;

use crate::SettingsError;

/// Error returned by this crate's repository operations.
#[derive(Debug)]
pub enum Error {
    /// The repository directory passed to [`crate::init_repository`] already exists.
    RepositoryAlreadyExists(PathBuf),
    /// The provided [`crate::RepositorySettings`] failed validation.
    InvalidSettings(SettingsError),
    /// Creating the repository directory layout failed.
    Io(std::io::Error),
    /// A SQLite operation failed.
    Sqlite(rusqlite::Error),
    /// Applying database migrations failed.
    Migration(rusqlite_migration::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RepositoryAlreadyExists(path) => {
                write!(f, "repository already exists: {}", path.display())
            }
            Self::InvalidSettings(err) => write!(f, "invalid repository settings: {err}"),
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Sqlite(err) => write!(f, "SQLite error: {err}"),
            Self::Migration(err) => write!(f, "database migration error: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RepositoryAlreadyExists(_) => None,
            Self::InvalidSettings(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Sqlite(err) => Some(err),
            Self::Migration(err) => Some(err),
        }
    }
}

impl From<SettingsError> for Error {
    fn from(err: SettingsError) -> Self {
        Self::InvalidSettings(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Sqlite(err)
    }
}

impl From<rusqlite_migration::Error> for Error {
    fn from(err: rusqlite_migration::Error) -> Self {
        Self::Migration(err)
    }
}
