use std::fmt;

/// Chunking method used when splitting file content into chunks for deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chunking {
    /// Content-defined chunking (rolling-fingerprint based).
    Cdc,
    /// No chunking: each file is stored as a single chunk.
    None,
}

impl Chunking {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Chunking::Cdc => "cdc",
            Chunking::None => "none",
        }
    }

    /// Parses a `chunking` value read back from `repository_settings`. Any value
    /// other than `"cdc"`/`"none"` would violate that column's `CHECK` constraint,
    /// so it can only occur if the database itself is corrupt.
    pub(crate) fn from_db_str(s: &str) -> Self {
        match s {
            "cdc" => Chunking::Cdc,
            "none" => Chunking::None,
            other => {
                unreachable!("repository_settings.chunking CHECK constraint violated: {other:?}")
            }
        }
    }
}

impl fmt::Display for Chunking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Valid range for [`RepositorySettings::cdc_target_size_bits`].
///
/// Average CDC chunk size is `2^cdc_target_size_bits` bytes; values outside this
/// range are technically possible but not sensible (too small: excessive metadata
/// overhead; too large: chunking becomes ineffective).
pub const CDC_TARGET_SIZE_BITS_RANGE: std::ops::RangeInclusive<u32> = 10..=30;

/// Error returned by [`RepositorySettings::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsError {
    /// `cdc_target_size_bits` is outside [`CDC_TARGET_SIZE_BITS_RANGE`].
    CdcTargetSizeBitsOutOfRange(u32),
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CdcTargetSizeBitsOutOfRange(value) => write!(
                f,
                "CDC target size (bits) {value} is not in range {}-{}",
                CDC_TARGET_SIZE_BITS_RANGE.start(),
                CDC_TARGET_SIZE_BITS_RANGE.end()
            ),
        }
    }
}

impl std::error::Error for SettingsError {}

/// User-defined per-repository settings, set once when a repository is created.
///
/// Changing these later would require a migration, since they affect how existing
/// data is interpreted (chunk boundaries, content hashes). The hash algorithm
/// (blake3) is not part of this: it's currently fixed in code, not user-configurable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositorySettings {
    cdc_target_size_bits: u32,
    chunking: Chunking,
}

impl RepositorySettings {
    /// Creates validated repository settings.
    ///
    /// Returns [`SettingsError::CdcTargetSizeBitsOutOfRange`] if `cdc_target_size_bits`
    /// is outside [`CDC_TARGET_SIZE_BITS_RANGE`].
    pub fn new(cdc_target_size_bits: u32, chunking: Chunking) -> Result<Self, SettingsError> {
        if !CDC_TARGET_SIZE_BITS_RANGE.contains(&cdc_target_size_bits) {
            return Err(SettingsError::CdcTargetSizeBitsOutOfRange(
                cdc_target_size_bits,
            ));
        }
        Ok(Self {
            cdc_target_size_bits,
            chunking,
        })
    }

    pub fn cdc_target_size_bits(&self) -> u32 {
        self.cdc_target_size_bits
    }

    pub fn chunking(&self) -> Chunking {
        self.chunking
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_accepts_the_valid_range() {
        assert!(RepositorySettings::new(10, Chunking::Cdc).is_ok());
        assert!(RepositorySettings::new(30, Chunking::None).is_ok());
    }

    #[test]
    fn new_rejects_values_outside_the_valid_range() {
        assert_eq!(
            RepositorySettings::new(9, Chunking::Cdc),
            Err(SettingsError::CdcTargetSizeBitsOutOfRange(9))
        );
        assert_eq!(
            RepositorySettings::new(31, Chunking::Cdc),
            Err(SettingsError::CdcTargetSizeBitsOutOfRange(31))
        );
    }
}
