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
}

impl fmt::Display for Chunking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Hash algorithm used to identify chunk content for deduplication.
///
/// Currently only `Blake3` is supported. This is modeled as an enum (rather than a
/// hard-coded constant) so a repository's chosen algorithm is recorded even though
/// there is only one valid value today, and so this can be extended without changing
/// the database schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    /// BLAKE3, truncated to 16 bytes (128 bits).
    Blake3,
}

impl HashAlgorithm {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            HashAlgorithm::Blake3 => "blake3",
        }
    }
}

impl fmt::Display for HashAlgorithm {
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
/// data is interpreted (chunk boundaries, content hashes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositorySettings {
    cdc_target_size_bits: u32,
    chunking: Chunking,
    hash_algorithm: HashAlgorithm,
}

impl RepositorySettings {
    /// Creates validated repository settings with the [`HashAlgorithm::Blake3`] hash
    /// algorithm.
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
            hash_algorithm: HashAlgorithm::Blake3,
        })
    }

    pub fn cdc_target_size_bits(&self) -> u32 {
        self.cdc_target_size_bits
    }

    pub fn chunking(&self) -> Chunking {
        self.chunking
    }

    pub fn hash_algorithm(&self) -> HashAlgorithm {
        self.hash_algorithm
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
