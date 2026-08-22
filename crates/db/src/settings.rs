//! Repository settings fixed at creation - DESIGN-METADATA-009 in
//! `docs/design/metadata-schema-with-contents-table.md`.

/// Settings fixed once, at repository creation, and never changed afterward
/// (REQ-STORAGE-003, REQ-STORAGE-008).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositorySettings {
    cdc_target_size_bits: Option<u32>,
    creation_time_millis: i64,
}

impl RepositorySettings {
    /// `cdc_target_size_bits`: `None` selects whole-file chunking, `Some(bits)`
    /// selects content-defined chunking with that target size - mirrors
    /// `cdc::ChunkerConfig`'s own shape exactly. Not validated here:
    /// validating a user-supplied value is the caller's job, reusing
    /// `cdc::ChunkerConfig::new` directly (DESIGN-METADATA-009's "CLI
    /// validation" section) - the `repository_settings` table's own `CHECK`
    /// constraint is the last-resort backstop, not the primary check.
    ///
    /// `creation_time_millis`: milliseconds since the Unix epoch
    /// (DESIGN-METADATA-011), the moment this repository was actually
    /// created - computed by the caller, not here, since "now" is not this
    /// type's concern.
    pub fn new(cdc_target_size_bits: Option<u32>, creation_time_millis: i64) -> Self {
        Self {
            cdc_target_size_bits,
            creation_time_millis,
        }
    }

    pub fn cdc_target_size_bits(&self) -> Option<u32> {
        self.cdc_target_size_bits
    }

    pub fn creation_time_millis(&self) -> i64 {
        self.creation_time_millis
    }
}
