use serde_json::Value;
use std::path::{Component, Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLimits {
    pub max_entries: u64,
    pub max_expanded_bytes: u64,
    pub max_entry_bytes: u64,
    pub max_nesting_depth: u32,
    pub max_compression_ratio: u64,
    pub free_space_reserve_bytes: u64,
    pub max_active_seconds: u64,
}

impl ArchiveLimits {
    pub const fn desktop_defaults() -> Self {
        Self {
            max_entries: 500_000,
            max_expanded_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            max_entry_bytes: 512 * 1024 * 1024 * 1024,
            max_nesting_depth: 16,
            max_compression_ratio: 1000,
            free_space_reserve_bytes: 10 * 1024 * 1024 * 1024,
            max_active_seconds: 6 * 60 * 60,
        }
    }

    pub const fn android_defaults() -> Self {
        Self {
            max_entries: 100_000,
            max_expanded_bytes: 256 * 1024 * 1024 * 1024,
            max_entry_bytes: 64 * 1024 * 1024 * 1024,
            max_nesting_depth: 16,
            max_compression_ratio: 1000,
            free_space_reserve_bytes: 2 * 1024 * 1024 * 1024,
            max_active_seconds: 2 * 60 * 60,
        }
    }

    pub const fn hard_ceiling(self) -> Self {
        Self {
            max_entries: self.max_entries.saturating_mul(4),
            max_expanded_bytes: self.max_expanded_bytes.saturating_mul(4),
            max_entry_bytes: self.max_entry_bytes.saturating_mul(4),
            max_nesting_depth: self.max_nesting_depth.saturating_mul(4),
            max_compression_ratio: self.max_compression_ratio.saturating_mul(4),
            free_space_reserve_bytes: self.free_space_reserve_bytes,
            max_active_seconds: self.max_active_seconds.saturating_mul(4),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchiveUsage {
    pub entries: u64,
    pub expanded_bytes: u64,
    pub max_entry_bytes: u64,
    pub nesting_depth: u32,
    pub active_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveSafetyError {
    UnsafePath,
    EntryCount,
    ExpandedBytes,
    EntryBytes,
    NestingDepth,
    CompressionRatio,
    FreeSpaceReserve,
    ActiveTime,
    OverrideConfirmation,
    HardCeiling,
}

pub fn validate_limits_override(
    defaults: ArchiveLimits,
    requested: ArchiveLimits,
    confirmed: bool,
) -> Result<ArchiveLimits, ArchiveSafetyError> {
    let ceiling = defaults.hard_ceiling();
    if requested.max_entries == 0
        || requested.max_expanded_bytes == 0
        || requested.max_entry_bytes == 0
        || requested.max_nesting_depth == 0
        || requested.max_compression_ratio == 0
        || requested.max_active_seconds == 0
    {
        return Err(ArchiveSafetyError::HardCeiling);
    }
    if requested.max_entries > ceiling.max_entries
        || requested.max_expanded_bytes > ceiling.max_expanded_bytes
        || requested.max_entry_bytes > ceiling.max_entry_bytes
        || requested.max_nesting_depth > ceiling.max_nesting_depth
        || requested.max_compression_ratio > ceiling.max_compression_ratio
        || requested.max_active_seconds > ceiling.max_active_seconds
    {
        return Err(ArchiveSafetyError::HardCeiling);
    }
    if !confirmed
        && (requested.max_entries > defaults.max_entries
            || requested.max_expanded_bytes > defaults.max_expanded_bytes
            || requested.max_entry_bytes > defaults.max_entry_bytes
            || requested.max_nesting_depth > defaults.max_nesting_depth
            || requested.max_compression_ratio > defaults.max_compression_ratio
            || requested.free_space_reserve_bytes < defaults.free_space_reserve_bytes
            || requested.max_active_seconds > defaults.max_active_seconds)
    {
        return Err(ArchiveSafetyError::OverrideConfirmation);
    }
    Ok(requested)
}

pub fn validate_limits_override_value(
    defaults: ArchiveLimits,
    value: &Value,
    confirmed: bool,
) -> Result<ArchiveLimits, ArchiveSafetyError> {
    let object = value.as_object().ok_or(ArchiveSafetyError::HardCeiling)?;
    let read = |key: &str, fallback: u64| -> Result<u64, ArchiveSafetyError> {
        match object.get(key) {
            None => Ok(fallback),
            Some(value) => value.as_u64().ok_or(ArchiveSafetyError::HardCeiling),
        }
    };
    let requested = ArchiveLimits {
        max_entries: read("maxEntries", defaults.max_entries)?,
        max_expanded_bytes: read("maxExpandedBytes", defaults.max_expanded_bytes)?,
        max_entry_bytes: read("maxEntryBytes", defaults.max_entry_bytes)?,
        max_nesting_depth: u32::try_from(read(
            "maxNestingDepth",
            defaults.max_nesting_depth as u64,
        )?)
        .map_err(|_| ArchiveSafetyError::HardCeiling)?,
        max_compression_ratio: read("maxCompressionRatio", defaults.max_compression_ratio)?,
        free_space_reserve_bytes: read("freeSpaceReserveBytes", defaults.free_space_reserve_bytes)?,
        max_active_seconds: read("maxActiveSeconds", defaults.max_active_seconds)?,
    };
    validate_limits_override(defaults, requested, confirmed)
}

pub fn validate_member_path(path: &str) -> Result<(), ArchiveSafetyError> {
    let normalized = path.trim_end_matches('/');
    if normalized.is_empty()
        || normalized.contains('\0')
        || normalized.contains('\\')
        || normalized.contains(':')
        || normalized.starts_with("//")
        || Path::new(normalized).is_absolute()
    {
        return Err(ArchiveSafetyError::UnsafePath);
    }
    if normalized
        .split('/')
        .any(|part| part.is_empty() || part == "..")
    {
        return Err(ArchiveSafetyError::UnsafePath);
    }
    if Path::new(normalized)
        .components()
        .any(|component| matches!(component, Component::RootDir | Component::Prefix(_)))
    {
        return Err(ArchiveSafetyError::UnsafePath);
    }
    if path.chars().any(|ch| ch.is_control()) {
        return Err(ArchiveSafetyError::UnsafePath);
    }
    if path.split('/').any(super::util::is_windows_device_name) {
        return Err(ArchiveSafetyError::UnsafePath);
    }
    Ok(())
}

pub fn check_limits(
    limits: ArchiveLimits,
    usage: ArchiveUsage,
    compressed_bytes: u64,
    free_space_bytes: Option<u64>,
) -> Result<(), ArchiveSafetyError> {
    if usage.entries > limits.max_entries {
        return Err(ArchiveSafetyError::EntryCount);
    }
    if usage.expanded_bytes > limits.max_expanded_bytes {
        return Err(ArchiveSafetyError::ExpandedBytes);
    }
    if usage.max_entry_bytes > limits.max_entry_bytes {
        return Err(ArchiveSafetyError::EntryBytes);
    }
    if usage.nesting_depth > limits.max_nesting_depth {
        return Err(ArchiveSafetyError::NestingDepth);
    }
    if usage.expanded_bytes > 0 && compressed_bytes == 0 {
        return Err(ArchiveSafetyError::CompressionRatio);
    }
    if compressed_bytes > 0 {
        let max_expanded = compressed_bytes.saturating_mul(limits.max_compression_ratio);
        if usage.expanded_bytes > max_expanded {
            return Err(ArchiveSafetyError::CompressionRatio);
        }
    }
    if free_space_bytes.is_some_and(|free| free < limits.free_space_reserve_bytes) {
        return Err(ArchiveSafetyError::FreeSpaceReserve);
    }
    if usage.active_seconds > limits.max_active_seconds {
        return Err(ArchiveSafetyError::ActiveTime);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ArchiveLimits {
        ArchiveLimits {
            max_entries: 2,
            max_expanded_bytes: 100,
            max_entry_bytes: 80,
            max_nesting_depth: 2,
            max_compression_ratio: 10,
            free_space_reserve_bytes: 20,
            max_active_seconds: 60,
        }
    }

    #[test]
    fn rejects_unsafe_member_paths() {
        for path in [
            "../x",
            "/tmp/x",
            "C:/x",
            "a\\b",
            "CON.txt",
            "NUL ",
            "CON .txt",
            "a/\u{7f}b",
        ] {
            assert_eq!(
                validate_member_path(path),
                Err(ArchiveSafetyError::UnsafePath)
            );
        }
        assert!(validate_member_path("folder/file.txt").is_ok());
    }

    #[test]
    fn enforces_task_wide_limits() {
        let mut usage = ArchiveUsage {
            entries: 3,
            ..Default::default()
        };
        assert_eq!(
            check_limits(limits(), usage, 1, Some(100)),
            Err(ArchiveSafetyError::EntryCount)
        );
        usage.entries = 1;
        usage.expanded_bytes = 101;
        assert_eq!(
            check_limits(limits(), usage, 1, Some(100)),
            Err(ArchiveSafetyError::ExpandedBytes)
        );
    }

    #[test]
    fn requires_confirmation_for_higher_finite_limits() {
        let defaults = limits();
        let mut requested = defaults;
        requested.max_entries += 1;
        assert_eq!(
            validate_limits_override(defaults, requested, false),
            Err(ArchiveSafetyError::OverrideConfirmation)
        );
        assert_eq!(
            validate_limits_override(defaults, requested, true),
            Ok(requested)
        );
        requested.max_entries = defaults.max_entries * 5;
        assert_eq!(
            validate_limits_override(defaults, requested, true),
            Err(ArchiveSafetyError::HardCeiling)
        );
    }

    #[test]
    fn requires_confirmation_to_reduce_free_space_reserve() {
        let defaults = limits();
        let mut requested = defaults;
        requested.free_space_reserve_bytes = 0;

        assert_eq!(
            validate_limits_override(defaults, requested, false),
            Err(ArchiveSafetyError::OverrideConfirmation)
        );
        assert_eq!(
            validate_limits_override(defaults, requested, true),
            Ok(requested)
        );
    }

    #[test]
    fn rejects_nesting_depth_values_that_do_not_fit_the_engine_type() {
        let defaults = limits();
        let value = serde_json::json!({ "maxNestingDepth": u64::from(u32::MAX) + 1 });
        assert_eq!(
            validate_limits_override_value(defaults, &value, true),
            Err(ArchiveSafetyError::HardCeiling)
        );
    }

    #[test]
    fn compression_ratio_uses_exact_overflow_safe_comparison() {
        let mut configured = limits();
        configured.max_expanded_bytes = u64::MAX;
        let mut usage = ArchiveUsage {
            expanded_bytes: 101,
            ..Default::default()
        };
        assert_eq!(
            check_limits(configured, usage, 10, Some(100)),
            Err(ArchiveSafetyError::CompressionRatio)
        );
        usage.expanded_bytes = 1;
        assert_eq!(
            check_limits(configured, usage, 0, Some(100)),
            Err(ArchiveSafetyError::CompressionRatio)
        );
        usage.expanded_bytes = 0;
        assert!(check_limits(configured, usage, 0, Some(100)).is_ok());
    }
}
