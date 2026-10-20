use std::time::{SystemTime, UNIX_EPOCH};

pub use risuko_util::is_windows_device_name;
pub(crate) use risuko_util::safe_filename;

pub(crate) const ERROR_SNIPPET_BYTES: usize = 512;

pub(crate) const RESPONSE_BODY_LIMIT: usize = 64 * 1024;

pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn atomic_saturating_sub(counter: &std::sync::atomic::AtomicU64, amount: u64) {
    use std::sync::atomic::Ordering;
    let mut current = counter.load(Ordering::Relaxed);
    while let Err(actual) = counter.compare_exchange_weak(
        current,
        current.saturating_sub(amount),
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        current = actual;
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn dedup_path(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }

    let (stem, ext) = match name.rfind('.') {
        Some(dot) if dot > 0 => (&name[..dot], &name[dot..]),
        _ => (name, ""),
    };

    for n in 1u32.. {
        let numbered = if ext.is_empty() {
            format!("{stem}.{n}")
        } else {
            format!("{stem}.{n}{ext}")
        };
        let path = dir.join(&numbered);
        if !path.exists() {
            return path;
        }
    }
    candidate
}
