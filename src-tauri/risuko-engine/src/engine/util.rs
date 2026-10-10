use std::time::{SystemTime, UNIX_EPOCH};
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

pub(crate) fn safe_filename(name: &str, fallback: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_control()
                || matches!(
                    c,
                    '/' | '\\' | ':' | '<' | '>' | '|' | '?' | '*' | '"' | '\0'
                )
            {
                '_'
            } else {
                c
            }
        })
        .collect();
    let trimmed = cleaned.trim_end().trim_matches('.');
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    if is_windows_device_name(trimmed) {
        return fallback.to_string();
    }
    trimmed.to_string()
}

pub fn is_windows_device_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).trim_end();
    let upper = stem.to_ascii_uppercase();
    if matches!(
        upper.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) {
        return true;
    }
    if upper.len() == 4 {
        let b = upper.as_bytes();
        let port = b[3];
        if (b[0..3] == *b"COM" || b[0..3] == *b"LPT") && (b'1'..=b'9').contains(&port) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_filename_replaces_reserved_and_falls_back() {
        assert_eq!(safe_filename("a/b:c", "x"), "a_b_c");
        assert_eq!(safe_filename("...", "fallback"), "fallback");
        assert_eq!(safe_filename("", "fallback"), "fallback");
        assert_eq!(safe_filename("normal.txt", "x"), "normal.txt");
    }

    #[test]
    fn safe_filename_windows_device_names_and_trailing_spaces() {
        assert_eq!(safe_filename("CON", "fallback"), "fallback");
        assert_eq!(safe_filename("NUL.txt", "fallback"), "fallback");
        assert_eq!(safe_filename("com9", "fallback"), "fallback");
        assert_eq!(safe_filename("foo  ", "fallback"), "foo");
        assert_eq!(safe_filename("con.tar.gz", "fallback"), "fallback");
        assert_eq!(safe_filename("lpt1 .txt", "fallback"), "fallback");
        assert_eq!(safe_filename("console.log", "fallback"), "console.log");
    }
}
