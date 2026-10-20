pub fn safe_filename(name: &str, fallback: &str) -> String {
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
