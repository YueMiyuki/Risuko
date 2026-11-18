const MAX_SAFE_FILENAME_BYTES: usize = 230;

pub fn safe_filename(name: &str, fallback: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_control()
                || is_invisible_format_char(c)
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
    let trimmed = cleaned
        .trim_start_matches(['.', ' '])
        .trim_end_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    if is_windows_device_name(trimmed) {
        return fallback.to_string();
    }
    truncate_filename(trimmed, MAX_SAFE_FILENAME_BYTES)
}

pub fn is_invisible_format_char(c: char) -> bool {
    matches!(
        c,
        '\u{061C}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{2069}'
            | '\u{FEFF}'
    )
}

pub fn truncate_filename(name: &str, max_bytes: usize) -> String {
    if name.len() <= max_bytes {
        return name.to_string();
    }
    let (stem, ext) = match name.rfind('.') {
        Some(dot) if dot > 0 && name.len() - dot <= 16 => name.split_at(dot),
        _ => (name, ""),
    };
    let mut keep = max_bytes.saturating_sub(ext.len()).min(stem.len());
    while !stem.is_char_boundary(keep) {
        keep -= 1;
    }
    format!("{}{ext}", &stem[..keep])
}

pub fn is_windows_device_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).trim();
    let upper = stem.to_ascii_uppercase();
    if matches!(
        upper.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) {
        return true;
    }
    let port = upper
        .strip_prefix("COM")
        .or_else(|| upper.strip_prefix("LPT"));
    matches!(
        port,
        Some("1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³")
    )
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
        assert_eq!(safe_filename("foo. .", "fallback"), "foo");
        assert_eq!(safe_filename(" .", "fallback"), "fallback");
        assert_eq!(safe_filename("   ", "fallback"), "fallback");
        assert_eq!(safe_filename("con.tar.gz", "fallback"), "fallback");
        assert_eq!(safe_filename("lpt1 .txt", "fallback"), "fallback");
        assert_eq!(safe_filename(" CON", "fallback"), "fallback");
        assert_eq!(safe_filename(" COM1.txt", "fallback"), "fallback");
        assert_eq!(safe_filename("console.log", "fallback"), "console.log");
    }

    #[test]
    fn safe_filename_neutralises_direction_overrides() {
        assert_eq!(
            safe_filename("invoice\u{202E}fdp.exe", "x"),
            "invoice_fdp.exe"
        );
        assert_eq!(safe_filename("a\u{200B}b\u{FEFF}.txt", "x"), "a_b_.txt");
    }

    #[test]
    fn safe_filename_caps_length_on_a_char_boundary() {
        let name = safe_filename(&format!("{}.mkv", "\u{6f22}".repeat(200)), "x");
        assert!(name.len() <= MAX_SAFE_FILENAME_BYTES);
        assert!(name.ends_with(".mkv"));
        assert_eq!(truncate_filename("short.txt", 230), "short.txt");
        assert_eq!(truncate_filename(&"a".repeat(300), 10), "a".repeat(10));
    }

    #[test]
    fn safe_filename_superscript_port_device_names() {
        assert_eq!(safe_filename("COM¹", "fallback"), "fallback");
        assert_eq!(safe_filename("com².txt", "fallback"), "fallback");
        assert_eq!(safe_filename("LPT³", "fallback"), "fallback");
        assert_eq!(safe_filename("lpt¹.log", "fallback"), "fallback");
        assert_eq!(safe_filename("COM⁴", "fallback"), "COM⁴");
    }

    #[test]
    fn is_windows_device_name_leading_spaces() {
        assert!(is_windows_device_name(" CON"));
        assert!(is_windows_device_name(" COM1.txt"));
        assert!(is_windows_device_name("  lpt9 .txt"));
        assert!(is_windows_device_name(" COM²"));
        assert!(is_windows_device_name("CON"));
        assert!(!is_windows_device_name(" console.log"));
        assert!(!is_windows_device_name("COM0"));
        assert!(!is_windows_device_name("COM10"));
    }
}
