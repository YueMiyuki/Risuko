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
    let trimmed = cleaned
        .trim_start_matches(['.', ' '])
        .trim_end_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    if is_windows_device_name(trimmed) {
        return fallback.to_string();
    }
    trimmed.to_string()
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
