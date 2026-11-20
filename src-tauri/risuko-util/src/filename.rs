const MAX_SAFE_FILENAME_BYTES: usize = 230;
const MAX_EXTENSION_BYTES: usize = 16;

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
    let capped = truncate_filename(
        cleaned
            .trim_start_matches(['.', ' '])
            .trim_end_matches(is_trailing_junk),
        MAX_SAFE_FILENAME_BYTES,
    );
    let name = capped.trim_end_matches(is_trailing_junk);
    if name.is_empty() || is_windows_device_name(name) {
        return fallback.to_string();
    }
    name.to_string()
}

fn is_trailing_junk(c: char) -> bool {
    c == '.' || c.is_whitespace()
}

pub fn is_invisible_format_char(c: char) -> bool {
    matches!(
        c,
        '\u{061C}'
            | '\u{200B}'
            | '\u{200E}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{206F}'
            | '\u{FEFF}'
    )
}

pub fn truncate_filename(name: &str, max_bytes: usize) -> String {
    if name.len() <= max_bytes {
        return name.to_string();
    }
    if let Some(dot) = name.rfind('.') {
        let (stem, ext) = name.split_at(dot);
        if dot > 0 && ext.len() <= MAX_EXTENSION_BYTES && ext.len() < max_bytes {
            let head = char_prefix(stem, max_bytes - ext.len());
            if !head.is_empty() {
                return format!("{head}{ext}");
            }
        }
    }
    char_prefix(name, max_bytes).to_string()
}

fn char_prefix(s: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(s.len());
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
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
    fn truncate_filename_never_exceeds_the_budget() {
        let long_ext = format!("{}.{}", "a".repeat(20), "b".repeat(14));
        assert_eq!(truncate_filename(&long_ext, 10), "a".repeat(10));
        assert_eq!(truncate_filename("name.mkv", 4), "name");
        assert_eq!(truncate_filename("name.mkv", 0), "");
        assert_eq!(truncate_filename("abcdefghij.mkv", 8), "abcd.mkv");
        assert_eq!(truncate_filename("\u{6f22}.mkv", 5), "\u{6f22}.m");
        assert_eq!(
            truncate_filename("\u{6f22}\u{6f22}.mkv", 6),
            "\u{6f22}\u{6f22}"
        );
    }

    #[test]
    fn safe_filename_keeps_joiners_in_legitimate_names() {
        for name in [
            "\u{645}\u{6cc}\u{200C}\u{62e}\u{648}\u{627}\u{647}\u{645}.txt",
            "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}.png",
            "\u{915}\u{94d}\u{200D}\u{937}.txt",
        ] {
            assert_eq!(safe_filename(name, "x"), name);
        }
        assert!(!is_invisible_format_char('\u{200C}'));
        assert!(!is_invisible_format_char('\u{200D}'));
        for c in ['\u{200B}', '\u{200E}', '\u{200F}', '\u{206A}', '\u{206F}'] {
            assert!(is_invisible_format_char(c), "{c:?}");
        }
        assert_eq!(
            safe_filename("a\u{200E}b\u{200F}c\u{206A}d", "x"),
            "a_b_c_d"
        );
    }

    #[test]
    fn safe_filename_trims_and_rechecks_after_the_cap() {
        let spaced = safe_filename(&format!("{} tail", "a".repeat(229)), "x");
        assert_eq!(spaced, "a".repeat(229));

        let dotted = safe_filename(&format!("{}.{}", "a".repeat(229), "b".repeat(30)), "x");
        assert_eq!(dotted, "a".repeat(229));

        assert_eq!(
            safe_filename(&format!("CON{}x", " ".repeat(300)), "fallback"),
            "fallback"
        );
        assert_eq!(
            safe_filename(&format!("{}x", "\u{3000}".repeat(100)), "fallback"),
            "fallback"
        );
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
