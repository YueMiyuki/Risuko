use super::types::{Ed2kFileLink, Ed2kSource};

pub fn is_ed2k_uri(uri: &str) -> bool {
    uri.trim().to_lowercase().starts_with("ed2k://")
}

pub fn parse_ed2k_link(uri: &str) -> Result<Ed2kFileLink, String> {
    let trimmed = uri.trim();
    if !trimmed.to_lowercase().starts_with("ed2k://|file|") {
        return Err("Not a valid ed2k file link".to_string());
    }

    let body = &trimmed["ed2k://|file|".len()..];
    let body = body.strip_suffix("|/").unwrap_or(body);
    let body = body.strip_suffix('/').unwrap_or(body);

    let parts: Vec<&str> = body.split('|').collect();
    if parts.len() < 3 {
        return Err("ed2k link has too few fields".to_string());
    }

    let file_name = percent_encoding::percent_decode_str(parts[0])
        .decode_utf8()
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| parts[0].to_string());

    let file_size: u64 = parts[1]
        .parse()
        .map_err(|_| format!("Invalid file size: {}", parts[1]))?;

    if file_size == 0 {
        return Err("File size cannot be zero".to_string());
    }

    let hash_str = parts[2].to_lowercase();
    if hash_str.len() != 32 || !hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!("Invalid ed2k hash: {}", parts[2]));
    }

    let mut file_hash_bytes = [0u8; 16];
    for (i, byte) in file_hash_bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hash_str[i * 2..i * 2 + 2], 16)
            .map_err(|_| "Invalid hash hex digit")?;
    }

    let mut sources = Vec::new();
    let mut aich_hash = None;

    for part in parts.iter().skip(3) {
        if let Some(h) = part.strip_prefix("h=") {
            aich_hash = Some(h.to_string());
        } else if let Some(src) = part.strip_prefix("sources,") {
            for entry in src.split(',') {
                if let Some((ip, port_str)) = entry.split_once(':') {
                    if let Ok(port) = port_str.parse::<u16>() {
                        if port > 0 {
                            sources.push(Ed2kSource {
                                ip: ip.to_string(),
                                port,
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(Ed2kFileLink {
        file_name,
        file_size,
        file_hash: hash_str,
        file_hash_bytes,
        sources,
        aich_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_ed2k_uri() {
        assert!(is_ed2k_uri(
            "ed2k://|file|test.txt|1234|0123456789abcdef0123456789abcdef|/"
        ));
        assert!(is_ed2k_uri(
            "ED2K://|file|test.txt|1234|0123456789abcdef0123456789abcdef|/"
        ));
        assert!(is_ed2k_uri("  ed2k://|file|test.txt|1234|aaaa|/  "));
        assert!(!is_ed2k_uri("http://example.com"));
        assert!(!is_ed2k_uri("magnet:?xt=urn:btih:abc"));
        assert!(!is_ed2k_uri(""));
    }

    #[test]
    fn test_parse_basic_link() {
        let uri = "ed2k://|file|The_Two_Towers-The_Purist_Edit-Trailer.avi|14997504|965c013e991ee246d63d45ea71954c4d|/";
        let parsed = parse_ed2k_link(uri).unwrap();
        assert_eq!(
            parsed.file_name,
            "The_Two_Towers-The_Purist_Edit-Trailer.avi"
        );
        assert_eq!(parsed.file_size, 14997504);
        assert_eq!(parsed.file_hash, "965c013e991ee246d63d45ea71954c4d");
        assert!(parsed.sources.is_empty());
        assert!(parsed.aich_hash.is_none());
    }

    #[test]
    fn test_parse_link_with_sources() {
        let uri = "ed2k://|file|test.avi|14997504|965c013e991ee246d63d45ea71954c4d|/|sources,202.89.123.6:4662|/";
        let parsed = parse_ed2k_link(uri).unwrap();
        assert_eq!(parsed.sources.len(), 1);
        assert_eq!(parsed.sources[0].ip, "202.89.123.6");
        assert_eq!(parsed.sources[0].port, 4662);
    }

    #[test]
    fn test_parse_link_with_aich() {
        let uri = "ed2k://|file|test.avi|14997504|965c013e991ee246d63d45ea71954c4d|h=H52BRVWPBBTAED5NXQDH2RJDDAKRUWST|/";
        let parsed = parse_ed2k_link(uri).unwrap();
        assert_eq!(
            parsed.aich_hash.as_deref(),
            Some("H52BRVWPBBTAED5NXQDH2RJDDAKRUWST")
        );
    }

    #[test]
    fn test_parse_invalid_hash() {
        let uri = "ed2k://|file|test.txt|1234|not_a_valid_hash|/";
        assert!(parse_ed2k_link(uri).is_err());
    }

    #[test]
    fn test_parse_zero_size() {
        let uri = "ed2k://|file|test.txt|0|0123456789abcdef0123456789abcdef|/";
        assert!(parse_ed2k_link(uri).is_err());
    }

    #[test]
    fn test_parse_not_file_link() {
        let uri = "ed2k://|server|207.44.222.51|4242|/";
        assert!(parse_ed2k_link(uri).is_err());
    }
}
