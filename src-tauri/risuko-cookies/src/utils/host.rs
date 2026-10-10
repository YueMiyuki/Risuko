pub fn cookie_covers_host(request_host: &str, cookie_host: &str) -> bool {
    let r = request_host.to_lowercase();
    let c = cookie_host.to_lowercase();

    if let Some(domain) = c.strip_prefix('.') {
        r == domain || r.strip_suffix(domain).is_some_and(|p| p.ends_with('.'))
    } else {
        r == c
    }
}

pub fn host_key_candidates(request_host: &str) -> Vec<String> {
    let h = request_host.trim_start_matches('.').to_lowercase();
    let mut out = vec![h.clone(), format!(".{h}")];
    let mut rest = h.as_str();
    while let Some((_, parent)) = rest.split_once('.') {
        if parent.is_empty() {
            break;
        }
        out.push(format!(".{parent}"));
        rest = parent;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{cookie_covers_host, host_key_candidates};

    #[test]
    fn domain_cookie_with_dot_covers_subdomain() {
        assert!(cookie_covers_host("www.spigotmc.org", ".spigotmc.org"));
        assert!(cookie_covers_host("dl.spigotmc.org", ".spigotmc.org"));
        assert!(cookie_covers_host("spigotmc.org", ".spigotmc.org"));
    }

    #[test]
    fn host_only_cookie_exact_match() {
        assert!(cookie_covers_host("www.spigotmc.org", "www.spigotmc.org"));
        assert!(!cookie_covers_host("dl.spigotmc.org", "www.spigotmc.org"));
        assert!(!cookie_covers_host("www.spigotmc.org", "spigotmc.org"));
    }

    #[test]
    fn no_false_match_on_suffix() {
        assert!(!cookie_covers_host("notspigotmc.org", ".spigotmc.org"));
        assert!(!cookie_covers_host("evil.spigotmc.org", ".example.com"));
    }

    #[test]
    fn candidates_cover_host_and_parents() {
        assert_eq!(
            host_key_candidates("A.b.example.com"),
            [
                "a.b.example.com",
                ".a.b.example.com",
                ".b.example.com",
                ".example.com",
                ".com"
            ]
        );
    }
}
