use crate::browser::chromium::Cookie;
use crate::utils::host::cookie_covers_host;
use crate::utils::{paths, time};
use eyre::{bail, Result};

pub fn is_available() -> bool {
    find_cookie_file().is_some()
}

fn find_cookie_file() -> Option<std::path::PathBuf> {
    paths::find_first_existing(&[
        "~/Library/Cookies/Cookies.binarycookies",
        "~/Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies",
    ])
}

pub fn extract_cookies(host: Option<&str>) -> Result<Vec<Cookie>> {
    let cookie_db = match find_cookie_file() {
        Some(p) => p,
        None => bail!("safari cookie file not found"),
    };
    tracing::debug!(target: "risuko_cookies", "safari: using db path {}", cookie_db.display());

    let data = std::fs::read(&cookie_db)?;
    let raw_cookies = parse_binary_cookies(&data)?;

    tracing::debug!(target: "risuko_cookies", "safari: total cookies parsed = {}", raw_cookies.len());

    let mut cookies = Vec::new();
    let mut skipped_domain = 0usize;

    for raw in raw_cookies {
        if let Some(request_host) = host {
            if !cookie_covers_host(request_host, &raw.domain) {
                tracing::trace!(target: "risuko_cookies", "safari: skip cookie '{}' domain={} (does not cover {})", raw.name, raw.domain, request_host);
                skipped_domain += 1;
                continue;
            }
        }
        tracing::trace!(target: "risuko_cookies", "safari: keep cookie '{}' domain={} value_len={}", raw.name, raw.domain, raw.value.len());
        cookies.push(Cookie {
            name: raw.name,
            value: raw.value,
            domain: raw.domain,
            path: raw.path,
            secure: raw.secure,
            http_only: raw.http_only,
            expires: raw.expires,
        });
    }

    tracing::debug!(target: "risuko_cookies",
        "safari: host_filter={:?} -> kept={} skipped_domain={}",
        host, cookies.len(), skipped_domain
    );

    Ok(cookies)
}

struct RawSafariCookie {
    name: String,
    value: String,
    domain: String,
    path: String,
    secure: bool,
    http_only: bool,
    expires: Option<u64>,
}

fn parse_binary_cookies(data: &[u8]) -> Result<Vec<RawSafariCookie>> {
    if data.len() < 8 {
        bail!("binary cookies file too short");
    }

    if &data[..4] != b"cook" {
        bail!("invalid binary cookies magic");
    }

    let num_pages = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
    if num_pages == 0 {
        return Ok(Vec::new());
    }

    let header_size = 8 + num_pages * 4;
    if data.len() < header_size {
        bail!("binary cookies header truncated");
    }

    let mut cookies = Vec::new();
    for i in 0..num_pages {
        let off = 8 + i * 4;
        let page_offset = u32::from_be_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        if page_offset >= data.len() {
            bail!("page offset out of bounds");
        }
        parse_page(&data[page_offset..], &mut cookies)?;
    }

    Ok(cookies)
}

fn parse_page(page: &[u8], cookies: &mut Vec<RawSafariCookie>) -> Result<()> {
    if page.len() < 8 {
        bail!("page too short");
    }

    let num_cookies = u32::from_be_bytes(page[4..8].try_into().unwrap()) as usize;
    if num_cookies == 0 {
        return Ok(());
    }

    let header_size = 8 + num_cookies * 4;
    if page.len() < header_size {
        bail!("page header truncated");
    }

    for i in 0..num_cookies {
        let off = 8 + i * 4;
        let cookie_offset = u32::from_be_bytes(page[off..off + 4].try_into().unwrap()) as usize;
        if cookie_offset >= page.len() {
            continue;
        }
        if let Err(e) = parse_cookie(&page[cookie_offset..], cookies) {
            tracing::debug!(target: "risuko_cookies", "safari: skipping cookie record: {}", e);
        }
    }

    Ok(())
}

fn parse_cookie(cookie: &[u8], cookies: &mut Vec<RawSafariCookie>) -> Result<()> {
    if cookie.len() < 44 {
        bail!("cookie record too short");
    }

    let flags = u32::from_be_bytes(cookie[4..8].try_into().unwrap());
    let url_offset = u32::from_be_bytes(cookie[12..16].try_into().unwrap()) as usize;
    let name_offset = u32::from_be_bytes(cookie[16..20].try_into().unwrap()) as usize;
    let path_offset = u32::from_be_bytes(cookie[20..24].try_into().unwrap()) as usize;
    let value_offset = u32::from_be_bytes(cookie[24..28].try_into().unwrap()) as usize;

    let expiry_bytes: [u8; 8] = cookie[28..36].try_into().unwrap();
    let expiry = f64::from_be_bytes(expiry_bytes);

    let domain = read_cstr(cookie, url_offset)?;
    let name = read_cstr(cookie, name_offset)?;
    let path = read_cstr(cookie, path_offset)?;
    let value = read_cstr(cookie, value_offset)?;

    cookies.push(RawSafariCookie {
        name,
        value,
        domain,
        path,
        secure: (flags & 0x01) != 0,
        http_only: (flags & 0x04) != 0,
        expires: time::safari_to_unix(expiry),
    });

    Ok(())
}

fn read_cstr(data: &[u8], offset: usize) -> Result<String> {
    if offset >= data.len() {
        bail!("string offset out of bounds");
    }
    let end = data[offset..]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(data.len() - offset);
    Ok(String::from_utf8_lossy(&data[offset..offset + end]).to_string())
}

#[cfg(test)]
mod tests {
    use super::parse_page;

    #[test]
    fn bad_record_does_not_abort_page() {
        let mut page = vec![0u8; 8];
        page[4..8].copy_from_slice(&2u32.to_be_bytes());
        page.extend_from_slice(&999u32.to_be_bytes());
        page.extend_from_slice(&16u32.to_be_bytes());
        page.extend_from_slice(&[0u8; 4]);
        let mut out = Vec::new();
        assert!(parse_page(&page, &mut out).is_ok());
        assert!(out.is_empty());
    }
}
