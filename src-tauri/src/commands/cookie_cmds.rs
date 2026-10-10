use risuko_cookies::{cookies_for_url, list_browsers, BrowserInfo, HostCookies};
use risuko_engine::engine;
use risuko_engine::engine::cookie_store::{
    cookies_to_header, eligible_cookies, CookieEntry, StoredCookie,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedCookieView {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
    pub expires: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedCookies {
    pub host: String,
    pub user_agent: String,
    pub cookie_header: String,
    pub count: usize,
    pub has_cf_clearance: bool,
    pub cookie_names: Vec<String>,
    pub cookies: Vec<ImportedCookieView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CookieEntryView {
    pub host: String,
    pub browser_id: String,
    pub user_agent: String,
    pub cookie_count: usize,
    pub imported_at: u64,
    pub last_validated_at: u64,
}

impl From<&CookieEntry> for CookieEntryView {
    fn from(e: &CookieEntry) -> Self {
        Self {
            host: e.host.clone(),
            browser_id: e.browser_id.clone(),
            user_agent: e.user_agent.clone(),
            cookie_count: e.cookies.len(),
            imported_at: e.imported_at,
            last_validated_at: e.last_validated_at,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryWithCookiesPayload {
    pub cookie: Option<String>,
    pub user_agent: Option<String>,
}

#[tauri::command]
pub async fn list_browsers_cmd() -> Vec<BrowserInfo> {
    list_browsers().await
}

#[tauri::command]
pub async fn import_browser_cookies(
    browser: String,
    url: String,
    persist: Option<bool>,
    user_agent: Option<String>,
) -> Result<ImportedCookies, String> {
    tracing::info!("import_browser_cookies: browser={browser}");
    let host_cookies = match cookies_for_url(&browser, &url).await {
        Ok(hc) => hc,
        Err(e) => {
            // Windows Chrome v20 app-bound cookies decrypt only as admin
            if e.contains(risuko_cookies::ELEVATION_REQUIRED) {
                tracing::info!(
                    "import_browser_cookies: app-bound cookies require elevation (browser={browser})"
                );
                return Err("ELEVATION_REQUIRED".to_string());
            }
            return Err(e);
        }
    };
    build_imported_cookies(host_cookies, &browser, persist, user_agent).await
}

#[tauri::command]
pub async fn import_browser_cookies_elevated(
    browser: String,
    url: String,
    persist: Option<bool>,
    user_agent: Option<String>,
) -> Result<ImportedCookies, String> {
    #[cfg(target_os = "windows")]
    {
        tracing::info!("import_browser_cookies_elevated: browser={browser}");
        let host_cookies = elevate::extract_cookies_elevated(&browser, &url).await?;
        build_imported_cookies(host_cookies, &browser, persist, user_agent).await
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (&url, &persist, &user_agent);
        Err(format!(
            "elevated cookie import for {browser} is only supported on Windows"
        ))
    }
}

async fn build_imported_cookies(
    host_cookies: HostCookies,
    browser: &str,
    persist: Option<bool>,
    user_agent: Option<String>,
) -> Result<ImportedCookies, String> {
    let HostCookies {
        host,
        user_agent: browser_user_agent,
        cookies,
    } = host_cookies;

    tracing::debug!(
        "import_browser_cookies: extractor returned {} cookie(s) for browser={browser}",
        cookies.len()
    );
    for c in cookies.iter() {
        tracing::debug!(
            "  imported cookie name={} domain={} path={} secure={} http_only={} value_len={}",
            c.name,
            c.domain,
            c.path,
            c.secure,
            c.http_only,
            c.value.len()
        );
    }

    if cookies.is_empty() {
        return Err(format!("no cookies found in {browser} for {host}"));
    }

    let extracted: Vec<StoredCookie> = cookies
        .iter()
        .map(|c| StoredCookie {
            name: c.name.clone(),
            value: c.value.clone(),
            domain: c.domain.clone(),
            path: c.path.clone(),
            secure: c.secure,
            http_only: c.http_only,
            expires: c.expires,
        })
        .collect();
    let stored: Vec<StoredCookie> = eligible_cookies(&extracted).into_iter().cloned().collect();
    if stored.is_empty() {
        return Err(format!(
            "no unexpired, header-safe cookies found in {browser} for {host}"
        ));
    }
    let cookie_header = cookies_to_header(&stored);
    let has_cf_clearance = stored
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case("cf_clearance"));
    let cookie_names: Vec<String> = stored.iter().map(|c| c.name.clone()).collect();

    tracing::debug!(
        "import_browser_cookies: {} cookie(s) imported (cf_clearance present: {has_cf_clearance})",
        cookie_names.len()
    );

    let effective_user_agent = user_agent
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| browser_user_agent.clone());

    if persist.unwrap_or(true) {
        let manager = engine::get_manager()
            .await
            .ok_or_else(|| "cookie persistence failed: engine manager unavailable".to_string())?;
        let entry = CookieEntry {
            host: host.clone(),
            browser_id: browser.to_string(),
            user_agent: effective_user_agent.clone(),
            cookies: stored.clone(),
            imported_at: 0,
            last_validated_at: 0,
        };
        manager.cookie_store().upsert(entry)?;
    }

    let cookies_view: Vec<ImportedCookieView> = stored
        .iter()
        .map(|c| ImportedCookieView {
            name: c.name.clone(),
            value: c.value.clone(),
            domain: c.domain.clone(),
            path: c.path.clone(),
            secure: c.secure,
            http_only: c.http_only,
            expires: c.expires,
        })
        .collect();

    Ok(ImportedCookies {
        host,
        user_agent: effective_user_agent,
        cookie_header,
        count: stored.len(),
        has_cf_clearance,
        cookie_names,
        cookies: cookies_view,
    })
}

#[tauri::command]
pub async fn list_cookie_entries() -> Result<Vec<CookieEntryView>, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager
        .cookie_store()
        .list()
        .iter()
        .map(Into::into)
        .collect())
}

#[tauri::command]
pub async fn delete_cookie_entry(host: String) -> Result<bool, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.cookie_store().remove(&host)
}

#[tauri::command]
pub async fn clear_cookie_entries() -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.cookie_store().clear()
}

#[tauri::command]
pub async fn retry_with_cookies(
    gid: String,
    payload: RetryWithCookiesPayload,
) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager
        .retry_with_cookies(&gid, payload.cookie, payload.user_agent)
        .await
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedUserAgent {
    pub user_agent: String,
}

#[tauri::command]
pub async fn capture_user_agent() -> Result<CapturedUserAgent, String> {
    tracing::info!("capture_user_agent: starting one-shot listener");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind localhost listener failed: {e}"))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("read local addr failed: {e}"))?
        .port();
    let url = format!("http://127.0.0.1:{port}/risuko-ua-capture");

    if let Err(e) = open::that(&url) {
        tracing::warn!("capture_user_agent: open::that({url}) failed: {e}");
    }

    let captured = timeout(Duration::from_secs(60), async {
        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("capture_user_agent: accept failed: {e}");
                    continue;
                }
            };
            tracing::debug!("capture_user_agent: connection from {peer}");

            let mut buf = vec![0u8; 4096];
            let mut total = 0usize;
            let parsed: Option<(String, String)> = loop {
                match timeout(Duration::from_secs(5), stream.read(&mut buf[total..])).await {
                    Ok(Ok(0)) => break None,
                    Ok(Ok(n)) => {
                        total += n;
                        if let Some(end) = find_header_end(&buf[..total]) {
                            break parse_request(&buf[..end]);
                        }
                        if total == buf.len() {
                            break None;
                        }
                    }
                    _ => break None,
                }
            };

            let body = "<!doctype html><meta charset=utf-8><title>Risuko</title>\
                 <style>body{font:14px/1.5 system-ui;padding:2rem;color:#333}\
                 h1{font-size:18px}</style>\
                 <h1>User-Agent captured \u{2713}</h1>\
                 <p>You can close this tab and return to Risuko.</p>\
                 <script>setTimeout(()=>window.close(),1500)</script>";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;

            if let Some((path, ua)) = parsed {
                if path.starts_with("/risuko-ua-capture") && !ua.is_empty() {
                    return ua;
                }
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for browser to open the capture URL".to_string())?;

    tracing::info!("capture_user_agent: captured ua={captured}");
    Ok(CapturedUserAgent {
        user_agent: captured,
    })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn parse_request(buf: &[u8]) -> Option<(String, String)> {
    let text = std::str::from_utf8(buf).ok()?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let _method = parts.next()?;
    let path = parts.next()?.to_string();

    let mut user_agent = String::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("user-agent") {
                user_agent = value.trim().to_string();
            }
        }
    }
    Some((path, user_agent))
}

#[cfg(target_os = "windows")]
mod elevate {
    use risuko_cookies::HostCookies;
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{TerminateProcess, WaitForSingleObject};
    use windows_sys::Win32::UI::Shell::{
        ShellExecuteExW, SEE_MASK_NOCLOSEPROCESS, SHELLEXECUTEINFOW,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::SW_HIDE;

    const HELPER_TIMEOUT_MS: u32 = 120_000;

    fn to_wide(s: &str) -> Vec<u16> {
        OsStr::new(s)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    pub async fn extract_cookies_elevated(browser: &str, url: &str) -> Result<HostCookies, String> {
        let browser = browser.to_string();
        let url = url.to_string();
        tokio::task::spawn_blocking(move || run_elevated(&browser, &url))
            .await
            .map_err(|e| format!("join error: {e}"))?
    }

    fn run_elevated(browser: &str, url: &str) -> Result<HostCookies, String> {
        let exe = std::env::current_exe().map_err(|e| format!("current_exe failed: {e}"))?;

        let out_path = tempfile::Builder::new()
            .prefix("risuko-cookies-")
            .suffix(".json")
            .tempfile()
            .map_err(|e| format!("create temp file failed: {e}"))?
            .into_temp_path();

        let params = format!(
            "extract-cookies --browser \"{}\" --url \"{}\" --out \"{}\"",
            browser.replace('"', ""),
            url.replace('"', ""),
            out_path.to_string_lossy().replace('"', "")
        );

        let verb = to_wide("runas");
        let file = to_wide(&exe.to_string_lossy());
        let params_w = to_wide(&params);

        let mut info: SHELLEXECUTEINFOW = unsafe { std::mem::zeroed() };
        info.cbSize = std::mem::size_of::<SHELLEXECUTEINFOW>() as u32;
        info.fMask = SEE_MASK_NOCLOSEPROCESS;
        info.lpVerb = verb.as_ptr();
        info.lpFile = file.as_ptr();
        info.lpParameters = params_w.as_ptr();
        info.nShow = SW_HIDE;

        let ok = unsafe { ShellExecuteExW(&mut info) };
        if ok == 0 {
            return Err(
                "administrator approval was declined or the elevated helper failed to start"
                    .to_string(),
            );
        }
        if info.hProcess.is_null() {
            return Err("elevated helper did not return a process handle".to_string());
        }

        let timed_out = unsafe {
            let timed_out = WaitForSingleObject(info.hProcess, HELPER_TIMEOUT_MS) == WAIT_TIMEOUT;
            if timed_out {
                TerminateProcess(info.hProcess, 1);
            }
            CloseHandle(info.hProcess);
            timed_out
        };
        if timed_out {
            return Err("the elevated helper timed out".to_string());
        }

        let bytes =
            std::fs::read(&out_path).map_err(|e| format!("read elevated output failed: {e}"))?;
        if bytes.is_empty() {
            return Err("the elevated helper produced no cookies".to_string());
        }
        serde_json::from_slice::<HostCookies>(&bytes)
            .map_err(|e| format!("parse elevated output failed: {e}"))
    }
}
