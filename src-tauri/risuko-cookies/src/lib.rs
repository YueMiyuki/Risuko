mod browser;
mod platform;
mod utils;

use serde::{Deserialize, Serialize};
use url::Url;

// Marker the Tauri side matches to offer a UAC retry
#[cfg(target_os = "windows")]
pub const ELEVATION_REQUIRED: &str = platform::windows::ELEVATION_REQUIRED;

#[cfg(not(target_os = "windows"))]
pub const ELEVATION_REQUIRED: &str = "risuko:elevation-required";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
    pub expires: Option<u64>,
}

#[cfg(not(target_os = "android"))]
impl From<browser::chromium::Cookie> for Cookie {
    fn from(c: browser::chromium::Cookie) -> Self {
        Self {
            name: c.name,
            value: c.value,
            domain: c.domain,
            path: c.path,
            secure: c.secure,
            http_only: c.http_only,
            expires: c.expires,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserInfo {
    pub id: String,
    pub name: String,
    pub available: bool,
    pub user_agent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostCookies {
    pub host: String,
    pub user_agent: String,
    pub cookies: Vec<Cookie>,
}

pub async fn list_browsers() -> Vec<BrowserInfo> {
    #[cfg(target_os = "android")]
    {
        Vec::new()
    }
    #[cfg(not(target_os = "android"))]
    {
        tokio::task::spawn_blocking(list_browsers_sync)
            .await
            .unwrap_or_default()
    }
}

#[cfg(not(target_os = "android"))]
fn list_browsers_sync() -> Vec<BrowserInfo> {
    let mut browsers = Vec::new();

    macro_rules! probe {
        ($id:literal, $name:literal, $ua:expr, $config:expr) => {{
            let available = browser::chromium::is_available(&$config);
            browsers.push(BrowserInfo {
                id: $id.into(),
                name: $name.into(),
                available,
                user_agent: $ua.into(),
            });
        }};
        (firefox: $id:literal, $name:literal, $ua:expr, $config:expr) => {{
            let available = browser::firefox::is_available(&$config);
            browsers.push(BrowserInfo {
                id: $id.into(),
                name: $name.into(),
                available,
                user_agent: $ua.into(),
            });
        }};
    }

    probe!(
        "chrome",
        "Google Chrome",
        default_ua("chrome"),
        browser::chromium::BrowserConfig::chrome()
    );
    probe!(
        "edge",
        "Microsoft Edge",
        default_ua("edge"),
        browser::chromium::BrowserConfig::edge()
    );
    probe!(
        "brave",
        "Brave",
        default_ua("brave"),
        browser::chromium::BrowserConfig::brave()
    );
    probe!(
        "chromium",
        "Chromium",
        default_ua("chromium"),
        browser::chromium::BrowserConfig::chromium()
    );
    probe!(
        "vivaldi",
        "Vivaldi",
        default_ua("vivaldi"),
        browser::chromium::BrowserConfig::vivaldi()
    );
    probe!(
        "opera",
        "Opera",
        default_ua("opera"),
        browser::chromium::BrowserConfig::opera()
    );

    #[cfg(target_os = "macos")]
    probe!(
        "arc",
        "Arc",
        default_ua("chrome"),
        browser::chromium::BrowserConfig::arc()
    );

    probe!(firefox: "firefox", "Firefox", default_ua("firefox"), browser::firefox::BrowserConfig::firefox());
    probe!(firefox: "librewolf", "LibreWolf", default_ua("firefox"), browser::firefox::BrowserConfig::librewolf());
    probe!(firefox: "zen", "Zen Browser", default_ua("firefox"), browser::firefox::BrowserConfig::zen());

    #[cfg(target_os = "macos")]
    {
        let available = browser::safari::is_available();
        browsers.push(BrowserInfo {
            id: "safari".into(),
            name: "Safari".into(),
            available,
            user_agent: default_ua("safari"),
        });
    }

    browsers
}

pub async fn cookies_for_host(browser: &str, host: &str) -> Result<Vec<Cookie>, String> {
    #[cfg(target_os = "android")]
    {
        let _ = (browser, host);
        Err("not supported on android".into())
    }
    #[cfg(not(target_os = "android"))]
    {
        let browser = browser.to_string();
        let host = host.to_string();
        tokio::task::spawn_blocking(move || cookies_for_host_sync(&browser, &host))
            .await
            .map_err(|e| e.to_string())?
    }
}

#[cfg(not(target_os = "android"))]
fn cookies_for_host_sync(browser: &str, host: &str) -> Result<Vec<Cookie>, String> {
    tracing::debug!(target: "risuko_cookies", "cookies_for_host_sync: browser={} host={}", browser, host);

    let result = match browser {
        "chrome" => browser::chromium::extract_cookies(
            &browser::chromium::BrowserConfig::chrome(),
            Some(host),
        ),
        "edge" => browser::chromium::extract_cookies(
            &browser::chromium::BrowserConfig::edge(),
            Some(host),
        ),
        "brave" => browser::chromium::extract_cookies(
            &browser::chromium::BrowserConfig::brave(),
            Some(host),
        ),
        "chromium" => browser::chromium::extract_cookies(
            &browser::chromium::BrowserConfig::chromium(),
            Some(host),
        ),
        "vivaldi" => browser::chromium::extract_cookies(
            &browser::chromium::BrowserConfig::vivaldi(),
            Some(host),
        ),
        "opera" => browser::chromium::extract_cookies(
            &browser::chromium::BrowserConfig::opera(),
            Some(host),
        ),
        "arc" => {
            #[cfg(target_os = "macos")]
            {
                browser::chromium::extract_cookies(
                    &browser::chromium::BrowserConfig::arc(),
                    Some(host),
                )
            }
            #[cfg(not(target_os = "macos"))]
            {
                Err(eyre::eyre!("arc only available on macos"))
            }
        }
        "firefox" => browser::firefox::extract_cookies(
            &browser::firefox::BrowserConfig::firefox(),
            Some(host),
        ),
        "librewolf" => browser::firefox::extract_cookies(
            &browser::firefox::BrowserConfig::librewolf(),
            Some(host),
        ),
        "zen" => {
            browser::firefox::extract_cookies(&browser::firefox::BrowserConfig::zen(), Some(host))
        }
        "safari" => {
            #[cfg(target_os = "macos")]
            {
                browser::safari::extract_cookies(Some(host))
            }
            #[cfg(not(target_os = "macos"))]
            {
                Err(eyre::eyre!("safari only available on macos"))
            }
        }
        _ => Err(eyre::eyre!("unsupported browser: {}", browser)),
    }
    .map_err(|e| e.to_string());

    match &result {
        Ok(cookies) => {
            tracing::debug!(target: "risuko_cookies", "cookies_for_host_sync: browser={} host={} -> {} cookies", browser, host, cookies.len())
        }
        Err(e) => {
            tracing::debug!(target: "risuko_cookies", "cookies_for_host_sync: browser={} host={} -> error: {}", browser, host, e)
        }
    }

    Ok(result?.into_iter().map(Cookie::from).collect())
}

pub async fn cookies_for_url(browser: &str, target: &str) -> Result<HostCookies, String> {
    let url = Url::parse(target)
        .or_else(|_| Url::parse(&format!("https://{}", target)))
        .map_err(|e| e.to_string())?;
    let host = url.host_str().ok_or("invalid url")?;
    tracing::debug!(target: "risuko_cookies", "cookies_for_url: browser={} target={} -> host={}", browser, target, host);

    let cookies = cookies_for_host(browser, host).await?;
    let user_agent = default_ua(browser);

    Ok(HostCookies {
        host: host.to_string(),
        user_agent,
        cookies,
    })
}

fn default_ua(browser: &str) -> String {
    match browser {
        "firefox" | "librewolf" | "zen" => {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0".into()
        }
        "safari" => {
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15".into()
        }
        _ => {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".into()
        }
    }
}
