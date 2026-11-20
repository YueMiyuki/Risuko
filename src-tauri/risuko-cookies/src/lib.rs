mod browser;
mod platform;
mod utils;

use serde::{Deserialize, Serialize};
use url::Url;

pub const APP_BOUND_UNSUPPORTED: &str = "risuko:app-bound-cookies";

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
enum CookieSource {
    Chromium(fn() -> browser::chromium::BrowserConfig),
    Firefox(fn() -> browser::firefox::BrowserConfig),
    #[cfg(target_os = "macos")]
    Safari,
}

#[cfg(not(target_os = "android"))]
struct KnownBrowser {
    id: &'static str,
    name: &'static str,
    source: CookieSource,
}

#[cfg(not(target_os = "android"))]
const BROWSERS: &[KnownBrowser] = &[
    KnownBrowser {
        id: "chrome",
        name: "Google Chrome",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::chrome),
    },
    KnownBrowser {
        id: "edge",
        name: "Microsoft Edge",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::edge),
    },
    KnownBrowser {
        id: "brave",
        name: "Brave",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::brave),
    },
    KnownBrowser {
        id: "chromium",
        name: "Chromium",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::chromium),
    },
    KnownBrowser {
        id: "vivaldi",
        name: "Vivaldi",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::vivaldi),
    },
    KnownBrowser {
        id: "opera",
        name: "Opera",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::opera),
    },
    #[cfg(target_os = "macos")]
    KnownBrowser {
        id: "arc",
        name: "Arc",
        source: CookieSource::Chromium(browser::chromium::BrowserConfig::arc),
    },
    KnownBrowser {
        id: "firefox",
        name: "Firefox",
        source: CookieSource::Firefox(browser::firefox::BrowserConfig::firefox),
    },
    KnownBrowser {
        id: "librewolf",
        name: "LibreWolf",
        source: CookieSource::Firefox(browser::firefox::BrowserConfig::librewolf),
    },
    KnownBrowser {
        id: "zen",
        name: "Zen Browser",
        source: CookieSource::Firefox(browser::firefox::BrowserConfig::zen),
    },
    #[cfg(target_os = "macos")]
    KnownBrowser {
        id: "safari",
        name: "Safari",
        source: CookieSource::Safari,
    },
];

#[cfg(not(target_os = "android"))]
impl CookieSource {
    fn is_available(&self) -> bool {
        match self {
            Self::Chromium(config) => browser::chromium::is_available(&config()),
            Self::Firefox(config) => browser::firefox::is_available(&config()),
            #[cfg(target_os = "macos")]
            Self::Safari => browser::safari::is_available(),
        }
    }

    fn extract(&self, host: &str) -> eyre::Result<Vec<browser::chromium::Cookie>> {
        match self {
            Self::Chromium(config) => browser::chromium::extract_cookies(&config(), Some(host)),
            Self::Firefox(config) => browser::firefox::extract_cookies(&config(), Some(host)),
            #[cfg(target_os = "macos")]
            Self::Safari => browser::safari::extract_cookies(Some(host)),
        }
    }
}

#[cfg(not(target_os = "android"))]
fn list_browsers_sync() -> Vec<BrowserInfo> {
    BROWSERS
        .iter()
        .map(|known| BrowserInfo {
            id: known.id.into(),
            name: known.name.into(),
            available: known.source.is_available(),
            user_agent: default_ua(known.id),
        })
        .collect()
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

    let result = match BROWSERS.iter().find(|known| known.id == browser) {
        Some(known) => known.source.extract(host),
        None if matches!(browser, "arc" | "safari") => {
            Err(eyre::eyre!("{browser} only available on macos"))
        }
        None => Err(eyre::eyre!("unsupported browser: {}", browser)),
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
