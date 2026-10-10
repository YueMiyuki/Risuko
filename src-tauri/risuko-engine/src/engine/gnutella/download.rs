use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::peer::fetch_by_urn_with_proxy;
use super::types::{is_gnutella_uri, parse_gnutella_uri, GnutellaError};
use crate::engine::options::EngineOptions;

pub async fn run_gnutella_download(
    uri: &str,
    dir: &str,
    opts: &EngineOptions,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
) -> Result<PathBuf, String> {
    if !is_gnutella_uri(uri) {
        return Err(format!("not a Gnutella URI: {uri}"));
    }
    let proxy = opts.p2p_proxy_connector()?;
    let link = parse_gnutella_uri(uri).ok_or_else(|| "invalid gnutella URI".to_string())?;
    let urn = link
        .urn
        .as_deref()
        .ok_or_else(|| "Gnutella URI missing urn:sha1: parameter".to_string())?;
    if link.file_size == 0 {
        return Err("Gnutella URI missing xl/size parameter".into());
    }
    total.store(link.file_size, Ordering::Relaxed);

    if cancel_token.is_cancelled() {
        return Err("cancelled".into());
    }

    run_urn_fetch(
        UrnFetch {
            host: &link.host,
            port: link.port,
            n2r_path: if link.n2r_path.is_empty() {
                "/uri-res/N2R"
            } else {
                &link.n2r_path
            },
            urn,
            file_size: link.file_size,
            file_name: &link.file_name,
            default_name: "gnutella-download",
            dir,
        },
        completed,
        speed,
        connections,
        cancel_token,
        proxy,
    )
    .await
    .map_err(|e: GnutellaError| e.to_string())
}

fn urn_out_path(dir: &str, file_name: &str, urn: &str, default_name: &str) -> PathBuf {
    let name = if file_name.is_empty() {
        urn.trim_start_matches("urn:sha1:")
    } else {
        file_name
    };
    PathBuf::from(dir).join(crate::engine::util::safe_filename(name, default_name))
}

pub(crate) fn partial_path_for_uri(uri: &str, dir: &str) -> Option<PathBuf> {
    let (file_name, urn, default_name) = if is_gnutella_uri(uri) {
        let link = parse_gnutella_uri(uri)?;
        (link.file_name, link.urn?, "gnutella-download")
    } else {
        let link = crate::engine::g2::parse_g2_uri(uri)?;
        (link.file_name, link.urn?, "g2-download")
    };
    Some(super::peer::part_path_for(&urn_out_path(
        dir,
        &file_name,
        &urn,
        default_name,
    )))
}

pub(crate) struct UrnFetch<'a> {
    pub host: &'a str,
    pub port: u16,
    pub n2r_path: &'a str,
    pub urn: &'a str,
    pub file_size: u64,
    pub file_name: &'a str,
    pub default_name: &'a str,
    pub dir: &'a str,
}

pub(crate) async fn run_urn_fetch(
    req: UrnFetch<'_>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    proxy: risuko_http::ProxyConnector,
) -> Result<PathBuf, GnutellaError> {
    let out_path = urn_out_path(req.dir, req.file_name, req.urn, req.default_name);
    let fetch_path = out_path.clone();
    connections.store(1, Ordering::Relaxed);
    let fetch = fetch_by_urn_with_proxy(
        req.host,
        req.port,
        req.n2r_path,
        req.urn,
        req.file_size,
        &fetch_path,
        completed.clone(),
        cancel_token,
        proxy,
    );
    tokio::pin!(fetch);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prev = completed.load(Ordering::Relaxed);
    let result = loop {
        tokio::select! {
            res = &mut fetch => break res,
            _ = interval.tick() => {
                let current = completed.load(Ordering::Relaxed);
                speed.store(current.saturating_sub(prev), Ordering::Relaxed);
                prev = current;
            }
        }
    };
    connections.store(0, Ordering::Relaxed);
    speed.store(0, Ordering::Relaxed);
    result?;
    Ok(out_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_path_matches_the_fetch_target() {
        let gnutella = "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=a%2Fb.bin&xl=10";
        assert_eq!(
            partial_path_for_uri(gnutella, "/d"),
            Some(PathBuf::from("/d/a_b.bin.part"))
        );
        let g2 = "g2://h:6346/sha1/ABCDEF?xl=10&dn=foo.bin";
        assert_eq!(
            partial_path_for_uri(g2, "/d"),
            Some(PathBuf::from("/d/foo.bin.part"))
        );
        assert_eq!(partial_path_for_uri("g2://h:6346/", "/d"), None);
    }
}
