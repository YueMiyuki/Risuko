use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::watch;
use tokio::time::{interval_at, sleep, Duration, Instant};

use super::session::Mode;
use super::{Identity, Shared};
use crate::kad::KadService;
use crate::protocol::{build_offer_files, OfferEntry};
use crate::server::{ServerConnection, ServerEvent};
use crate::share::SharedFile;
use crate::types::*;

const OFFER_INTERVAL: Duration = Duration::from_secs(60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const RECONNECT_DELAY: Duration = Duration::from_secs(30);
const MAX_OFFERS_PER_PACKET: usize = 200;
const KAD_PUBLISH_TICK: Duration = Duration::from_secs(30);
const KAD_REPUBLISH: Duration = Duration::from_secs(5 * 60 * 60);
const KAD_RETRY: Duration = Duration::from_secs(30 * 60);

const FILE_COMPLETE_ID: u32 = 0xfbfb_fbfb;
const FILE_COMPLETE_PORT: u16 = 0xfbfb;
const FILE_INCOMPLETE_ID: u32 = 0xfcfc_fcfc;
const FILE_INCOMPLETE_PORT: u16 = 0xfcfc;

pub(super) async fn run_server_announcer(shared: Arc<Shared>, kad_udp_port: Option<u16>) {
    let mut changes = shared.registry.subscribe();
    let mut next = 0usize;
    loop {
        let entry = {
            let servers = shared.servers.read();
            (!servers.is_empty()).then(|| servers[next % servers.len()].clone())
        };
        let Some(entry) = entry.filter(|_| shared.registry.has_offerable()) else {
            tokio::select! {
                _ = shared.shutdown.cancelled() => return,
                _ = changes.changed() => {}
                _ = sleep(OFFER_INTERVAL) => {}
            }
            continue;
        };
        next += 1;
        if let Some(addr) = entry.to_socket_addr() {
            if let Err(e) = announce_to(&shared, addr, kad_udp_port, &mut changes).await {
                tracing::debug!("[ed2k] Upload announce on {} ended: {e}", entry.name);
            }
            shared.set_identity(Identity::default());
        }
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = sleep(RECONNECT_DELAY) => {}
        }
    }
}

async fn announce_to(
    shared: &Arc<Shared>,
    addr: std::net::SocketAddrV4,
    kad_udp_port: Option<u16>,
    changes: &mut watch::Receiver<u64>,
) -> Result<(), String> {
    let mut conn = ServerConnection::new_with_proxy(
        addr,
        shared.user_hash,
        shared.port,
        kad_udp_port,
        shared.proxy.clone(),
    );
    let (mut events, _tx) = tokio::select! {
        _ = shared.shutdown.cancelled() => return Ok(()),
        result = conn.connect() => result?,
    };
    let mut offered: HashMap<[u8; 16], bool> = HashMap::new();
    let mut login: Option<(u32, u32)> = None;
    let mut offer_tick = interval_at(Instant::now() + OFFER_INTERVAL, OFFER_INTERVAL);
    let mut keepalive = interval_at(Instant::now() + KEEPALIVE_INTERVAL, KEEPALIVE_INTERVAL);
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => return Ok(()),
            event = events.recv() => match event {
                Some(ServerEvent::Connected { client_id, flags }) => {
                    tracing::info!(
                        "[ed2k] Sharing via server {addr} with {} ID",
                        if is_high_id(client_id) { "a high" } else { "a low" }
                    );
                    login = Some((client_id, flags));
                    shared.set_identity(Identity { client_id, server: Some(addr) });
                    offer(&conn, shared, &mut offered, client_id, flags).await?;
                }
                Some(ServerEvent::CallbackRequested(target)) => {
                    if login.is_some() && shared.registry.has_offerable() {
                        shared.spawn_dial(target, Mode::Callback);
                    }
                }
                Some(ServerEvent::Disconnected(reason)) => {
                    return Err(format!("disconnected: {}", reason.unwrap_or_default()));
                }
                None => return Err("server connection closed".into()),
                Some(_) => {}
            },
            changed = changes.changed() => {
                if changed.is_err() || !shared.registry.has_offerable() {
                    return Ok(());
                }
                if let Some((client_id, flags)) = login {
                    offer(&conn, shared, &mut offered, client_id, flags).await?;
                }
            }
            _ = offer_tick.tick() => {
                if !shared.registry.has_offerable() {
                    return Ok(());
                }
                if let Some((client_id, flags)) = login {
                    offer(&conn, shared, &mut offered, client_id, flags).await?;
                }
            }
            _ = keepalive.tick() => {
                if login.is_some() {
                    conn.send(build_offer_files(&[])).await?;
                }
            }
        }
    }
}

fn offer_endpoint(file: &SharedFile, client_id: u32, port: u16, flags: u32) -> (u32, u16) {
    if flags & SRV_TCPFLG_COMPRESSION != 0 {
        if file.is_complete() {
            (FILE_COMPLETE_ID, FILE_COMPLETE_PORT)
        } else {
            (FILE_INCOMPLETE_ID, FILE_INCOMPLETE_PORT)
        }
    } else if is_high_id(client_id) {
        (client_id, port)
    } else {
        (0, 0)
    }
}

fn pending_offers(
    files: Vec<Arc<SharedFile>>,
    offered: &HashMap<[u8; 16], bool>,
    flags: u32,
) -> Vec<Arc<SharedFile>> {
    let mut pending: Vec<_> = files
        .into_iter()
        .filter(|file| {
            file.is_offerable()
                && offered.get(&file.hash()) != Some(&file.is_complete())
                && (!file.is_large() || flags & SRV_TCPFLG_LARGEFILES != 0)
        })
        .collect();
    pending.sort_by_key(|file| file.hash());
    pending.truncate(MAX_OFFERS_PER_PACKET);
    pending
}

async fn offer(
    conn: &ServerConnection,
    shared: &Shared,
    offered: &mut HashMap<[u8; 16], bool>,
    client_id: u32,
    flags: u32,
) -> Result<(), String> {
    let files = pending_offers(shared.registry.files(), offered, flags);
    if files.is_empty() {
        return Ok(());
    }
    let entries: Vec<OfferEntry<'_>> = files
        .iter()
        .map(|file| {
            let (client_id, client_port) = offer_endpoint(file, client_id, shared.port, flags);
            OfferEntry {
                file_hash: file.hash(),
                client_id,
                client_port,
                name: file.name(),
                size: file.size(),
            }
        })
        .collect();
    conn.send(build_offer_files(&entries)).await?;
    for file in &files {
        offered.insert(file.hash(), file.is_complete());
    }
    tracing::debug!(
        "[ed2k] Offered {} shared file(s) to the server",
        files.len()
    );
    Ok(())
}

pub(super) async fn run_kad_publisher(shared: Arc<Shared>, kad: Arc<KadService>) {
    let mut due: HashMap<[u8; 16], Instant> = HashMap::new();
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = sleep(KAD_PUBLISH_TICK) => {}
        }
        if !is_high_id(shared.identity().client_id) {
            continue;
        }
        let now = Instant::now();
        let files = shared.registry.files();
        due.retain(|hash, _| files.iter().any(|file| file.hash() == *hash));
        let Some(file) = files
            .into_iter()
            .find(|file| file.is_offerable() && due.get(&file.hash()).is_none_or(|at| *at <= now))
        else {
            continue;
        };
        let cancel = shared.shutdown.child_token();
        let result = kad
            .publish_source(
                file.hash(),
                file.size(),
                shared.port,
                shared.user_hash,
                cancel,
            )
            .await;
        let wait = match result {
            Ok(stored) if stored > 0 => {
                tracing::debug!("[ed2k] Published {} to {stored} Kad node(s)", file.name());
                KAD_REPUBLISH
            }
            Ok(_) => KAD_RETRY,
            Err(e) => {
                tracing::debug!("[ed2k] Kad publish for {} failed: {e}", file.name());
                KAD_RETRY
            }
        };
        due.insert(file.hash(), Instant::now() + wait);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::share::PartMap;
    use std::path::PathBuf;

    fn complete(hash: u8, size: u64) -> Arc<SharedFile> {
        Arc::new(SharedFile::complete(
            [hash; 16],
            "f",
            size,
            PathBuf::new(),
            Vec::new(),
        ))
    }

    #[test]
    fn offers_mark_completeness_on_compression_servers_and_use_high_ids_otherwise() {
        let done = complete(1, 10);
        let parts = PartMap::new(ED2K_CHUNK_SIZE * 2);
        parts.mark_verified(0);
        let partial = SharedFile::partial([2; 16], "p", ED2K_CHUNK_SIZE * 2, PathBuf::new(), parts);
        assert_eq!(
            offer_endpoint(&done, 0x0100_0000, 4662, SRV_TCPFLG_COMPRESSION),
            (FILE_COMPLETE_ID, FILE_COMPLETE_PORT)
        );
        assert_eq!(
            offer_endpoint(&partial, 0x0100_0000, 4662, SRV_TCPFLG_COMPRESSION),
            (FILE_INCOMPLETE_ID, FILE_INCOMPLETE_PORT)
        );
        assert_eq!(
            offer_endpoint(&done, 0x0100_0000, 4662, 0),
            (0x0100_0000, 4662)
        );
        assert_eq!(offer_endpoint(&done, 5, 4662, 0), (0, 0));
    }

    #[test]
    fn pending_offers_skip_published_and_unsupported_large_files() {
        let small = complete(1, 10);
        let large = complete(2, OLD_MAX_FILE_SIZE + 1);
        let mut offered = HashMap::new();
        let files = vec![small.clone(), large.clone()];
        assert_eq!(pending_offers(files.clone(), &offered, 0).len(), 1);
        assert_eq!(
            pending_offers(files.clone(), &offered, SRV_TCPFLG_LARGEFILES).len(),
            2
        );
        offered.insert(small.hash(), true);
        assert_eq!(pending_offers(files.clone(), &offered, 0).len(), 0);
        offered.insert(small.hash(), false);
        assert_eq!(pending_offers(files, &offered, 0).len(), 1);
    }
}
