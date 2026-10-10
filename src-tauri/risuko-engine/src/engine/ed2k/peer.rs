use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

use super::protocol::*;
use super::types::*;

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub enum PeerEvent {
    HelloAnswer(PeerCaps),
    FileStatus {
        file_hash: [u8; 16],
        parts: Vec<bool>,
    },
    HashsetAnswer {
        file_hash: [u8; 16],
        hashes: Vec<[u8; 16]>,
    },
    SlotGiven,
    SlotTaken,
    QueueRanking(u16),
    NoFile,
    DataReceived {
        file_hash: [u8; 16],
        start: u64,
        data: Vec<u8>,
    },
    Disconnected(Option<String>),
}

pub struct PeerConnection {
    addr: SocketAddrV4,
    client_hash: [u8; 16],
    client_id: u32,
    client_port: u16,
    server_ip: u32,
    server_port: u16,
    tx: Option<mpsc::Sender<Ed2kPacket>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    proxy: risuko_http::ProxyConnector,
}

impl PeerConnection {
    pub fn new_with_proxy(
        addr: SocketAddrV4,
        client_hash: [u8; 16],
        client_id: u32,
        client_port: u16,
        server_ip: u32,
        server_port: u16,
        proxy: risuko_http::ProxyConnector,
    ) -> Self {
        Self {
            addr,
            client_hash,
            client_id,
            client_port,
            server_ip,
            server_port,
            tx: None,
            tasks: Vec::new(),
            proxy,
        }
    }

    pub async fn connect(
        &mut self,
    ) -> Result<(mpsc::Receiver<PeerEvent>, mpsc::Sender<Ed2kPacket>), String> {
        let stream = timeout(
            PEER_CONNECT_TIMEOUT,
            self.proxy
                .connect_tcp(&self.addr.ip().to_string(), self.addr.port()),
        )
        .await
        .map_err(|_| format!("Timed out connecting to peer {}", self.addr))?
        .map_err(|e| format!("Failed to connect to peer {}: {}", self.addr, e))?;

        let (read_half, mut write_half) = tokio::io::split(stream);

        let hello = build_hello_client(
            &self.client_hash,
            self.client_id,
            self.client_port,
            self.server_ip,
            self.server_port,
        );
        write_half
            .write_all(&hello.encode())
            .await
            .map_err(|e| format!("Failed to send hello to peer: {}", e))?;

        let (event_tx, event_rx) = mpsc::channel(64);
        let (packet_tx, mut packet_rx) = mpsc::channel::<Ed2kPacket>(32);
        self.tx = Some(packet_tx.clone());

        let writer_task = tokio::spawn(async move {
            while let Some(packet) = packet_rx.recv().await {
                if write_half.write_all(&packet.encode()).await.is_err() {
                    break;
                }
            }
        });

        let event_tx_clone = event_tx.clone();
        let reader_task = tokio::spawn(async move {
            let mut reader = read_half;
            let mut buf = bytes::BytesMut::with_capacity(65536);
            'outer: loop {
                match reader.read_buf(&mut buf).await {
                    Ok(0) => {
                        let _ = event_tx_clone.send(PeerEvent::Disconnected(None)).await;
                        break;
                    }
                    Ok(_) => loop {
                        match Ed2kPacket::decode(&mut buf) {
                            Ok(Some(packet)) => {
                                if Self::handle_peer_packet(&event_tx_clone, &packet)
                                    .await
                                    .is_err()
                                {
                                    break 'outer;
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                let _ = event_tx_clone
                                    .send(PeerEvent::Disconnected(Some(e.to_string())))
                                    .await;
                                break 'outer;
                            }
                        }
                    },
                    Err(e) => {
                        let _ = event_tx_clone
                            .send(PeerEvent::Disconnected(Some(e.to_string())))
                            .await;
                        break;
                    }
                }
            }
        });

        self.tasks.push(writer_task);
        self.tasks.push(reader_task);

        Ok((event_rx, packet_tx))
    }

    async fn handle_peer_packet(
        tx: &mpsc::Sender<PeerEvent>,
        packet: &Ed2kPacket,
    ) -> Result<(), ()> {
        let Some(event) = Self::parse_peer_packet(packet) else {
            return Ok(());
        };
        tx.send(event).await.map_err(|_| ())
    }

    fn parse_peer_packet(packet: &Ed2kPacket) -> Option<PeerEvent> {
        match (packet.protocol, packet.opcode) {
            (PROTO_EDONKEY, OP_HELLO_ANSWER) => (packet.payload.len() >= 17)
                .then(|| PeerEvent::HelloAnswer(parse_hello_answer_caps(&packet.payload))),
            (PROTO_EDONKEY, OP_FILE_STATUS) => {
                let (file_hash, parts) = parse_file_status(&packet.payload).ok()?;
                Some(PeerEvent::FileStatus { file_hash, parts })
            }
            (PROTO_EDONKEY, OP_HASHSET_ANSWER) => {
                let (file_hash, hashes) = parse_hashset_answer(&packet.payload).ok()?;
                Some(PeerEvent::HashsetAnswer { file_hash, hashes })
            }
            (PROTO_EDONKEY, OP_SLOT_GIVEN) => Some(PeerEvent::SlotGiven),
            (PROTO_EDONKEY, OP_SLOT_TAKEN | OP_CANCEL_TRANSFER) => Some(PeerEvent::SlotTaken),
            (PROTO_EDONKEY, OP_FILE_REQ_ANS_NOFILE) => Some(PeerEvent::NoFile),
            (PROTO_EMULE, OP_EMULE_QUEUE_RANKING) => {
                let rank = match packet.payload.as_slice() {
                    [a, b, ..] => u16::from_le_bytes([*a, *b]),
                    _ => 0,
                };
                Some(PeerEvent::QueueRanking(rank))
            }
            (PROTO_EDONKEY, OP_SENDING_PART) => {
                let (file_hash, start, end) = parse_sending_part_header(&packet.payload).ok()?;
                let data = &packet.payload[24..];
                if end < start || (end - start) as usize != data.len() {
                    return None;
                }
                Some(PeerEvent::DataReceived {
                    file_hash,
                    start: u64::from(start),
                    data: data.to_vec(),
                })
            }
            (PROTO_EMULE, OP_SENDING_PART_I64) => {
                let (file_hash, start, end) =
                    parse_sending_part_header_i64(&packet.payload).ok()?;
                let data = &packet.payload[32..];
                if end < start || end - start != data.len() as u64 {
                    return None;
                }
                Some(PeerEvent::DataReceived {
                    file_hash,
                    start,
                    data: data.to_vec(),
                })
            }
            _ => None,
        }
    }

    pub async fn request_file(&self, file_hash: &[u8; 16]) -> Result<(), String> {
        let tx = self.tx.as_ref().ok_or("Not connected")?;
        tx.send(build_file_request(file_hash))
            .await
            .map_err(|_| "Send failed".to_string())
    }

    pub async fn request_file_status(&self, file_hash: &[u8; 16]) -> Result<(), String> {
        let tx = self.tx.as_ref().ok_or("Not connected")?;
        tx.send(build_file_status_request(file_hash))
            .await
            .map_err(|_| "Send failed".to_string())
    }

    pub async fn request_hashset(&self, file_hash: &[u8; 16]) -> Result<(), String> {
        let tx = self.tx.as_ref().ok_or("Not connected")?;
        tx.send(build_hashset_request(file_hash))
            .await
            .map_err(|_| "Send failed".to_string())
    }

    pub async fn request_slot(&self, file_hash: &[u8; 16]) -> Result<(), String> {
        let tx = self.tx.as_ref().ok_or("Not connected")?;
        tx.send(build_slot_request(file_hash))
            .await
            .map_err(|_| "Send failed".to_string())
    }

    pub async fn request_parts(
        &self,
        file_hash: &[u8; 16],
        ranges: &[(u64, u64)],
        large: bool,
    ) -> Result<(), String> {
        let tx = self.tx.as_ref().ok_or("Not connected")?;
        tx.send(build_request_parts(file_hash, ranges, large)?)
            .await
            .map_err(|_| "Send failed".to_string())
    }
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sending_part(start: u32, end: u32, len: usize) -> Ed2kPacket {
        let mut payload = vec![7u8; 16];
        payload.extend_from_slice(&start.to_le_bytes());
        payload.extend_from_slice(&end.to_le_bytes());
        payload.extend(std::iter::repeat_n(0u8, len));
        Ed2kPacket::new(PROTO_EDONKEY, OP_SENDING_PART, payload)
    }

    #[test]
    fn sending_part_requires_consistent_range() {
        let ok = PeerConnection::parse_peer_packet(&sending_part(10, 14, 4));
        assert!(matches!(
            ok,
            Some(PeerEvent::DataReceived { start: 10, .. })
        ));
        assert!(PeerConnection::parse_peer_packet(&sending_part(10, 14, 5)).is_none());
        assert!(PeerConnection::parse_peer_packet(&sending_part(14, 10, 0)).is_none());
    }

    fn sending_part_i64(start: u64, end: u64, len: usize) -> Ed2kPacket {
        let mut payload = vec![7u8; 16];
        payload.extend_from_slice(&start.to_le_bytes());
        payload.extend_from_slice(&end.to_le_bytes());
        payload.extend(std::iter::repeat_n(0u8, len));
        Ed2kPacket::new(PROTO_EMULE, OP_SENDING_PART_I64, payload)
    }

    #[test]
    fn sending_part_i64_carries_offsets_past_4_gib() {
        let start = 5_000_000_000u64;
        let ok = PeerConnection::parse_peer_packet(&sending_part_i64(start, start + 4, 4));
        assert!(matches!(
            ok,
            Some(PeerEvent::DataReceived { start: s, ref data, .. }) if s == start && data.len() == 4
        ));
        assert!(PeerConnection::parse_peer_packet(&sending_part_i64(10, 14, 5)).is_none());
        assert!(PeerConnection::parse_peer_packet(&sending_part_i64(14, 10, 0)).is_none());
        let mut wrong = sending_part_i64(0, 0, 0);
        wrong.protocol = PROTO_EDONKEY;
        assert!(PeerConnection::parse_peer_packet(&wrong).is_none());
    }

    #[test]
    fn dispatch_considers_the_protocol_byte() {
        let wrong = Ed2kPacket::new(PROTO_EMULE, OP_SLOT_GIVEN, vec![]);
        assert!(PeerConnection::parse_peer_packet(&wrong).is_none());
        let nofile = Ed2kPacket::new(PROTO_EDONKEY, OP_FILE_REQ_ANS_NOFILE, vec![0; 16]);
        assert!(matches!(
            PeerConnection::parse_peer_packet(&nofile),
            Some(PeerEvent::NoFile)
        ));
        let cancel = Ed2kPacket::new(PROTO_EDONKEY, OP_CANCEL_TRANSFER, vec![]);
        assert!(matches!(
            PeerConnection::parse_peer_packet(&cancel),
            Some(PeerEvent::SlotTaken)
        ));
    }
}
