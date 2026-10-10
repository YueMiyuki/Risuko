use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

use super::protocol::*;
use super::types::*;

const SERVER_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

pub enum ServerEvent {
    Connected {
        client_id: u32,
    },
    ServerMessage(String),
    ServerStatus {
        users: u32,
        files: u32,
    },
    FoundSources {
        file_hash: [u8; 16],
        sources: Vec<(u32, u16)>,
    },
    ServerList,
    Disconnected(Option<String>),
}

pub struct ServerConnection {
    addr: SocketAddrV4,
    client_hash: [u8; 16],
    client_port: u16,
    kad_udp_port: Option<u16>,
    tx: Option<mpsc::Sender<Ed2kPacket>>,
    proxy: risuko_http::ProxyConnector,
}

enum ServerPacketError {
    Parse(String),
    ChannelClosed,
}

impl ServerConnection {
    fn parse_packet_error(opcode: u8, err: String) -> ServerPacketError {
        let msg = format!("opcode 0x{opcode:02x}: {err}");
        tracing::warn!("[ed2k] Server packet parse error: {}", msg);
        ServerPacketError::Parse(msg)
    }

    pub fn new_with_proxy(
        addr: SocketAddrV4,
        client_hash: [u8; 16],
        client_port: u16,
        kad_udp_port: Option<u16>,
        proxy: risuko_http::ProxyConnector,
    ) -> Self {
        Self {
            addr,
            client_hash,
            client_port,
            kad_udp_port,
            tx: None,
            proxy,
        }
    }

    pub async fn connect(
        &mut self,
    ) -> Result<(mpsc::Receiver<ServerEvent>, mpsc::Sender<Ed2kPacket>), String> {
        let stream = timeout(
            SERVER_CONNECT_TIMEOUT,
            self.proxy
                .connect_tcp(&self.addr.ip().to_string(), self.addr.port()),
        )
        .await
        .map_err(|_| format!("Timed out connecting to {}", self.addr))?
        .map_err(|e| format!("Failed to connect to {}: {}", self.addr, e))?;

        let (read_half, mut write_half) = tokio::io::split(stream);

        let hello = build_hello_server(&self.client_hash, self.client_port, self.kad_udp_port);
        write_half
            .write_all(&hello.encode())
            .await
            .map_err(|e| format!("Failed to send hello: {}", e))?;

        let offer = build_offer_files_empty();
        write_half
            .write_all(&offer.encode())
            .await
            .map_err(|e| format!("Failed to send offer: {}", e))?;

        let (event_tx, event_rx) = mpsc::channel(64);
        let (packet_tx, mut packet_rx) = mpsc::channel::<Ed2kPacket>(32);
        self.tx = Some(packet_tx.clone());

        tokio::spawn(async move {
            while let Some(packet) = packet_rx.recv().await {
                if write_half.write_all(&packet.encode()).await.is_err() {
                    break;
                }
            }
        });

        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            let mut reader = read_half;
            let mut buf = bytes::BytesMut::with_capacity(8192);
            'outer: loop {
                match reader.read_buf(&mut buf).await {
                    Ok(0) => {
                        let _ = event_tx_clone.send(ServerEvent::Disconnected(None)).await;
                        break;
                    }
                    Ok(_) => loop {
                        let packet = match Ed2kPacket::decode(&mut buf) {
                            Ok(Some(packet)) => packet,
                            Ok(None) => break,
                            Err(e) => {
                                let _ = event_tx_clone
                                    .send(ServerEvent::Disconnected(Some(e.to_string())))
                                    .await;
                                break 'outer;
                            }
                        };
                        match Self::handle_server_packet(&event_tx_clone, &packet).await {
                            Ok(()) => {}
                            Err(ServerPacketError::Parse(message)) => {
                                tracing::debug!(
                                    "[ed2k] Ignoring malformed server packet: {}",
                                    message
                                );
                            }
                            Err(ServerPacketError::ChannelClosed) => break 'outer,
                        }
                    },
                    Err(e) => {
                        let _ = event_tx_clone
                            .send(ServerEvent::Disconnected(Some(e.to_string())))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok((event_rx, packet_tx))
    }

    async fn handle_server_packet(
        tx: &mpsc::Sender<ServerEvent>,
        packet: &Ed2kPacket,
    ) -> Result<(), ServerPacketError> {
        let event = match packet.opcode {
            OP_ID_CHANGE => {
                let client_id = parse_id_change(&packet.payload)
                    .map_err(|e| Self::parse_packet_error(packet.opcode, e))?;
                ServerEvent::Connected { client_id }
            }
            OP_SERVER_MESSAGE => {
                let msg = parse_server_message(&packet.payload)
                    .map_err(|e| Self::parse_packet_error(packet.opcode, e))?;
                ServerEvent::ServerMessage(msg)
            }
            OP_SERVER_STATUS => {
                let (users, files) = parse_server_status(&packet.payload)
                    .map_err(|e| Self::parse_packet_error(packet.opcode, e))?;
                ServerEvent::ServerStatus { users, files }
            }
            OP_FOUND_SOURCES => {
                let (hash, sources) = parse_found_sources(&packet.payload)
                    .map_err(|e| Self::parse_packet_error(packet.opcode, e))?;
                ServerEvent::FoundSources {
                    file_hash: hash,
                    sources,
                }
            }
            OP_SERVER_LIST => ServerEvent::ServerList,
            _ => return Ok(()),
        };

        tx.send(event)
            .await
            .map_err(|_| ServerPacketError::ChannelClosed)
    }

    pub async fn request_sources(&self, file_hash: &[u8; 16]) -> Result<(), String> {
        let tx = self.tx.as_ref().ok_or("Not connected")?;
        let packet = build_get_sources(file_hash);
        tx.send(packet)
            .await
            .map_err(|_| "Send channel closed".to_string())
    }
}
