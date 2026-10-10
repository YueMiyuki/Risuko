use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ed2kFileLink {
    pub file_name: String,
    pub file_size: u64,
    pub file_hash: String,
    #[serde(skip)]
    pub file_hash_bytes: [u8; 16],
    pub sources: Vec<Ed2kSource>,
    pub aich_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ed2kSource {
    pub ip: String,
    pub port: u16,
}

pub const ED2K_CHUNK_SIZE: u64 = 9_728_000;

pub const PROTO_EDONKEY: u8 = 0xe3;
pub const PROTO_EMULE: u8 = 0xc5;
pub const PROTO_PACKED: u8 = 0xd4;

pub const OP_HELLO_SERVER: u8 = 0x01;
pub const OP_OFFER_FILES: u8 = 0x15;
pub const OP_GET_SOURCES: u8 = 0x19;

pub const OP_ID_CHANGE: u8 = 0x40;
pub const OP_SERVER_MESSAGE: u8 = 0x38;
pub const OP_SERVER_STATUS: u8 = 0x34;
pub const OP_FOUND_SOURCES: u8 = 0x42;
pub const OP_SERVER_LIST: u8 = 0x32;

pub const OP_HELLO_CLIENT: u8 = 0x01;
pub const OP_HELLO_ANSWER: u8 = 0x4c;
pub const OP_FILE_REQUEST: u8 = 0x58;
pub const OP_FILE_STATUS_REQUEST: u8 = 0x4f;
pub const OP_FILE_STATUS: u8 = 0x50;
pub const OP_HASHSET_REQUEST: u8 = 0x51;
pub const OP_HASHSET_ANSWER: u8 = 0x52;
pub const OP_SLOT_REQUEST: u8 = 0x54;
pub const OP_SLOT_GIVEN: u8 = 0x55;
pub const OP_SLOT_TAKEN: u8 = 0x57;
pub const OP_FILE_REQ_ANS_NOFILE: u8 = 0x48;
pub const OP_CANCEL_TRANSFER: u8 = 0x56;
pub const OP_REQUEST_PARTS: u8 = 0x47;
pub const OP_SENDING_PART: u8 = 0x46;
pub const OP_REQUEST_PARTS_I64: u8 = 0xa3;
pub const OP_SENDING_PART_I64: u8 = 0xa2;

pub const OP_EMULE_QUEUE_RANKING: u8 = 0x60;

pub const TAG_NAME: u8 = 0x01;
pub const TAG_PORT: u8 = 0x0f;
pub const TAG_VERSION: u8 = 0x11;

pub const CT_EMULE_MISCOPTIONS1: u8 = 0xfa;
pub const CT_EMULE_VERSION: u8 = 0xfb;
pub const CT_EMULE_MISCOPTIONS2: u8 = 0xfe;

pub const MISC2_LARGE_FILES: u32 = 1 << 4;

pub const TAG_EMULE_UDP_PORT: u8 = 0x21;

pub const LOW_ID_THRESHOLD: u32 = 16_777_216;

pub fn is_high_id(client_id: u32) -> bool {
    client_id >= LOW_ID_THRESHOLD
}

pub fn client_id_to_ip(client_id: u32) -> std::net::Ipv4Addr {
    let bytes = client_id.to_le_bytes();
    std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStatus {
    Missing,
    Verifying,
    Downloaded,
}

pub fn chunk_count(file_size: u64) -> u64 {
    if file_size == 0 {
        return 0;
    }
    file_size.div_ceil(ED2K_CHUNK_SIZE)
}
