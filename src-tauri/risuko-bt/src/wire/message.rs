//! Peer wire messages (BEP-3 & BEP-10 extension container); frame `<len: u32 BE><msg_id: u8?><payload>` where length 0 is a keep-alive (no id, no payload)

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Practical upper bound for a single peer message; BEP-3 chunks and ut_metadata pieces are 16 KiB, we allow generous headroom
pub const MAX_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

pub mod id {
    pub const CHOKE: u8 = 0;
    pub const UNCHOKE: u8 = 1;
    pub const INTERESTED: u8 = 2;
    pub const NOT_INTERESTED: u8 = 3;
    pub const HAVE: u8 = 4;
    pub const BITFIELD: u8 = 5;
    pub const REQUEST: u8 = 6;
    pub const PIECE: u8 = 7;
    pub const CANCEL: u8 = 8;
    pub const PORT: u8 = 9;
    pub const HAVE_ALL: u8 = 0x0E;
    pub const HAVE_NONE: u8 = 0x0F;
    pub const REJECT_REQUEST: u8 = 0x10;
    pub const SUGGEST_PIECE: u8 = 0x0D;
    pub const ALLOWED_FAST: u8 = 0x11;
    pub const EXTENDED: u8 = 20;
    pub const HASH_REQUEST: u8 = 21;
    pub const HASHES: u8 = 22;
    pub const HASH_REJECT: u8 = 23;
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("truncated message: id {id}, expected {expected} bytes, got {got}")]
    Truncated { id: u8, expected: usize, got: usize },
    #[error("unknown message id {0}")]
    UnknownId(u8),
    #[error("message too large: {0} bytes")]
    TooLarge(usize),
}

#[derive(Debug, Clone)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield(Bytes),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        data: Bytes,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    HaveAll,
    HaveNone,
    RejectRequest {
        index: u32,
        begin: u32,
        length: u32,
    },
    /// BEP-6 advisory piece suggestion
    SuggestPiece(u32),
    /// BEP-6 piece that may be requested while choked
    AllowedFast(u32),
    Extended {
        ext_id: u8,
        payload: Bytes,
    },
    /// BEP 52: request `length` hashes at `base_layer` of the file's Merkle tree from `index`, plus `proof_layers` levels of sibling hashes for verification
    HashRequest {
        pieces_root: [u8; 32],
        base_layer: u32,
        index: u32,
        length: u32,
        proof_layers: u32,
    },
    /// BEP 52 response; `hashes` carries `length` requested hashes followed by `proof_layers` sibling hashes (SHA-256, 32 bytes each)
    Hashes {
        pieces_root: [u8; 32],
        base_layer: u32,
        index: u32,
        length: u32,
        proof_layers: u32,
        hashes: Bytes,
    },
    /// BEP 52: peer cannot fulfil the matching `HashRequest`
    HashReject {
        pieces_root: [u8; 32],
        base_layer: u32,
        index: u32,
        length: u32,
        proof_layers: u32,
    },
    /// Fallback for message ids we don't recognise — keep the raw payload so future decode upgrades don't need protocol changes
    Unknown {
        id: u8,
        payload: Bytes,
    },
}

pub struct MessageEncoder;

impl MessageEncoder {
    /// Encode a message to bytes ready to write on the wire (length prefix included)
    pub fn encode(msg: &Message) -> Bytes {
        let mut buf = BytesMut::with_capacity(64);
        // Reserve 4 bytes for the length prefix; fill after we know body size
        buf.put_u32(0);
        match msg {
            Message::KeepAlive => {}
            Message::Choke => buf.put_u8(id::CHOKE),
            Message::Unchoke => buf.put_u8(id::UNCHOKE),
            Message::Interested => buf.put_u8(id::INTERESTED),
            Message::NotInterested => buf.put_u8(id::NOT_INTERESTED),
            Message::Have { piece_index } => {
                buf.put_u8(id::HAVE);
                buf.put_u32(*piece_index);
            }
            Message::Bitfield(b) => {
                buf.put_u8(id::BITFIELD);
                buf.extend_from_slice(b);
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                buf.put_u8(id::REQUEST);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
            }
            Message::Piece { index, begin, data } => {
                buf.put_u8(id::PIECE);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.extend_from_slice(data);
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                buf.put_u8(id::CANCEL);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
            }
            Message::Port(port) => {
                buf.put_u8(id::PORT);
                buf.put_u16(*port);
            }
            Message::HaveAll => buf.put_u8(id::HAVE_ALL),
            Message::HaveNone => buf.put_u8(id::HAVE_NONE),
            Message::RejectRequest {
                index,
                begin,
                length,
            } => {
                buf.put_u8(id::REJECT_REQUEST);
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
            }
            Message::SuggestPiece(index) => {
                buf.put_u8(id::SUGGEST_PIECE);
                buf.put_u32(*index);
            }
            Message::AllowedFast(index) => {
                buf.put_u8(id::ALLOWED_FAST);
                buf.put_u32(*index);
            }
            Message::Extended { ext_id, payload } => {
                buf.put_u8(id::EXTENDED);
                buf.put_u8(*ext_id);
                buf.extend_from_slice(payload);
            }
            Message::HashRequest {
                pieces_root,
                base_layer,
                index,
                length,
                proof_layers,
            } => {
                buf.put_u8(id::HASH_REQUEST);
                buf.extend_from_slice(pieces_root);
                buf.put_u32(*base_layer);
                buf.put_u32(*index);
                buf.put_u32(*length);
                buf.put_u32(*proof_layers);
            }
            Message::Hashes {
                pieces_root,
                base_layer,
                index,
                length,
                proof_layers,
                hashes,
            } => {
                buf.put_u8(id::HASHES);
                buf.extend_from_slice(pieces_root);
                buf.put_u32(*base_layer);
                buf.put_u32(*index);
                buf.put_u32(*length);
                buf.put_u32(*proof_layers);
                buf.extend_from_slice(hashes);
            }
            Message::HashReject {
                pieces_root,
                base_layer,
                index,
                length,
                proof_layers,
            } => {
                buf.put_u8(id::HASH_REJECT);
                buf.extend_from_slice(pieces_root);
                buf.put_u32(*base_layer);
                buf.put_u32(*index);
                buf.put_u32(*length);
                buf.put_u32(*proof_layers);
            }
            Message::Unknown { id, payload } => {
                buf.put_u8(*id);
                buf.extend_from_slice(payload);
            }
        }
        let body_len = (buf.len() - 4) as u32;
        buf[..4].copy_from_slice(&body_len.to_be_bytes());
        buf.freeze()
    }
}

/// Stateful decoder that consumes bytes from a `BytesMut` buffer
#[derive(Default)]
pub struct MessageDecoder;

impl MessageDecoder {
    /// Try to decode a single message from the buffer; returns `Ok(None)` when more bytes are needed
    pub fn try_decode(buf: &mut BytesMut) -> Result<Option<Message>, MessageError> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&buf[..4]);
        let body_len = u32::from_be_bytes(len_bytes) as usize;
        if body_len > MAX_MESSAGE_BYTES {
            return Err(MessageError::TooLarge(body_len));
        }
        if buf.len() < 4 + body_len {
            return Ok(None);
        }
        buf.advance(4);
        if body_len == 0 {
            return Ok(Some(Message::KeepAlive));
        }
        let id = buf.get_u8();
        let remaining = body_len - 1;
        let take = |buf: &mut BytesMut, n: usize, id: u8| -> Result<Bytes, MessageError> {
            if buf.remaining() < n {
                return Err(MessageError::Truncated {
                    id,
                    expected: n,
                    got: buf.remaining(),
                });
            }
            Ok(buf.copy_to_bytes(n))
        };
        let msg = match id {
            id::CHOKE => {
                take(buf, remaining, id)?;
                Message::Choke
            }
            id::UNCHOKE => {
                take(buf, remaining, id)?;
                Message::Unchoke
            }
            id::INTERESTED => {
                take(buf, remaining, id)?;
                Message::Interested
            }
            id::NOT_INTERESTED => {
                take(buf, remaining, id)?;
                Message::NotInterested
            }
            id::HAVE => {
                if remaining != 4 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 4,
                        got: remaining,
                    });
                }
                Message::Have {
                    piece_index: buf.get_u32(),
                }
            }
            id::BITFIELD => {
                let b = take(buf, remaining, id)?;
                Message::Bitfield(b)
            }
            id::REQUEST => {
                if remaining != 12 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 12,
                        got: remaining,
                    });
                }
                let index = buf.get_u32();
                let begin = buf.get_u32();
                let length = buf.get_u32();
                Message::Request {
                    index,
                    begin,
                    length,
                }
            }
            id::PIECE => {
                if remaining < 8 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 8,
                        got: remaining,
                    });
                }
                let index = buf.get_u32();
                let begin = buf.get_u32();
                let data = take(buf, remaining - 8, id)?;
                Message::Piece { index, begin, data }
            }
            id::CANCEL => {
                if remaining != 12 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 12,
                        got: remaining,
                    });
                }
                let index = buf.get_u32();
                let begin = buf.get_u32();
                let length = buf.get_u32();
                Message::Cancel {
                    index,
                    begin,
                    length,
                }
            }
            id::PORT => {
                if remaining != 2 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 2,
                        got: remaining,
                    });
                }
                Message::Port(buf.get_u16())
            }
            id::HAVE_ALL => {
                if remaining != 0 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 0,
                        got: remaining,
                    });
                }
                Message::HaveAll
            }
            id::HAVE_NONE => {
                if remaining != 0 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 0,
                        got: remaining,
                    });
                }
                Message::HaveNone
            }
            id::REJECT_REQUEST => {
                if remaining != 12 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 12,
                        got: remaining,
                    });
                }
                let index = buf.get_u32();
                let begin = buf.get_u32();
                let length = buf.get_u32();
                Message::RejectRequest {
                    index,
                    begin,
                    length,
                }
            }
            id::SUGGEST_PIECE => {
                if remaining != 4 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 4,
                        got: remaining,
                    });
                }
                Message::SuggestPiece(buf.get_u32())
            }
            id::ALLOWED_FAST => {
                if remaining != 4 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 4,
                        got: remaining,
                    });
                }
                Message::AllowedFast(buf.get_u32())
            }
            id::EXTENDED => {
                if remaining < 1 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 1,
                        got: remaining,
                    });
                }
                let ext_id = buf.get_u8();
                let payload = take(buf, remaining - 1, id)?;
                Message::Extended { ext_id, payload }
            }
            id::HASH_REQUEST | id::HASH_REJECT => {
                // 32-byte root + 4*4-byte u32 fields = 48 bytes
                if remaining != 48 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 48,
                        got: remaining,
                    });
                }
                let mut pieces_root = [0u8; 32];
                buf.copy_to_slice(&mut pieces_root);
                let base_layer = buf.get_u32();
                let index = buf.get_u32();
                let length = buf.get_u32();
                let proof_layers = buf.get_u32();
                if id == id::HASH_REQUEST {
                    Message::HashRequest {
                        pieces_root,
                        base_layer,
                        index,
                        length,
                        proof_layers,
                    }
                } else {
                    Message::HashReject {
                        pieces_root,
                        base_layer,
                        index,
                        length,
                        proof_layers,
                    }
                }
            }
            id::HASHES => {
                if remaining < 48 {
                    return Err(MessageError::Truncated {
                        id,
                        expected: 48,
                        got: remaining,
                    });
                }
                let mut pieces_root = [0u8; 32];
                buf.copy_to_slice(&mut pieces_root);
                let base_layer = buf.get_u32();
                let index = buf.get_u32();
                let length = buf.get_u32();
                let proof_layers = buf.get_u32();
                let expected_hash_bytes = (length as usize)
                    .checked_add(proof_layers as usize)
                    .and_then(|n| n.checked_mul(32))
                    .ok_or(MessageError::TooLarge(usize::MAX))?;
                let actual = remaining - 48;
                if actual != expected_hash_bytes {
                    return Err(MessageError::Truncated {
                        id,
                        expected: expected_hash_bytes,
                        got: actual,
                    });
                }
                let hashes = take(buf, actual, id)?;
                Message::Hashes {
                    pieces_root,
                    base_layer,
                    index,
                    length,
                    proof_layers,
                    hashes,
                }
            }
            other => {
                let payload = take(buf, remaining, other)?;
                Message::Unknown { id: other, payload }
            }
        };
        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(msg: Message) -> Message {
        let bytes = MessageEncoder::encode(&msg);
        let mut buf = BytesMut::from(&bytes[..]);
        let decoded = MessageDecoder::try_decode(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty(), "decoder did not consume full frame");
        decoded
    }

    #[test]
    fn keepalive_round_trip() {
        assert!(matches!(round_trip(Message::KeepAlive), Message::KeepAlive));
    }

    #[test]
    fn request_round_trip() {
        let m = Message::Request {
            index: 5,
            begin: 16_384,
            length: 16_384,
        };
        if let Message::Request {
            index,
            begin,
            length,
        } = round_trip(m)
        {
            assert_eq!((index, begin, length), (5, 16_384, 16_384));
        } else {
            panic!();
        }
    }

    #[test]
    fn piece_round_trip() {
        let payload = Bytes::from_static(b"hello world");
        let m = Message::Piece {
            index: 0,
            begin: 0,
            data: payload.clone(),
        };
        if let Message::Piece { data, .. } = round_trip(m) {
            assert_eq!(data, payload);
        } else {
            panic!();
        }
    }

    #[test]
    fn partial_frame_yields_none() {
        let bytes = MessageEncoder::encode(&Message::Have { piece_index: 7 });
        let mut buf = BytesMut::from(&bytes[..bytes.len() - 1]);
        assert!(MessageDecoder::try_decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn unknown_id_preserved() {
        let mut raw = BytesMut::new();
        raw.put_u32(3);
        raw.put_u8(99);
        raw.put_slice(b"hi");
        let decoded = MessageDecoder::try_decode(&mut raw).unwrap().unwrap();
        match decoded {
            Message::Unknown { id, payload } => {
                assert_eq!(id, 99);
                assert_eq!(payload.as_ref(), b"hi");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn rejects_oversized_frame() {
        let mut raw = BytesMut::new();
        raw.put_u32((MAX_MESSAGE_BYTES + 1) as u32);
        let err = MessageDecoder::try_decode(&mut raw).unwrap_err();
        assert!(matches!(err, MessageError::TooLarge(_)));
    }

    #[test]
    fn hash_request_round_trip() {
        let m = Message::HashRequest {
            pieces_root: [0x42u8; 32],
            base_layer: 0,
            index: 16,
            length: 4,
            proof_layers: 3,
        };
        if let Message::HashRequest {
            pieces_root,
            base_layer,
            index,
            length,
            proof_layers,
        } = round_trip(m)
        {
            assert_eq!(pieces_root, [0x42u8; 32]);
            assert_eq!((base_layer, index, length, proof_layers), (0, 16, 4, 3));
        } else {
            panic!();
        }
    }

    #[test]
    fn hashes_round_trip() {
        let mut hashes = Vec::new();
        // length=2 + proof_layers=3 = 5 hashes total
        for i in 0..5 {
            hashes.extend_from_slice(&[i as u8; 32]);
        }
        let m = Message::Hashes {
            pieces_root: [0xaau8; 32],
            base_layer: 1,
            index: 0,
            length: 2,
            proof_layers: 3,
            hashes: Bytes::from(hashes.clone()),
        };
        if let Message::Hashes {
            pieces_root,
            base_layer,
            index,
            length,
            proof_layers,
            hashes: got,
        } = round_trip(m)
        {
            assert_eq!(pieces_root, [0xaau8; 32]);
            assert_eq!((base_layer, index, length, proof_layers), (1, 0, 2, 3));
            assert_eq!(got.as_ref(), &hashes[..]);
        } else {
            panic!();
        }
    }

    #[test]
    fn hashes_rejects_truncated_payload() {
        // Encode a Hashes with length=2/proof_layers=3 (=> 5*32 hash bytes) then strip a hash to force a length mismatch on decode
        let mut hashes = Vec::with_capacity(5 * 32);
        for i in 0..5 {
            hashes.extend_from_slice(&[i as u8; 32]);
        }
        let bytes = MessageEncoder::encode(&Message::Hashes {
            pieces_root: [0u8; 32],
            base_layer: 0,
            index: 0,
            length: 2,
            proof_layers: 3,
            hashes: Bytes::from(hashes),
        });
        // Drop trailing 32 bytes => length mismatch
        let mut chopped = BytesMut::from(&bytes[..bytes.len() - 32]);
        // Rewrite length prefix to match the new (shorter) body
        let new_body_len = (chopped.len() - 4) as u32;
        chopped[..4].copy_from_slice(&new_body_len.to_be_bytes());
        let err = MessageDecoder::try_decode(&mut chopped).unwrap_err();
        assert!(matches!(err, MessageError::Truncated { .. }));
    }

    #[test]
    fn hash_reject_round_trip() {
        let m = Message::HashReject {
            pieces_root: [0x11u8; 32],
            base_layer: 2,
            index: 8,
            length: 1,
            proof_layers: 0,
        };
        if let Message::HashReject {
            pieces_root,
            base_layer,
            index,
            length,
            proof_layers,
        } = round_trip(m)
        {
            assert_eq!(pieces_root, [0x11u8; 32]);
            assert_eq!((base_layer, index, length, proof_layers), (2, 8, 1, 0));
        } else {
            panic!();
        }
    }

    #[test]
    fn fast_extension_round_trips() {
        assert!(matches!(round_trip(Message::HaveAll), Message::HaveAll));
        assert!(matches!(round_trip(Message::HaveNone), Message::HaveNone));
        match round_trip(Message::RejectRequest {
            index: 3,
            begin: 16_384,
            length: 16_384,
        }) {
            Message::RejectRequest {
                index,
                begin,
                length,
            } => assert_eq!((index, begin, length), (3, 16_384, 16_384)),
            other => panic!("expected RejectRequest, got {other:?}"),
        }
    }

    #[test]
    fn fast_have_all_and_none_reject_trailing_payload() {
        for id in [id::HAVE_ALL, id::HAVE_NONE] {
            let mut frame = BytesMut::from(&[0, 0, 0, 2, id, 0][..]);
            let err = MessageDecoder::try_decode(&mut frame).unwrap_err();
            assert!(matches!(
                err,
                MessageError::Truncated {
                    id: actual,
                    expected: 0,
                    got: 1,
                } if actual == id
            ));
        }
    }
}
