//! BEP 52 Merkle-tree primitives BitTorrent v2 verifies file content via a binary SHA-256 Merkle tree: - leaves are 16 KiB blocks (the trailing block of a file is zero-padded to 16 KiB for hashing purposes only) - the leaf count is rounded up to the next power of two by appending zero-hash leaves (`[0u8; 32]`) - internal nodes hash the concatenation of their two children - the root is the file's `pieces root` Within a torrent's metadata, the `piece layers` dict gives the intermediate layer at the **piece** level: one SHA-256 hash per piece-aligned chunk of the file. Given those, plus the file's root and length, we can reconstruct enough of the tree to verify any block via an O(log n) sibling proof

use super::hash::{sha256, Id32};

/// 16 KiB — the BEP 52 Merkle-tree block size (leaf granularity)
pub const BLOCK_SIZE: u32 = 16 * 1024;

/// Hash a 16 KiB-padded block as a Merkle leaf
pub fn hash_block(block: &[u8]) -> Id32 {
    if block.len() == BLOCK_SIZE as usize {
        return sha256(block);
    }
    // Trailing partial block: zero-pad to BLOCK_SIZE for hashing
    let mut padded = vec![0u8; BLOCK_SIZE as usize];
    padded[..block.len()].copy_from_slice(block);
    sha256(&padded)
}

/// Hash a pair of internal nodes (concatenation, SHA-256)
pub fn hash_pair(left: &Id32, right: &Id32) -> Id32 {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(&left.0);
    buf[32..].copy_from_slice(&right.0);
    sha256(&buf)
}

/// Compute the Merkle root over a slice of leaf hashes. Pads to the next power of two with zero hashes per BEP 52
pub fn compute_root(leaves: &[Id32]) -> Id32 {
    if leaves.is_empty() {
        return Id32([0u8; 32]);
    }
    if leaves.len() == 1 {
        // Single-leaf trees are already at root
        return leaves[0];
    }
    let mut layer: Vec<Id32> = leaves.to_vec();
    // Pad up to next power of two
    let target = layer.len().next_power_of_two();
    let zero = Id32([0u8; 32]);
    layer.resize(target, zero);
    while layer.len() > 1 {
        layer = layer
            .chunks_exact(2)
            .map(|pair| hash_pair(&pair[0], &pair[1]))
            .collect();
    }
    layer[0]
}

/// Compute the Merkle root over `leaves` padded out to `padded_len` (which must be a power of two ≥ leaves.len()). Used to derive a piece's root from its constituent block hashes when the piece is the last piece of a file: BEP 52 pads the leaf layer at the file level, not at the piece level, so the per-piece subtree may need its own padding logic
pub fn compute_root_padded(leaves: &[Id32], padded_len: usize) -> Id32 {
    assert!(padded_len.is_power_of_two() || padded_len == 0);
    assert!(leaves.len() <= padded_len);
    if padded_len == 0 {
        return Id32([0u8; 32]);
    }
    let mut layer: Vec<Id32> = leaves.to_vec();
    layer.resize(padded_len, Id32([0u8; 32]));
    while layer.len() > 1 {
        layer = layer
            .chunks_exact(2)
            .map(|pair| hash_pair(&pair[0], &pair[1]))
            .collect();
    }
    layer[0]
}

/// Per-file proof table built from the metainfo `piece layers` entry. Holds the SHA-256 hashes at the piece-leaf layer of the file's Merkle tree, one entry per piece. Once a piece's blocks have been downloaded we hash them to derive that piece's root and check it against `piece_root_hashes[piece_index_within_file]`
#[derive(Debug, Clone)]
pub struct MerkleProofTable {
    /// File's overall Merkle root (== `pieces root` from metainfo)
    pub file_root: Id32,
    /// Total number of bytes in the file
    pub file_length: u64,
    /// Bytes per piece (BEP 52: power of two ≥ 16 KiB)
    pub piece_length: u32,
    /// Number of pieces actually present in the file
    pub piece_count: u32,
    /// Number of 16 KiB blocks per piece (= piece_length / 16 KiB)
    pub blocks_per_piece: u32,
    /// SHA-256 root of each piece, in order. Length == `piece_count` For files smaller than one piece, this is empty and `file_root` alone is used
    pub piece_root_hashes: Vec<Id32>,
}

#[derive(Debug, thiserror::Error)]
pub enum MerkleError {
    #[error("merkle: layer length {got} does not match expected {expected}")]
    LayerLengthMismatch { got: usize, expected: usize },
    #[error("merkle: layer length not multiple of 32 bytes")]
    LayerNotAligned,
    #[error("merkle: piece index {0} out of range")]
    PieceOutOfRange(u32),
    #[error("merkle: declared file root does not match recomputed root")]
    RootMismatch,
}

impl MerkleProofTable {
    /// Build a proof table for one file. `layer_bytes` is the value from the metainfo `piece layers` dict for this file's pieces-root, or empty for files smaller than one piece. Verifies that the layer hashes (padded to a power of two) collapse to the declared root
    pub fn from_layer_bytes(
        file_root: Id32,
        file_length: u64,
        piece_length: u32,
        layer_bytes: &[u8],
    ) -> Result<Self, MerkleError> {
        if !piece_length.is_power_of_two() || piece_length < BLOCK_SIZE {
            return Err(MerkleError::LayerLengthMismatch {
                got: piece_length as usize,
                expected: BLOCK_SIZE as usize,
            });
        }
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let piece_count = file_length.div_ceil(piece_length as u64) as u32;

        // For files smaller than one piece, no layer is required — verification is direct against `file_root`
        if file_length <= piece_length as u64 {
            if !layer_bytes.is_empty() {
                return Err(MerkleError::LayerLengthMismatch {
                    got: layer_bytes.len(),
                    expected: 0,
                });
            }
            return Ok(Self {
                file_root,
                file_length,
                piece_length,
                piece_count,
                blocks_per_piece,
                piece_root_hashes: Vec::new(),
            });
        }

        if !layer_bytes.len().is_multiple_of(32) {
            return Err(MerkleError::LayerNotAligned);
        }
        let layer_len = layer_bytes.len() / 32;
        if layer_len != piece_count as usize {
            return Err(MerkleError::LayerLengthMismatch {
                got: layer_len,
                expected: piece_count as usize,
            });
        }
        let piece_root_hashes: Vec<Id32> = layer_bytes
            .chunks_exact(32)
            .map(|c| Id32::from_slice(c).expect("32-byte chunk"))
            .collect();

        // Verify root: the file-level Merkle tree root is computed over the piece-layer leaves padded to the next power of two
        let recomputed = compute_root(&piece_root_hashes);
        if recomputed != file_root {
            return Err(MerkleError::RootMismatch);
        }

        Ok(Self {
            file_root,
            file_length,
            piece_length,
            piece_count,
            blocks_per_piece,
            piece_root_hashes,
        })
    }

    /// Number of bytes in the given piece (the last piece may be partial)
    pub fn piece_size(&self, piece_index: u32) -> Option<u32> {
        if piece_index >= self.piece_count {
            return None;
        }
        let start = piece_index as u64 * self.piece_length as u64;
        let end = (start + self.piece_length as u64).min(self.file_length);
        Some((end - start) as u32)
    }

    /// Compute the root of one piece's subtree by hashing its bytes in 16 KiB blocks and collapsing the (zero-padded) per-piece subtree
    pub fn expected_piece_root(&self, piece_bytes: &[u8]) -> Id32 {
        let mut block_hashes: Vec<Id32> = piece_bytes
            .chunks(BLOCK_SIZE as usize)
            .map(hash_block)
            .collect();
        let target = self.blocks_per_piece as usize;
        if block_hashes.len() < target {
            block_hashes.resize(target, Id32([0u8; 32]));
        }
        if target == 1 {
            return block_hashes[0];
        }
        compute_root_padded(&block_hashes, target.next_power_of_two())
    }

    /// Number of pieces padded to the next power of two — the row length used when requesting / serving the entire piece layer at once (BEP 52 `HASH_REQUEST` `length` field). Returns 0 for files that fit in a single piece (no layer is exchanged for those)
    pub fn piece_layer_padded_len(&self) -> u32 {
        if self.piece_root_hashes.is_empty() {
            0
        } else {
            (self.piece_count as usize).next_power_of_two().max(2) as u32
        }
    }

    /// Number of layers between the 16 KiB-leaf layer (base_layer 0) and the piece layer. Equals `log2(piece_length / 16 KiB)`. This is the `base_layer` field for a piece-layer `HASH_REQUEST`
    pub fn piece_layer_base(&self) -> u32 {
        self.blocks_per_piece.trailing_zeros()
    }

    /// Encode the piece layer for an outbound `HASHES` response: leaf hashes padded with zero-hashes to `piece_layer_padded_len`, flat 32-byte concatenation. Returns `None` for single-piece files (no layer to serve)
    pub fn serve_full_piece_layer(&self) -> Option<Vec<u8>> {
        let padded = self.piece_layer_padded_len() as usize;
        if padded == 0 {
            return None;
        }
        let mut out = Vec::with_capacity(padded * 32);
        for h in &self.piece_root_hashes {
            out.extend_from_slice(&h.0);
        }
        out.resize(padded * 32, 0);
        Some(out)
    }

    /// Validate a `HASHES` response carrying the entire piece layer (`length == next_pow2(piece_count)`, `proof_layers == 0`) for one file, returning the canonical (un-padded) piece-layer bytes ready for storage in `piece layers`. The response is rejected if the recovered hashes do not collapse to `file_root`
    pub fn verify_full_piece_layer_response(
        file_root: Id32,
        file_length: u64,
        piece_length: u32,
        response_hashes: &[u8],
    ) -> Result<Vec<u8>, MerkleError> {
        if !piece_length.is_power_of_two() || piece_length < BLOCK_SIZE {
            return Err(MerkleError::LayerLengthMismatch {
                got: piece_length as usize,
                expected: BLOCK_SIZE as usize,
            });
        }
        let piece_count = file_length.div_ceil(piece_length as u64) as u32;
        if piece_count <= 1 || file_length <= piece_length as u64 {
            return Err(MerkleError::LayerLengthMismatch {
                got: response_hashes.len(),
                expected: 0,
            });
        }
        if !response_hashes.len().is_multiple_of(32) {
            return Err(MerkleError::LayerNotAligned);
        }
        let padded = (piece_count as usize).next_power_of_two().max(2);
        if response_hashes.len() != padded * 32 {
            return Err(MerkleError::LayerLengthMismatch {
                got: response_hashes.len() / 32,
                expected: padded,
            });
        }
        let leaves: Vec<Id32> = response_hashes
            .chunks_exact(32)
            .map(|c| Id32::from_slice(c).expect("32-byte chunk"))
            .collect();
        // Trailing entries beyond `piece_count` must be zero — this is the only valid padding per BEP 52
        for h in &leaves[piece_count as usize..] {
            if h.0 != [0u8; 32] {
                return Err(MerkleError::RootMismatch);
            }
        }
        // Root must collapse from the (already-padded) leaves
        let recomputed = compute_root_padded(&leaves, padded);
        if recomputed != file_root {
            return Err(MerkleError::RootMismatch);
        }
        // Strip padding to produce canonical `piece layers` bytes
        let mut out = Vec::with_capacity(piece_count as usize * 32);
        for h in &leaves[..piece_count as usize] {
            out.extend_from_slice(&h.0);
        }
        Ok(out)
    }

    /// Verify a fully-downloaded piece against the expected layer hash
    pub fn verify_piece(&self, piece_index: u32, piece_bytes: &[u8]) -> Result<(), MerkleError> {
        if piece_index >= self.piece_count {
            return Err(MerkleError::PieceOutOfRange(piece_index));
        }
        let expected_size = self.piece_size(piece_index).unwrap();
        if piece_bytes.len() != expected_size as usize {
            return Err(MerkleError::LayerLengthMismatch {
                got: piece_bytes.len(),
                expected: expected_size as usize,
            });
        }
        let computed = if self.piece_root_hashes.is_empty() {
            // Single-piece file: BEP 52 derives the file root by padding to next_power_of_two(actual_block_count), not to blocks_per_piece. Using expected_piece_root (which pads to blocks_per_piece) would produce the wrong root when the two values differ (e.g. 48 KiB file with 256 KiB pieces: actual_blocks=3 → pad to 4, but blocks_per_piece=16)
            let block_hashes: Vec<Id32> = piece_bytes
                .chunks(BLOCK_SIZE as usize)
                .map(hash_block)
                .collect();
            compute_root(&block_hashes)
        } else {
            self.expected_piece_root(piece_bytes)
        };
        let expected = if self.piece_root_hashes.is_empty() {
            self.file_root
        } else {
            self.piece_root_hashes[piece_index as usize]
        };
        if computed != expected {
            return Err(MerkleError::RootMismatch);
        }
        Ok(())
    }
}

/// Per-torrent piece verifier strategy
use std::sync::Arc;

use super::metainfo::TorrentMeta;

pub fn supports_v2_wire(meta: &TorrentMeta) -> bool {
    let Some(v2) = meta.info_v2.as_ref() else {
        return false;
    };
    v2.files.iter().all(|file| {
        let layer = meta
            .piece_layers
            .get(&file.pieces_root)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        MerkleProofTable::from_layer_bytes(file.pieces_root, file.length, v2.piece_length, layer)
            .is_ok()
    })
}

#[derive(Debug, Clone)]
pub enum PieceVerifier {
    /// BEP-3 SHA-1 piece-hash list. Empty when the underlying metainfo is pure-v2 (in which case `V2Merkle` is used instead)
    V1Sha1 { pieces: Arc<Vec<u8>> },
    /// BEP 52 Merkle verification. `tables` is one entry per file in `info.files` order; `piece_to_file` resolves a torrent-global piece index to (file index, piece index within that file)
    V2Merkle {
        tables: Arc<Vec<MerkleProofTable>>,
        /// Byte offset of each file's first byte in the piece-aligned torrent space (BEP 52 starts every non-empty file on a piece boundary), length `tables.len() + 1`; the last entry is the end of the final file
        file_offsets: Arc<Vec<u64>>,
        piece_length: u32,
    },
}

impl PieceVerifier {
    /// Build a verifier from parsed metainfo. Returns an error only when the v2 layer hashes are inconsistent with the declared file roots — which would indicate a malformed `.torrent`
    pub fn from_meta(meta: &TorrentMeta) -> Result<Self, MerkleError> {
        // Hybrid torrents prefer v1 verification (cheaper, equally strict)
        if meta.meta_version.has_v1() {
            return Ok(Self::V1Sha1 {
                pieces: Arc::new(meta.info.pieces.clone()),
            });
        }
        let v2 = meta
            .info_v2
            .as_ref()
            .expect("pure-v2 metainfo without info_v2");
        let mut tables = Vec::with_capacity(v2.files.len());
        let mut offsets = Vec::with_capacity(v2.files.len() + 1);
        let mut acc: u64 = 0;
        let overflow = |f: &super::metainfo::TorrentMetaInfoV2| MerkleError::LayerLengthMismatch {
            got: f.length as usize,
            expected: u64::MAX as usize,
        };
        for f in &v2.files {
            let layer = meta
                .piece_layers
                .get(&f.pieces_root)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let table = MerkleProofTable::from_layer_bytes(
                f.pieces_root,
                f.length,
                v2.piece_length,
                layer,
            )?;
            tables.push(table);
            // Same alignment the v1 facade materialises as padding entries
            let start = acc
                .checked_add(super::metainfo::v2_alignment_padding(
                    acc,
                    f.length,
                    v2.piece_length,
                ))
                .ok_or_else(|| overflow(f))?;
            offsets.push(start);
            acc = start.checked_add(f.length).ok_or_else(|| overflow(f))?;
        }
        offsets.push(acc);
        Ok(Self::V2Merkle {
            tables: Arc::new(tables),
            file_offsets: Arc::new(offsets),
            piece_length: v2.piece_length,
        })
    }

    /// Verify a fully-downloaded piece. `piece_bytes` is the full piece (last piece may be partial). Returns `Ok(())` on successful match
    pub fn verify(&self, piece_index: u32, piece_bytes: &[u8]) -> Result<(), VerifyError> {
        match self {
            Self::V1Sha1 { pieces } => {
                let start = piece_index as usize * 20;
                let expected = pieces
                    .get(start..start + 20)
                    .ok_or(VerifyError::PieceOutOfRange(piece_index))?;
                let got = super::hash::sha1(piece_bytes);
                if got.as_bytes() != expected {
                    return Err(VerifyError::HashMismatch);
                }
                Ok(())
            }
            Self::V2Merkle {
                tables,
                file_offsets,
                piece_length,
            } => {
                // v2 pieces never span file boundaries (BEP 52 padding files ensure each file starts on a piece boundary). Locate the file owning this piece
                let piece_offset = piece_index as u64 * *piece_length as u64;
                let file_idx = match file_offsets.binary_search_by(|off| {
                    if *off <= piece_offset {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }) {
                    // binary_search returns Err(insertion_point) when no exact match
                    Ok(i) | Err(i) => i.saturating_sub(1),
                };
                let table = tables
                    .get(file_idx)
                    .ok_or(VerifyError::PieceOutOfRange(piece_index))?;
                let local_piece_idx =
                    ((piece_offset - file_offsets[file_idx]) / *piece_length as u64) as u32;
                // A file's last piece is followed by alignment padding up to the next file's piece boundary; only the file's own bytes are hashed
                let own_len = table
                    .piece_size(local_piece_idx)
                    .ok_or(VerifyError::PieceOutOfRange(piece_index))?
                    as usize;
                let piece_bytes = piece_bytes.get(..own_len).unwrap_or(piece_bytes);
                table
                    .verify_piece(local_piece_idx, piece_bytes)
                    .map_err(|e| match e {
                        MerkleError::PieceOutOfRange(i) => VerifyError::PieceOutOfRange(i),
                        MerkleError::RootMismatch => VerifyError::HashMismatch,
                        other => VerifyError::Merkle(other.to_string()),
                    })
            }
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("verify: piece hash mismatch")]
    HashMismatch,
    #[error("verify: piece index {0} out of range")]
    PieceOutOfRange(u32),
    #[error("verify: merkle: {0}")]
    Merkle(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synth_file(file_length: u64, piece_length: u32) -> (Vec<u8>, MerkleProofTable) {
        // Build a deterministic byte pattern, hash all blocks, derive layer
        let data: Vec<u8> = (0..file_length).map(|i| (i & 0xff) as u8).collect();
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let piece_count = file_length.div_ceil(piece_length as u64) as u32;

        // Per-piece roots
        let mut piece_roots = Vec::with_capacity(piece_count as usize);
        for p in 0..piece_count {
            let start = (p as u64 * piece_length as u64) as usize;
            let end = (start + piece_length as usize).min(file_length as usize);
            let piece_bytes = &data[start..end];
            let mut block_hashes: Vec<Id32> = piece_bytes
                .chunks(BLOCK_SIZE as usize)
                .map(hash_block)
                .collect();
            let target = blocks_per_piece as usize;
            if block_hashes.len() < target {
                block_hashes.resize(target, Id32([0u8; 32]));
            }
            let root = if target == 1 {
                block_hashes[0]
            } else {
                compute_root_padded(&block_hashes, target.next_power_of_two())
            };
            piece_roots.push(root);
        }

        let file_root = if piece_count == 1 {
            piece_roots[0]
        } else {
            compute_root(&piece_roots)
        };

        let layer_bytes: Vec<u8> = if file_length <= piece_length as u64 {
            Vec::new()
        } else {
            let mut buf = Vec::with_capacity(piece_count as usize * 32);
            for r in &piece_roots {
                buf.extend_from_slice(&r.0);
            }
            buf
        };

        let table =
            MerkleProofTable::from_layer_bytes(file_root, file_length, piece_length, &layer_bytes)
                .expect("table builds");
        (data, table)
    }

    #[test]
    fn single_block_file_verifies() {
        let (data, table) = synth_file(8 * 1024, 16 * 1024);
        assert_eq!(table.piece_count, 1);
        assert!(table.piece_root_hashes.is_empty());
        table.verify_piece(0, &data).unwrap();
    }

    #[test]
    fn multi_piece_file_verifies() {
        // 4 pieces of 64 KiB each (4 blocks per piece)
        let piece_len: u32 = 64 * 1024;
        let (data, table) = synth_file(4 * piece_len as u64, piece_len);
        assert_eq!(table.piece_count, 4);
        assert_eq!(table.piece_root_hashes.len(), 4);
        for p in 0..4 {
            let start = (p * piece_len) as usize;
            let end = start + piece_len as usize;
            table.verify_piece(p, &data[start..end]).unwrap();
        }
    }

    #[test]
    fn last_piece_partial_verifies() {
        // 3.5 pieces => last piece is partial
        let piece_len: u32 = 64 * 1024;
        let file_len: u64 = (3 * piece_len + piece_len / 2) as u64;
        let (data, table) = synth_file(file_len, piece_len);
        assert_eq!(table.piece_count, 4);
        let last_size = table.piece_size(3).unwrap();
        let start = (3 * piece_len) as usize;
        table
            .verify_piece(3, &data[start..start + last_size as usize])
            .unwrap();
    }

    #[test]
    fn corrupted_byte_rejected() {
        let piece_len: u32 = 64 * 1024;
        let (mut data, table) = synth_file(2 * piece_len as u64, piece_len);
        // Flip a byte in piece 1
        data[piece_len as usize + 100] ^= 0xff;
        let start = piece_len as usize;
        let end = 2 * piece_len as usize;
        let err = table.verify_piece(1, &data[start..end]).unwrap_err();
        assert!(matches!(err, MerkleError::RootMismatch));
        // Piece 0 still verifies
        table.verify_piece(0, &data[..piece_len as usize]).unwrap();
    }

    #[test]
    fn layer_length_mismatch_rejected() {
        let file_root = Id32([0x77u8; 32]);
        let bad_layer = vec![0u8; 33]; // not multiple of 32
        let err = MerkleProofTable::from_layer_bytes(file_root, 1024 * 1024, 16 * 1024, &bad_layer)
            .unwrap_err();
        assert!(matches!(err, MerkleError::LayerNotAligned));
    }

    #[test]
    fn wrong_root_rejected_at_build() {
        // 2 pieces, supply mismatched layer hashes that don't collapse to root
        let piece_len: u32 = 16 * 1024;
        let file_len: u64 = (2 * piece_len) as u64;
        let bogus_layer = vec![0u8; 2 * 32];
        let bogus_root = Id32([0x88u8; 32]);
        let err = MerkleProofTable::from_layer_bytes(bogus_root, file_len, piece_len, &bogus_layer)
            .unwrap_err();
        assert!(matches!(err, MerkleError::RootMismatch));
    }

    #[test]
    fn full_piece_layer_serve_and_verify_round_trip() {
        // 5-piece file (next_pow2 = 8) so we exercise zero-padding
        let piece_len: u32 = 64 * 1024;
        let file_len: u64 = 5 * piece_len as u64 - 1024;
        let (_data, table) = synth_file(file_len, piece_len);
        assert_eq!(table.piece_count, 5);
        assert_eq!(table.piece_layer_padded_len(), 8);
        let served = table.serve_full_piece_layer().expect("multi-piece serves");
        assert_eq!(served.len(), 8 * 32);
        // Last 3 entries must be zero (padding)
        assert!(served[(5 * 32)..(8 * 32)].iter().all(|&b| b == 0));
        let canonical = MerkleProofTable::verify_full_piece_layer_response(
            table.file_root,
            file_len,
            piece_len,
            &served,
        )
        .expect("verifies");
        assert_eq!(canonical.len(), 5 * 32);
        // Round-trip: rebuild a table from the canonical bytes
        let rebuilt =
            MerkleProofTable::from_layer_bytes(table.file_root, file_len, piece_len, &canonical)
                .expect("rebuilds");
        assert_eq!(rebuilt.piece_root_hashes, table.piece_root_hashes);
    }

    #[test]
    fn full_piece_layer_response_rejects_tampered_padding() {
        let piece_len: u32 = 64 * 1024;
        let file_len: u64 = 5 * piece_len as u64 - 1024;
        let (_data, table) = synth_file(file_len, piece_len);
        let mut served = table.serve_full_piece_layer().unwrap();
        served[6 * 32] ^= 0xff; // corrupt a padding entry
        let err = MerkleProofTable::verify_full_piece_layer_response(
            table.file_root,
            file_len,
            piece_len,
            &served,
        )
        .unwrap_err();
        assert!(matches!(err, MerkleError::RootMismatch));
    }

    #[test]
    fn full_piece_layer_response_rejects_tampered_leaf() {
        let piece_len: u32 = 64 * 1024;
        let file_len: u64 = 4 * piece_len as u64;
        let (_data, table) = synth_file(file_len, piece_len);
        let mut served = table.serve_full_piece_layer().unwrap();
        served[100] ^= 0x01;
        let err = MerkleProofTable::verify_full_piece_layer_response(
            table.file_root,
            file_len,
            piece_len,
            &served,
        )
        .unwrap_err();
        assert!(matches!(err, MerkleError::RootMismatch));
    }

    #[test]
    fn small_file_has_no_piece_layer() {
        let piece_len: u32 = 16 * 1024;
        let (_data, table) = synth_file(8 * 1024, piece_len);
        assert_eq!(table.piece_layer_padded_len(), 0);
        assert!(table.serve_full_piece_layer().is_none());
    }
}
