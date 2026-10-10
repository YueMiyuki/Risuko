use super::hash::{sha256, Id32};

pub const BLOCK_SIZE: u32 = 16 * 1024;

pub fn hash_block(block: &[u8]) -> Id32 {
    if block.len() == BLOCK_SIZE as usize {
        return sha256(block);
    }
    let mut padded = vec![0u8; BLOCK_SIZE as usize];
    padded[..block.len()].copy_from_slice(block);
    sha256(&padded)
}

pub fn hash_pair(left: &Id32, right: &Id32) -> Id32 {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(&left.0);
    buf[32..].copy_from_slice(&right.0);
    sha256(&buf)
}

fn reduce_layer(layer: &[Id32]) -> Vec<Id32> {
    layer
        .chunks_exact(2)
        .map(|pair| hash_pair(&pair[0], &pair[1]))
        .collect()
}

pub fn compute_root(leaves: &[Id32]) -> Id32 {
    if leaves.is_empty() {
        return Id32([0u8; 32]);
    }
    if leaves.len() == 1 {
        return leaves[0];
    }
    let mut layer: Vec<Id32> = leaves.to_vec();
    let target = layer.len().next_power_of_two();
    let zero = Id32([0u8; 32]);
    layer.resize(target, zero);
    while layer.len() > 1 {
        layer = reduce_layer(&layer);
    }
    layer[0]
}

pub fn pad_hash(blocks: u32) -> Id32 {
    let mut pad = Id32([0u8; 32]);
    let mut span = 1u32;
    while span < blocks {
        pad = hash_pair(&pad, &pad);
        span = span.saturating_mul(2);
    }
    pad
}

pub fn compute_root_with_pad(leaves: &[Id32], padded_len: usize, pad: Id32) -> Id32 {
    assert!(padded_len.is_power_of_two() || padded_len == 0);
    assert!(leaves.len() <= padded_len);
    if padded_len == 0 {
        return Id32([0u8; 32]);
    }
    let mut layer: Vec<Id32> = leaves.to_vec();
    layer.resize(padded_len, pad);
    while layer.len() > 1 {
        layer = reduce_layer(&layer);
    }
    layer[0]
}

pub fn compute_root_padded(leaves: &[Id32], padded_len: usize) -> Id32 {
    compute_root_with_pad(leaves, padded_len, Id32([0u8; 32]))
}

pub fn piece_layer_root(piece_hashes: &[Id32], blocks_per_piece: u32) -> Id32 {
    compute_root_with_pad(
        piece_hashes,
        piece_hashes.len().next_power_of_two(),
        pad_hash(blocks_per_piece),
    )
}

#[derive(Debug, Clone)]
pub struct MerkleProofTable {
    pub file_root: Id32,
    pub file_length: u64,
    pub piece_length: u32,
    pub piece_count: u32,
    pub blocks_per_piece: u32,
    pub piece_root_hashes: Vec<Id32>,
    upper_layers: std::sync::OnceLock<Vec<Vec<Id32>>>,
}

pub const HASH_REQUEST_CHUNK: u32 = 512;
pub const MAX_SERVED_HASHES: u32 = 8192;
pub const MAX_SERVED_LEAF_HASHES: u32 = 512;

pub fn piece_layer_requests(piece_count: u32) -> Vec<(u32, u32)> {
    let padded = piece_count.max(2).next_power_of_two();
    if padded <= HASH_REQUEST_CHUNK {
        return vec![(0, padded)];
    }
    (0..padded)
        .step_by(HASH_REQUEST_CHUNK as usize)
        .take_while(|index| *index < piece_count)
        .map(|index| (index, HASH_REQUEST_CHUNK))
        .collect()
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
                upper_layers: std::sync::OnceLock::new(),
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

        let recomputed = piece_layer_root(&piece_root_hashes, blocks_per_piece);
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
            upper_layers: std::sync::OnceLock::new(),
        })
    }

    pub fn piece_size(&self, piece_index: u32) -> Option<u32> {
        if piece_index >= self.piece_count {
            return None;
        }
        let start = piece_index as u64 * self.piece_length as u64;
        let end = (start + self.piece_length as u64).min(self.file_length);
        Some((end - start) as u32)
    }

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

    pub fn piece_layer_padded_len(&self) -> u32 {
        if self.piece_root_hashes.is_empty() {
            0
        } else {
            (self.piece_count as usize).next_power_of_two().max(2) as u32
        }
    }

    pub fn piece_layer_base(&self) -> u32 {
        self.blocks_per_piece.trailing_zeros()
    }

    #[cfg(test)]
    pub fn serve_full_piece_layer(&self) -> Option<Vec<u8>> {
        let padded = self.piece_layer_padded_len() as usize;
        if padded == 0 {
            return None;
        }
        let pad = pad_hash(self.blocks_per_piece);
        let mut out = Vec::with_capacity(padded * 32);
        for h in &self.piece_root_hashes {
            out.extend_from_slice(&h.0);
        }
        while out.len() < padded * 32 {
            out.extend_from_slice(&pad.0);
        }
        Some(out)
    }

    fn upper_layers(&self) -> &[Vec<Id32>] {
        self.upper_layers.get_or_init(|| {
            let padded = self.piece_layer_padded_len() as usize;
            let mut layer = self.piece_root_hashes.clone();
            layer.resize(padded, pad_hash(self.blocks_per_piece));
            let mut layers = vec![layer];
            while layers.last().is_some_and(|l| l.len() > 1) {
                let next = reduce_layer(layers.last().unwrap());
                layers.push(next);
            }
            layers
        })
    }

    fn block_count(&self) -> u64 {
        self.file_length.div_ceil(BLOCK_SIZE as u64)
    }

    fn leaf_count(&self) -> u64 {
        if self.piece_root_hashes.is_empty() {
            self.block_count().max(1).next_power_of_two()
        } else {
            self.blocks_per_piece as u64 * self.piece_layer_padded_len() as u64
        }
    }

    pub fn leaf_request_pieces(
        &self,
        index: u32,
        length: u32,
        proof_layers: u32,
    ) -> Option<Vec<u32>> {
        if self.file_length == 0 || length < 2 || !length.is_power_of_two() {
            return None;
        }
        if length > MAX_SERVED_LEAF_HASHES || !index.is_multiple_of(length) {
            return None;
        }
        let leaves = self.leaf_count();
        if index as u64 + length as u64 > leaves {
            return None;
        }
        let height = leaves.trailing_zeros();
        let subtree_layers = length.trailing_zeros();
        let uncles = crate::wire::message::hashes_uncle_count(length, proof_layers) as u32;
        if subtree_layers.checked_add(uncles)? > height {
            return None;
        }
        let piece_layer = self.blocks_per_piece.trailing_zeros();
        let (mut lo, mut hi) = (index as u64, index as u64 + length as u64);
        let mut node = (index / length) as u64;
        for layer in subtree_layers..subtree_layers + uncles {
            if !self.piece_root_hashes.is_empty() && layer >= piece_layer {
                break;
            }
            let sibling = node ^ 1;
            lo = lo.min(sibling << layer);
            hi = hi.max((sibling + 1) << layer);
            node >>= 1;
        }
        let hi = hi.min(self.block_count());
        if lo >= hi {
            return Some(Vec::new());
        }
        let bpp = self.blocks_per_piece as u64;
        Some(((lo / bpp) as u32..=((hi - 1) / bpp) as u32).collect())
    }

    pub fn answer_leaf_request(
        &self,
        index: u32,
        length: u32,
        proof_layers: u32,
        piece_blocks: &std::collections::HashMap<u32, Vec<Id32>>,
    ) -> Option<Vec<u8>> {
        self.leaf_request_pieces(index, length, proof_layers)?;
        let bpp = self.blocks_per_piece as u64;
        let blocks = self.block_count();
        let piece_layer = self.blocks_per_piece.trailing_zeros();
        let block_hash = |b: u64| -> Option<Id32> {
            if b >= blocks {
                return Some(Id32([0u8; 32]));
            }
            piece_blocks
                .get(&((b / bpp) as u32))?
                .get((b % bpp) as usize)
                .copied()
        };
        let node_hash = |layer: u32, i: u64| -> Option<Id32> {
            if !self.piece_root_hashes.is_empty() && layer >= piece_layer {
                let upper = self.upper_layers();
                return upper
                    .get((layer - piece_layer) as usize)?
                    .get(i as usize)
                    .copied();
            }
            let mut level: Vec<Id32> = ((i << layer)..((i + 1) << layer))
                .map(block_hash)
                .collect::<Option<_>>()?;
            while level.len() > 1 {
                level = reduce_layer(&level);
            }
            level.first().copied()
        };
        let subtree_layers = length.trailing_zeros();
        let uncles = crate::wire::message::hashes_uncle_count(length, proof_layers) as u32;
        let mut out = Vec::with_capacity((length + uncles) as usize * 32);
        for b in index as u64..index as u64 + length as u64 {
            out.extend_from_slice(&block_hash(b)?.0);
        }
        let mut node = (index / length) as u64;
        for layer in subtree_layers..subtree_layers + uncles {
            out.extend_from_slice(&node_hash(layer, node ^ 1)?.0);
            node >>= 1;
        }
        Some(out)
    }

    pub fn hashes_for_request(
        &self,
        base_layer: u32,
        index: u32,
        length: u32,
        proof_layers: u32,
    ) -> Option<Vec<u8>> {
        if self.piece_root_hashes.is_empty() || base_layer != self.piece_layer_base() {
            return None;
        }
        if length < 2 || !length.is_power_of_two() || length > MAX_SERVED_HASHES {
            return None;
        }
        if !index.is_multiple_of(length) {
            return None;
        }
        let layers = self.upper_layers();
        let base = &layers[0];
        let end = index.checked_add(length)? as usize;
        if end > base.len() {
            return None;
        }
        let subtree_layers = length.trailing_zeros();
        let uncles = crate::wire::message::hashes_uncle_count(length, proof_layers) as u32;
        let height = layers.len() as u32 - 1;
        if subtree_layers.checked_add(uncles)? > height {
            return None;
        }
        let mut out = Vec::with_capacity((length + uncles) as usize * 32);
        for hash in &base[index as usize..end] {
            out.extend_from_slice(&hash.0);
        }
        let mut node = index / length;
        for layer in &layers[subtree_layers as usize..(subtree_layers + uncles) as usize] {
            out.extend_from_slice(&layer[(node ^ 1) as usize].0);
            node >>= 1;
        }
        Some(out)
    }

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
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let pad = pad_hash(blocks_per_piece);
        if leaves[piece_count as usize..].iter().any(|h| *h != pad) {
            return Err(MerkleError::RootMismatch);
        }
        let recomputed = compute_root_with_pad(&leaves, padded, pad);
        if recomputed != file_root {
            return Err(MerkleError::RootMismatch);
        }
        let mut out = Vec::with_capacity(piece_count as usize * 32);
        for h in &leaves[..piece_count as usize] {
            out.extend_from_slice(&h.0);
        }
        Ok(out)
    }

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

use std::sync::Arc;

use super::metainfo::TorrentMeta;

pub fn build_v2_tables(meta: &TorrentMeta) -> Option<Result<Vec<MerkleProofTable>, MerkleError>> {
    let v2 = meta.info_v2.as_ref()?;
    Some(
        v2.files
            .iter()
            .map(|file| {
                let layer = meta
                    .piece_layers
                    .get(&file.pieces_root)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                MerkleProofTable::from_layer_bytes(
                    file.pieces_root,
                    file.length,
                    v2.piece_length,
                    layer,
                )
            })
            .collect(),
    )
}

pub fn supports_v2_wire(meta: &TorrentMeta) -> bool {
    matches!(build_v2_tables(meta), Some(Ok(_)))
}

#[derive(Debug, Clone)]
pub enum PieceVerifier {
    V1Sha1 {
        pieces: Arc<Vec<u8>>,
    },
    V2Merkle {
        tables: Arc<Vec<MerkleProofTable>>,
        file_offsets: Arc<Vec<u64>>,
        piece_length: u32,
    },
}

impl PieceVerifier {
    pub fn from_meta(meta: &TorrentMeta) -> Result<Self, MerkleError> {
        if meta.meta_version.has_v1() {
            return Ok(Self::V1Sha1 {
                pieces: Arc::new(meta.info.pieces.clone()),
            });
        }
        let v2 = meta
            .info_v2
            .as_ref()
            .expect("pure-v2 metainfo without info_v2");
        let tables = build_v2_tables(meta).transpose()?.unwrap_or_default();
        let mut offsets = Vec::with_capacity(v2.files.len() + 1);
        let mut acc: u64 = 0;
        let overflow = |f: &super::metainfo::TorrentMetaInfoV2| MerkleError::LayerLengthMismatch {
            got: f.length as usize,
            expected: u64::MAX as usize,
        };
        for f in &v2.files {
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
                let piece_offset = piece_index as u64 * *piece_length as u64;
                let file_idx = match file_offsets.binary_search_by(|off| {
                    if *off <= piece_offset {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }) {
                    Ok(i) | Err(i) => i.saturating_sub(1),
                };
                let table = tables
                    .get(file_idx)
                    .ok_or(VerifyError::PieceOutOfRange(piece_index))?;
                let local_piece_idx =
                    ((piece_offset - file_offsets[file_idx]) / *piece_length as u64) as u32;
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
    use std::collections::HashMap;

    fn synth_file(file_length: u64, piece_length: u32) -> (Vec<u8>, MerkleProofTable) {
        let data: Vec<u8> = (0..file_length).map(|i| (i & 0xff) as u8).collect();
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let piece_count = file_length.div_ceil(piece_length as u64) as u32;

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
            let blocks: Vec<Id32> = data.chunks(BLOCK_SIZE as usize).map(hash_block).collect();
            compute_root(&blocks)
        } else {
            piece_layer_root(&piece_roots, blocks_per_piece)
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
        data[piece_len as usize + 100] ^= 0xff;
        let start = piece_len as usize;
        let end = 2 * piece_len as usize;
        let err = table.verify_piece(1, &data[start..end]).unwrap_err();
        assert!(matches!(err, MerkleError::RootMismatch));
        table.verify_piece(0, &data[..piece_len as usize]).unwrap();
    }

    #[test]
    fn layer_length_mismatch_rejected() {
        let file_root = Id32([0x77u8; 32]);
        let bad_layer = vec![0u8; 33];
        let err = MerkleProofTable::from_layer_bytes(file_root, 1024 * 1024, 16 * 1024, &bad_layer)
            .unwrap_err();
        assert!(matches!(err, MerkleError::LayerNotAligned));
    }

    #[test]
    fn wrong_root_rejected_at_build() {
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
        let piece_len: u32 = 64 * 1024;
        let file_len: u64 = 5 * piece_len as u64 - 1024;
        let (_data, table) = synth_file(file_len, piece_len);
        assert_eq!(table.piece_count, 5);
        assert_eq!(table.piece_layer_padded_len(), 8);
        let served = table.serve_full_piece_layer().expect("multi-piece serves");
        assert_eq!(served.len(), 8 * 32);
        let pad = pad_hash(4);
        assert_ne!(pad.0, [0u8; 32]);
        assert!(served[(5 * 32)..(8 * 32)]
            .chunks_exact(32)
            .all(|entry| entry == pad.0));
        let canonical = MerkleProofTable::verify_full_piece_layer_response(
            table.file_root,
            file_len,
            piece_len,
            &served,
        )
        .expect("verifies");
        assert_eq!(canonical.len(), 5 * 32);
        let rebuilt =
            MerkleProofTable::from_layer_bytes(table.file_root, file_len, piece_len, &canonical)
                .expect("rebuilds");
        assert_eq!(rebuilt.piece_root_hashes, table.piece_root_hashes);
    }

    #[test]
    fn piece_layer_root_matches_the_leaf_level_tree() {
        let blocks: Vec<Id32> = (0u8..5)
            .map(|i| hash_block(&[i; BLOCK_SIZE as usize]))
            .collect();
        let truth = compute_root(&blocks);
        let pieces: Vec<Id32> = blocks
            .chunks(2)
            .map(|piece| compute_root_padded(piece, 2))
            .collect();
        assert_eq!(piece_layer_root(&pieces, 2), truth);
        assert_ne!(compute_root(&pieces), truth);

        let layer: Vec<u8> = pieces.iter().flat_map(|p| p.0).collect();
        let file_len = 5 * BLOCK_SIZE as u64;
        MerkleProofTable::from_layer_bytes(truth, file_len, 2 * BLOCK_SIZE, &layer)
            .expect("libtorrent-shaped layer validates");
    }

    #[test]
    fn hash_requests_are_chunked_and_skip_pure_padding() {
        assert_eq!(piece_layer_requests(1), vec![(0, 2)]);
        assert_eq!(piece_layer_requests(300), vec![(0, 512)]);
        assert_eq!(piece_layer_requests(512), vec![(0, 512)]);
        assert_eq!(
            piece_layer_requests(1100),
            vec![(0, 512), (512, 512), (1024, 512)]
        );
    }

    #[test]
    fn serves_piece_layer_ranges_with_uncle_proofs() {
        let piece_len = 2 * BLOCK_SIZE;
        let file_len = 6 * piece_len as u64 - 100;
        let (_data, table) = synth_file(file_len, piece_len);
        let base = table.piece_layer_base();
        let layers = table.upper_layers().to_vec();
        assert_eq!(layers.len(), 4);
        assert_eq!(layers[3][0], table.file_root);

        let whole = table.hashes_for_request(base, 0, 8, 0).unwrap();
        assert_eq!(whole, table.serve_full_piece_layer().unwrap());

        let ranged = table.hashes_for_request(base, 4, 2, 2).unwrap();
        assert_eq!(ranged.len(), 4 * 32);
        let hashes: Vec<Id32> = ranged
            .chunks_exact(32)
            .map(|c| Id32::from_slice(c).unwrap())
            .collect();
        let pair = hash_pair(&hashes[0], &hashes[1]);
        let up = hash_pair(&pair, &hashes[2]);
        assert_eq!(hash_pair(&hashes[3], &up), table.file_root);
        assert!(table.hashes_for_request(base, 4, 2, 3).is_none());

        assert!(
            table.hashes_for_request(base, 3, 2, 0).is_none(),
            "unaligned index"
        );
        assert!(
            table.hashes_for_request(base, 0, 3, 0).is_none(),
            "length not a power of two"
        );
        assert!(
            table.hashes_for_request(base, 8, 2, 0).is_none(),
            "past the layer"
        );
        assert!(
            table.hashes_for_request(0, 0, 2, 0).is_none(),
            "leaf layer not held"
        );
    }

    fn leaf_tree_root(data: &[u8], leaves: usize) -> Id32 {
        let mut level: Vec<Id32> = data.chunks(BLOCK_SIZE as usize).map(hash_block).collect();
        level.resize(leaves, Id32([0u8; 32]));
        compute_root(&level)
    }

    fn piece_blocks(data: &[u8], piece_len: u32) -> HashMap<u32, Vec<Id32>> {
        data.chunks(piece_len as usize)
            .enumerate()
            .map(|(p, piece)| {
                (
                    p as u32,
                    piece.chunks(BLOCK_SIZE as usize).map(hash_block).collect(),
                )
            })
            .collect()
    }

    fn climb(response: &[u8], index: u32, length: u32) -> Id32 {
        let hashes: Vec<Id32> = response
            .chunks_exact(32)
            .map(|c| Id32::from_slice(c).unwrap())
            .collect();
        let mut node = compute_root(&hashes[..length as usize]);
        let mut position = index / length;
        for uncle in &hashes[length as usize..] {
            node = if position.is_multiple_of(2) {
                hash_pair(&node, uncle)
            } else {
                hash_pair(uncle, &node)
            };
            position /= 2;
        }
        node
    }

    #[test]
    fn leaf_requests_prove_up_to_the_root() {
        let piece_len = 4 * BLOCK_SIZE;
        let file_len = 4 * piece_len as u64 + BLOCK_SIZE as u64 + 100;
        let (data, table) = synth_file(file_len, piece_len);
        assert_eq!(leaf_tree_root(&data, 32), table.file_root);
        let blocks = piece_blocks(&data, piece_len);

        let full_proof = 32u32.trailing_zeros();
        assert_eq!(
            table.leaf_request_pieces(6, 2, full_proof - 1),
            Some(vec![1])
        );
        let response = table
            .answer_leaf_request(6, 2, full_proof - 1, &blocks)
            .unwrap();
        assert_eq!(response.len(), (2 + 4) * 32);
        assert_eq!(climb(&response, 6, 2), table.file_root);

        assert_eq!(table.leaf_request_pieces(8, 8, 4), Some(vec![2, 3]));
        let response = table.answer_leaf_request(8, 8, 4, &blocks).unwrap();
        assert_eq!(response.len(), (8 + 2) * 32);
        assert_eq!(climb(&response, 8, 8), table.file_root);

        let response = table.answer_leaf_request(16, 4, 4, &blocks).unwrap();
        assert_eq!(climb(&response, 16, 4), table.file_root);

        let mut partial = blocks.clone();
        partial.remove(&1);
        assert!(table.answer_leaf_request(6, 2, 4, &partial).is_none());
        assert!(table.leaf_request_pieces(6, 2, full_proof).is_none());
        assert!(table.leaf_request_pieces(5, 2, 0).is_none());
        assert!(table.leaf_request_pieces(0, 1024, 0).is_none());
    }

    #[test]
    fn leaf_requests_work_for_single_piece_files() {
        let piece_len = 16 * BLOCK_SIZE;
        let file_len = 3 * BLOCK_SIZE as u64 - 10;
        let (data, table) = synth_file(file_len, piece_len);
        let blocks = piece_blocks(&data, piece_len);
        assert_eq!(table.leaf_request_pieces(2, 2, 1), Some(vec![0]));
        let response = table.answer_leaf_request(2, 2, 1, &blocks).unwrap();
        assert_eq!(climb(&response, 2, 2), table.file_root);
    }

    #[test]
    fn full_piece_layer_response_rejects_tampered_padding() {
        let piece_len: u32 = 64 * 1024;
        let file_len: u64 = 5 * piece_len as u64 - 1024;
        let (_data, table) = synth_file(file_len, piece_len);
        let mut served = table.serve_full_piece_layer().unwrap();
        served[6 * 32] ^= 0xff;
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
