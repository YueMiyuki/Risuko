use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use super::super::core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimedChunk {
    pub piece: u32,
    pub begin: u32,
    pub peer: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    Missing,
    Requested { peer: u32, since: Instant },
    Received,
}

pub struct ChunkRequest {
    pub info: ChunkInfo,
    pub prior_state: ChunkState,
}

#[derive(Debug)]
struct PieceChunks {
    states: Vec<ChunkState>,
    missing: u32,
    requested: u32,
}

impl PieceChunks {
    fn new(count: u32) -> Self {
        Self {
            states: vec![ChunkState::Missing; count as usize],
            missing: count,
            requested: 0,
        }
    }

    fn set(&mut self, i: usize, state: ChunkState) {
        let count = |s: &ChunkState| match s {
            ChunkState::Missing => (1, 0),
            ChunkState::Requested { .. } => (0, 1),
            ChunkState::Received => (0, 0),
        };
        let (old_missing, old_requested) = count(&self.states[i]);
        let (new_missing, new_requested) = count(&state);
        self.missing = self.missing + new_missing - old_missing;
        self.requested = self.requested + new_requested - old_requested;
        self.states[i] = state;
    }
}

#[derive(Debug)]
pub struct ChunkTracker {
    lengths: Lengths,
    pieces: BTreeMap<u32, PieceChunks>,
    partial: BTreeSet<u32>,
    contested: BTreeSet<u32>,
    endgame: bool,
}

impl ChunkTracker {
    pub fn new(lengths: Lengths) -> Self {
        Self {
            lengths,
            pieces: BTreeMap::new(),
            partial: BTreeSet::new(),
            contested: BTreeSet::new(),
            endgame: false,
        }
    }

    pub fn lengths(&self) -> &Lengths {
        &self.lengths
    }

    fn reindex(&mut self, piece: u32) {
        let (partial, contested) = self
            .pieces
            .get(&piece)
            .map_or((false, false), |c| (c.missing > 0, c.requested > 0));
        if partial {
            self.partial.insert(piece);
        } else {
            self.partial.remove(&piece);
        }
        if contested {
            self.contested.insert(piece);
        } else {
            self.contested.remove(&piece);
        }
    }

    pub fn set_endgame(&mut self, on: bool) {
        self.endgame = on;
    }

    pub fn endgame(&self) -> bool {
        self.endgame
    }

    #[cfg(test)]
    pub fn next_chunk(&mut self, piece: ValidPieceIndex, peer: u32) -> Option<ChunkRequest> {
        self.next_chunk_skipping(piece, peer, |_| false)
    }

    pub fn next_chunk_skipping(
        &mut self,
        piece: ValidPieceIndex,
        peer: u32,
        asked: impl Fn(u32) -> bool,
    ) -> Option<ChunkRequest> {
        let endgame = self.endgame;
        let lengths = self.lengths;
        let chunks = self.chunks_for(piece);
        let len = chunks.states.len();
        if len == 0 || (chunks.missing == 0 && !endgame) {
            return None;
        }
        let start = if endgame {
            (peer as usize)
                .wrapping_mul(2_654_435_761)
                .wrapping_add(piece.get() as usize)
                % len
        } else {
            0
        };
        let mut candidate: Option<usize> = None;
        let mut missing: Option<usize> = None;
        for k in 0..len {
            let i = (start + k) % len;
            match chunks.states[i] {
                ChunkState::Missing if !asked(i as u32) => {
                    missing = Some(i);
                    break;
                }
                ChunkState::Requested { peer: p, .. }
                    if endgame && p != peer && candidate.is_none() && !asked(i as u32) =>
                {
                    candidate = Some(i);
                }
                _ => {}
            }
        }
        let i = missing.or(candidate)?;
        let info = lengths.chunk_info(piece, i as u32)?;
        let prior_state = chunks.states[i];
        chunks.set(
            i,
            ChunkState::Requested {
                peer,
                since: Instant::now(),
            },
        );
        self.reindex(piece.get());
        Some(ChunkRequest { info, prior_state })
    }

    pub fn has_missing(&self, piece: ValidPieceIndex) -> bool {
        self.pieces
            .get(&piece.get())
            .is_none_or(|chunks| chunks.missing > 0)
    }

    pub fn partial_pieces(&self) -> impl Iterator<Item = u32> + '_ {
        self.partial.iter().copied()
    }

    #[cfg(test)]
    pub fn is_tracked(&self, piece: u32) -> bool {
        self.pieces.contains_key(&piece)
    }

    pub fn has_live_requests(&self, piece: u32) -> bool {
        self.contested.contains(&piece)
    }

    pub fn contested_pieces(&self) -> impl Iterator<Item = u32> + '_ {
        self.contested.iter().copied()
    }

    pub fn mark_received(&mut self, info: ChunkInfo) -> bool {
        let chunks = self.chunks_for(info.piece_index);
        let idx = info.chunk_index as usize;
        if idx < chunks.states.len() {
            chunks.set(idx, ChunkState::Received);
            let done = chunks.missing == 0 && chunks.requested == 0;
            self.reindex(info.piece_index.get());
            done
        } else {
            false
        }
    }

    pub fn release_peer(&mut self, peer: u32) -> Vec<u32> {
        let mut affected = Vec::new();
        for (&piece_idx, chunks) in self.pieces.iter_mut() {
            let mut freed = false;
            for i in 0..chunks.states.len() {
                if let ChunkState::Requested { peer: p, .. } = chunks.states[i] {
                    if p == peer {
                        chunks.set(i, ChunkState::Missing);
                        freed = true;
                    }
                }
            }
            if freed {
                affected.push(piece_idx);
            }
        }
        for &piece in &affected {
            self.reindex(piece);
        }
        affected
    }

    pub fn reset_piece(&mut self, piece: ValidPieceIndex) {
        if let Some(chunks) = self.pieces.get_mut(&piece.get()) {
            chunks.states.fill(ChunkState::Missing);
            chunks.missing = chunks.states.len() as u32;
            chunks.requested = 0;
        }
        self.reindex(piece.get());
    }

    pub fn unrequest_chunk(
        &mut self,
        piece: ValidPieceIndex,
        chunk_index: u32,
        prior_state: ChunkState,
    ) {
        if let Some(chunks) = self.pieces.get_mut(&piece.get()) {
            let i = chunk_index as usize;
            if matches!(chunks.states.get(i), Some(ChunkState::Requested { .. })) {
                chunks.set(i, prior_state);
            }
        }
        self.reindex(piece.get());
    }

    pub fn reject_chunk(&mut self, piece: ValidPieceIndex, chunk_index: u32, peer: u32) {
        if let Some(chunks) = self.pieces.get_mut(&piece.get()) {
            let i = chunk_index as usize;
            if matches!(chunks.states.get(i), Some(ChunkState::Requested { peer: p, .. }) if *p == peer)
            {
                chunks.set(i, ChunkState::Missing);
            }
        }
        self.reindex(piece.get());
    }

    pub fn pending_chunks(&self) -> usize {
        self.pieces
            .values()
            .map(|chunks| {
                chunks
                    .states
                    .iter()
                    .filter(|s| !matches!(s, ChunkState::Received))
                    .count()
            })
            .sum()
    }

    pub fn reclaim_stale(&mut self, timeout: Duration) -> Vec<ReclaimedChunk> {
        let now = Instant::now();
        let chunk_size = super::super::core::CHUNK_SIZE;
        let mut out = Vec::new();
        for (&piece_idx, chunks) in self.pieces.iter_mut() {
            for ci in 0..chunks.states.len() {
                if let ChunkState::Requested { peer, since } = chunks.states[ci] {
                    if now.duration_since(since) >= timeout {
                        chunks.set(ci, ChunkState::Missing);
                        out.push(ReclaimedChunk {
                            piece: piece_idx,
                            begin: (ci as u32) * chunk_size,
                            peer,
                        });
                    }
                }
            }
        }
        for r in &out {
            self.reindex(r.piece);
        }
        out
    }

    pub fn forget_piece(&mut self, piece: ValidPieceIndex) {
        self.pieces.remove(&piece.get());
        self.reindex(piece.get());
    }

    fn chunks_for(&mut self, piece: ValidPieceIndex) -> &mut PieceChunks {
        let lengths = &self.lengths;
        self.pieces.entry(piece.get()).or_insert_with(|| {
            PieceChunks::new(
                lengths
                    .piece_length_of(piece)
                    .div_ceil(super::super::core::CHUNK_SIZE),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_then_completes() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p0 = l.validate_piece(0).unwrap();

        let r0 = t.next_chunk(p0, 1).unwrap();
        assert_eq!(r0.info.chunk_index, 0);
        let r1 = t.next_chunk(p0, 1).unwrap();
        assert_eq!(r1.info.chunk_index, 1);

        assert!(!t.mark_received(r0.info));
        assert!(t.mark_received(r1.info));
    }

    #[test]
    fn endgame_duplicates_request() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let _a = t.next_chunk(p, 1).unwrap();
        let _b = t.next_chunk(p, 1).unwrap();
        assert!(t.next_chunk(p, 2).is_none());
        t.set_endgame(true);
        let dup = t.next_chunk(p, 2).unwrap();
        assert_eq!(dup.info.chunk_index, 0);
    }

    #[test]
    fn release_frees_requests() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let _ = t.next_chunk(p, 1).unwrap();
        t.release_peer(1);
        let again = t.next_chunk(p, 2).unwrap();
        assert_eq!(again.info.chunk_index, 0);
    }

    #[test]
    fn reject_frees_request_immediately() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let r = t.next_chunk(p, 1).unwrap();

        t.reject_chunk(p, r.info.chunk_index, 1);

        let again = t.next_chunk(p, 2).unwrap();
        assert_eq!(again.info.chunk_index, r.info.chunk_index);
    }

    #[test]
    fn reject_does_not_clear_other_peer_request() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let r0 = t.next_chunk(p, 1).unwrap();

        t.reject_chunk(p, r0.info.chunk_index, 2);

        let r1 = t.next_chunk(p, 2).unwrap();
        assert_ne!(r1.info.chunk_index, r0.info.chunk_index);
    }

    #[test]
    fn release_preserves_received_from_other_peers() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let r0 = t.next_chunk(p, 1).unwrap();
        let _r1 = t.next_chunk(p, 2).unwrap();
        assert!(!t.mark_received(r0.info));
        let freed = t.release_peer(2);
        assert_eq!(freed, vec![0]);
        let r1b = t.next_chunk(p, 3).unwrap();
        assert_eq!(r1b.info.chunk_index, 1);
        assert!(t.mark_received(r1b.info));
    }

    #[test]
    fn reclaim_stale_reverts_to_missing() {
        let l = Lengths::new(32 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();

        let _r0 = t.next_chunk(p, 42).unwrap();
        let _r1 = t.next_chunk(p, 42).unwrap();
        assert!(t.reclaim_stale(Duration::from_secs(3600)).is_empty());

        std::thread::sleep(Duration::from_millis(2));
        let reclaimed = t.reclaim_stale(Duration::from_millis(1));
        assert_eq!(reclaimed.len(), 2);
        assert!(reclaimed.iter().all(|r| r.peer == 42 && r.piece == 0));

        let fresh = t.next_chunk(p, 99).unwrap();
        assert_eq!(fresh.info.chunk_index, 0);
    }

    #[test]
    fn forget_piece_drops_state() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let _ = t.next_chunk(p, 1);
        assert_eq!(t.pending_chunks(), 2);
        t.forget_piece(p);
        assert_eq!(t.pending_chunks(), 0);
    }

    #[test]
    fn endgame_never_hands_a_peer_a_chunk_it_already_asked_for() {
        let l = Lengths::new(32 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        let a0 = t.next_chunk(p, 1).unwrap();
        let a1 = t.next_chunk(p, 1).unwrap();
        t.set_endgame(true);
        let dup = t.next_chunk(p, 2).unwrap();
        assert!(matches!(
            dup.prior_state,
            ChunkState::Requested { peer: 1, .. }
        ));
        let asked_by_1 = [a0.info.chunk_index, a1.info.chunk_index];
        assert!(t
            .next_chunk_skipping(p, 1, |c| asked_by_1.contains(&c))
            .is_none());
    }

    #[test]
    fn missing_count_follows_every_state_change() {
        let l = Lengths::new(64 * 1024, 64 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p = l.validate_piece(0).unwrap();
        assert!(t.has_missing(p));
        let r: Vec<_> = (0..4).map(|_| t.next_chunk(p, 1).unwrap()).collect();
        assert!(!t.has_missing(p));
        assert!(t.next_chunk(p, 2).is_none());
        assert_eq!(t.partial_pieces().count(), 0);
        t.unrequest_chunk(p, r[3].info.chunk_index, r[3].prior_state);
        assert!(t.has_missing(p));
        assert_eq!(t.partial_pieces().collect::<Vec<_>>(), vec![0]);
        t.reject_chunk(p, r[2].info.chunk_index, 1);
        t.mark_received(r[0].info);
        t.release_peer(1);
        assert_eq!(t.pieces[&0].missing, 3);
        t.reset_piece(p);
        assert_eq!(t.pieces[&0].missing, 4);
        assert_eq!(t.pieces[&0].requested, 0);
        assert_eq!(t.partial_pieces().collect::<Vec<_>>(), vec![0]);
        assert_eq!(t.contested_pieces().count(), 0);
    }

    #[test]
    fn contested_index_tracks_live_requests_only() {
        let l = Lengths::new(64 * 1024, 32 * 1024).unwrap();
        let mut t = ChunkTracker::new(l);
        let p0 = l.validate_piece(0).unwrap();
        let p1 = l.validate_piece(1).unwrap();
        let a = t.next_chunk(p0, 1).unwrap();
        let b = t.next_chunk(p0, 1).unwrap();
        let c = t.next_chunk(p1, 2).unwrap();
        assert_eq!(t.contested_pieces().collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(t.partial_pieces().collect::<Vec<_>>(), vec![1]);
        t.mark_received(a.info);
        assert!(t.mark_received(b.info));
        assert_eq!(t.contested_pieces().collect::<Vec<_>>(), vec![1]);
        assert!(t.has_live_requests(1));
        t.release_peer(2);
        assert!(!t.has_live_requests(1));
        assert!(t.is_tracked(1));
        assert_eq!(t.contested_pieces().count(), 0);
        assert_eq!(t.partial_pieces().collect::<Vec<_>>(), vec![1]);
        let _ = c;
        t.forget_piece(p1);
        assert_eq!(t.partial_pieces().count(), 0);
    }

    #[test]
    fn chunk_info_matches_chunks_of() {
        let l = Lengths::new(3 * 64 * 1024 + 5000, 64 * 1024).unwrap();
        for piece in 0..l.total_pieces() {
            let p = l.validate_piece(piece).unwrap();
            for c in l.chunks_of(p) {
                assert_eq!(l.chunk_info(p, c.chunk_index), Some(c));
            }
            assert_eq!(l.chunk_info(p, l.chunks_of(p).count() as u32), None);
        }
    }
}
