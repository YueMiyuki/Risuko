//! Per-piece availability tracking (rarest-first selection): for each piece keeps how many connected peers advertise it and whether we own it locally, where pieces are LOCAL (verified on disk), IN-FLIGHT (all chunks requested but unverified) or REQUESTABLE (neither); `choose_requestable_piece` returns only REQUESTABLE pieces so peers get different work, while `choose_piece` (interest checks) ignores the in-flight flag

use super::super::core::lengths::{Lengths, ValidPieceIndex};

pub struct PieceTracker {
    lengths: Lengths,
    have_local: Vec<bool>,
    /// Pieces where all chunks are requested but not yet hash-verified; cleared on verify success/failure or when a peer owning chunks disconnects
    in_flight: Vec<bool>,
    /// How many peers advertise each piece
    availability: Vec<u32>,
    sorted: Vec<ValidPieceIndex>,
    sorted_dirty: bool,
    /// Scratch buffer reused by `choose_piece` to avoid allocations
    scratch: Vec<ValidPieceIndex>,
}

impl PieceTracker {
    pub fn new(lengths: Lengths) -> Self {
        let n = lengths.total_pieces() as usize;
        Self {
            lengths,
            have_local: vec![false; n],
            in_flight: vec![false; n],
            availability: vec![0; n],
            sorted: Vec::new(),
            sorted_dirty: true,
            scratch: Vec::with_capacity(n.min(1024)),
        }
    }

    pub fn lengths(&self) -> &Lengths {
        &self.lengths
    }

    pub fn set_local(&mut self, idx: ValidPieceIndex, have: bool) {
        self.have_local[idx.get_usize()] = have;
        self.sorted_dirty = true;
        if have {
            // No longer in-flight once verified
            self.in_flight[idx.get_usize()] = false;
        }
    }

    pub fn has_local(&self, idx: ValidPieceIndex) -> bool {
        self.have_local[idx.get_usize()]
    }

    /// Mark a piece as fully in-flight (all chunks requested, awaiting hash verification); `choose_requestable_piece` will skip it
    pub fn mark_in_flight(&mut self, idx: ValidPieceIndex) {
        self.in_flight[idx.get_usize()] = true;
    }

    /// Clear the in-flight flag so the piece can be re-requested (e.g. after hash failure or peer disconnect freeing chunks)
    pub fn clear_in_flight(&mut self, idx: ValidPieceIndex) {
        self.in_flight[idx.get_usize()] = false;
    }

    pub fn bitfield(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.lengths.piece_bitfield_bytes()];
        for (i, have) in self.have_local.iter().enumerate() {
            if *have {
                let byte = i / 8;
                let bit = 7 - (i % 8);
                out[byte] |= 1 << bit;
            }
        }
        out
    }

    /// Increment the availability counter for every bit set in `bitfield`
    pub fn add_peer_bitfield(&mut self, bitfield: &[u8]) {
        self.sorted_dirty = true;
        let n = self.lengths.total_pieces() as usize;
        for (byte_idx, &b) in bitfield.iter().enumerate() {
            if b == 0 {
                continue;
            }
            for bit in 0..8 {
                if b & (1 << (7 - bit)) != 0 {
                    let i = byte_idx * 8 + bit;
                    // Ignore trailing padding bits past total_pieces
                    if i >= n {
                        break;
                    }
                    self.availability[i] = self.availability[i].saturating_add(1);
                }
            }
        }
    }

    /// Inverse of `add_peer_bitfield`, called when a peer disconnects
    pub fn remove_peer_bitfield(&mut self, bitfield: &[u8]) {
        self.sorted_dirty = true;
        let n = self.lengths.total_pieces() as usize;
        for (byte_idx, &b) in bitfield.iter().enumerate() {
            if b == 0 {
                continue;
            }
            for bit in 0..8 {
                if b & (1 << (7 - bit)) != 0 {
                    let i = byte_idx * 8 + bit;
                    // Ignore trailing padding bits past total_pieces
                    if i >= n {
                        break;
                    }
                    self.availability[i] = self.availability[i].saturating_sub(1);
                }
            }
        }
    }

    pub fn update_peer_have(&mut self, peer_bitfield: &mut [u8], idx: ValidPieceIndex) -> bool {
        let piece = idx.get_usize();
        let byte = piece / 8;
        let bit = 7 - (piece % 8);
        let Some(slot) = peer_bitfield.get_mut(byte) else {
            return false;
        };
        let mask = 1 << bit;
        if *slot & mask != 0 {
            return false;
        }

        *slot |= mask;
        self.availability[piece] = self.availability[piece].saturating_add(1);
        self.sorted_dirty = true;
        true
    }

    pub fn replace_peer_bitfield(&mut self, peer_bitfield: &mut [u8], replacement: &[u8]) {
        self.remove_peer_bitfield(peer_bitfield);
        peer_bitfield.fill(0);
        let copied = peer_bitfield.len().min(replacement.len());
        peer_bitfield[..copied].copy_from_slice(&replacement[..copied]);
        self.add_peer_bitfield(peer_bitfield);
    }

    pub fn note_peer_has(&mut self, idx: ValidPieceIndex) {
        let slot = &mut self.availability[idx.get_usize()];
        *slot = slot.saturating_add(1);
        self.sorted_dirty = true;
    }

    pub fn is_complete(&self) -> bool {
        self.have_local.iter().all(|b| *b)
    }

    /// Pick the rarest piece the peer has that we don't, breaking ties with the lowest index; returns None if nothing useful; used for interest checks (does NOT skip in-flight pieces); `peer_bitfield` is indexed like `bitfield()`: MSB-first within bytes
    pub fn choose_piece(&mut self, peer_bitfield: &[u8]) -> Option<ValidPieceIndex> {
        self.choose_impl(peer_bitfield, false, 0, None)
    }

    /// Like [`choose_piece`] but skips pieces whose index is in `exclude` and uses `hint` to break ties among equally-rare pieces, so peers spread across the candidate bucket instead of all picking the same stalled piece first in endgame
    pub fn choose_piece_excluding(
        &mut self,
        peer_bitfield: &[u8],
        hint: u32,
        exclude: &std::collections::HashSet<u32>,
    ) -> Option<ValidPieceIndex> {
        self.choose_impl(peer_bitfield, false, hint, Some(exclude))
    }

    /// Return requestable pieces in rarest-first order from one bitfield scan; `hint` rotates equal-availability buckets to spread first choice
    pub fn choose_requestable_pieces(
        &mut self,
        peer_bitfield: &[u8],
        hint: u32,
    ) -> Vec<ValidPieceIndex> {
        self.choose_many_impl(peer_bitfield, true, hint)
    }

    pub fn choose_missing_pieces(&self) -> Vec<ValidPieceIndex> {
        let mut pieces = Vec::new();
        for index in 0..self.lengths.total_pieces() {
            let idx = index as usize;
            if self.have_local[idx] || self.in_flight[idx] {
                continue;
            }
            if let Ok(vpi) = self.lengths.validate_piece(index) {
                pieces.push(vpi);
            }
        }
        pieces.sort_by_key(|vpi| (self.availability[vpi.get_usize()], vpi.get()));
        pieces
    }

    /// Return useful pieces, including in-flight pieces for endgame duplication
    pub fn choose_pieces(&mut self, peer_bitfield: &[u8], hint: u32) -> Vec<ValidPieceIndex> {
        self.choose_many_impl(peer_bitfield, false, hint)
    }

    fn choose_impl(
        &mut self,
        peer_bitfield: &[u8],
        skip_in_flight: bool,
        hint: u32,
        exclude: Option<&std::collections::HashSet<u32>>,
    ) -> Option<ValidPieceIndex> {
        self.scratch.clear();
        let mut rarest: Option<u32> = None;
        let n = self.lengths.total_pieces() as usize;
        for i in 0..n {
            let byte = i / 8;
            let bit = 7 - (i % 8);
            if peer_bitfield.get(byte).is_none_or(|b| b & (1 << bit) == 0) {
                continue;
            }
            if self.have_local[i] {
                continue;
            }
            if skip_in_flight && self.in_flight[i] {
                continue;
            }
            if exclude.is_some_and(|e| e.contains(&(i as u32))) {
                continue;
            }
            let avail = self.availability[i];
            if avail == 0 {
                continue;
            }
            match rarest {
                None => {
                    rarest = Some(avail);
                    self.scratch.clear();
                    self.scratch
                        .push(self.lengths.validate_piece(i as u32).unwrap());
                }
                Some(r) if avail < r => {
                    rarest = Some(avail);
                    self.scratch.clear();
                    self.scratch
                        .push(self.lengths.validate_piece(i as u32).unwrap());
                }
                Some(r) if avail == r => {
                    self.scratch
                        .push(self.lengths.validate_piece(i as u32).unwrap());
                }
                _ => {}
            }
        }
        if self.scratch.is_empty() {
            return None;
        }
        // Distribute piece selection across peers to maximize parallelism
        let idx = hint as usize % self.scratch.len();
        Some(self.scratch[idx])
    }

    fn rebuild_sorted(&mut self) {
        self.sorted.clear();
        let n = self.lengths.total_pieces() as usize;
        for i in 0..n {
            if self.have_local[i] || self.availability[i] == 0 {
                continue;
            }
            if let Ok(vpi) = self.lengths.validate_piece(i as u32) {
                self.sorted.push(vpi);
            }
        }
        let availability = &self.availability;
        self.sorted
            .sort_by_key(|idx| (availability[idx.get_usize()], idx.get()));
        self.sorted_dirty = false;
    }

    fn choose_many_impl(
        &mut self,
        peer_bitfield: &[u8],
        skip_in_flight: bool,
        hint: u32,
    ) -> Vec<ValidPieceIndex> {
        if self.sorted_dirty {
            self.rebuild_sorted();
        }
        self.scratch.clear();
        for &vpi in &self.sorted {
            let i = vpi.get_usize();
            let byte = i / 8;
            let bit = 7 - (i % 8);
            if peer_bitfield.get(byte).is_none_or(|b| b & (1 << bit) == 0) {
                continue;
            }
            if skip_in_flight && self.in_flight[i] {
                continue;
            }
            self.scratch.push(vpi);
        }
        let availability = &self.availability;
        let mut start = 0;
        while start < self.scratch.len() {
            let avail = availability[self.scratch[start].get_usize()];
            let mut end = start + 1;
            while end < self.scratch.len() && availability[self.scratch[end].get_usize()] == avail {
                end += 1;
            }
            let len = end - start;
            self.scratch[start..end].rotate_left(hint as usize % len);
            start = end;
        }
        self.scratch.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lengths(pieces: u32) -> Lengths {
        Lengths::new((pieces as u64) * 1024, 1024).unwrap()
    }

    #[test]
    fn bitfield_round_trip() {
        let mut t = PieceTracker::new(lengths(9));
        t.set_local(t.lengths.validate_piece(0).unwrap(), true);
        t.set_local(t.lengths.validate_piece(8).unwrap(), true);
        let bf = t.bitfield();
        assert_eq!(bf.len(), 2);
        assert_eq!(bf[0], 0b1000_0000);
        assert_eq!(bf[1], 0b1000_0000);
    }

    #[test]
    fn repeated_have_updates_availability_once() {
        let mut tracker = PieceTracker::new(lengths(4));
        let mut peer = vec![0u8; 1];
        let piece = tracker.lengths.validate_piece(1).unwrap();

        assert!(tracker.update_peer_have(&mut peer, piece));
        assert!(!tracker.update_peer_have(&mut peer, piece));
        assert_eq!(peer, vec![0b0100_0000]);
        assert_eq!(tracker.availability, vec![0, 1, 0, 0]);

        tracker.remove_peer_bitfield(&peer);
        assert_eq!(tracker.availability, vec![0, 0, 0, 0]);
    }

    #[test]
    fn replacing_bitfield_removes_old_counts_and_clears_short_tail() {
        let mut tracker = PieceTracker::new(lengths(10));
        let mut peer = vec![0u8; 2];

        tracker.replace_peer_bitfield(&mut peer, &[0b1000_0000, 0b1000_0000]);
        assert_eq!(tracker.availability[0], 1);
        assert_eq!(tracker.availability[8], 1);

        tracker.replace_peer_bitfield(&mut peer, &[0b0100_0000]);
        assert_eq!(peer, vec![0b0100_0000, 0]);
        assert_eq!(tracker.availability[0], 0);
        assert_eq!(tracker.availability[1], 1);
        assert_eq!(tracker.availability[8], 0);

        // Repeating the same replacement remains one peer of availability
        tracker.replace_peer_bitfield(&mut peer, &[0b0100_0000]);
        assert_eq!(tracker.availability[1], 1);
    }

    #[test]
    fn rarest_first_choice() {
        let mut t = PieceTracker::new(lengths(4));
        // Peer advertises all four pieces
        t.add_peer_bitfield(&[0b1111_0000]);
        // Another peer has piece 0 and 1 only
        t.add_peer_bitfield(&[0b1100_0000]);
        // So availability: [2, 2, 1, 1]. We should pick piece 2 (lowest rarest)
        let chosen = t.choose_piece(&[0b1111_0000]).unwrap();
        assert_eq!(chosen.get(), 2);

        // After we own piece 2, piece 3 should win
        t.set_local(chosen, true);
        let chosen = t.choose_piece(&[0b1111_0000]).unwrap();
        assert_eq!(chosen.get(), 3);
    }

    #[test]
    fn batched_requestable_pieces_preserve_rarest_hint_order() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        t.add_peer_bitfield(&[0b0011_0000]);
        // Availability is [1, 1, 2, 2]; hint rotates each equal bucket
        let chosen: Vec<u32> = t
            .choose_requestable_pieces(&[0b1111_0000], 1)
            .into_iter()
            .map(|p| p.get())
            .collect();

        assert_eq!(chosen, vec![1, 0, 3, 2]);
    }

    #[test]
    fn batched_endgame_pieces_include_in_flight() {
        let mut t = PieceTracker::new(lengths(2));
        t.add_peer_bitfield(&[0b1100_0000]);
        let first = t.lengths.validate_piece(0).unwrap();
        t.mark_in_flight(first);

        let requestable: Vec<u32> = t
            .choose_requestable_pieces(&[0b1100_0000], 0)
            .into_iter()
            .map(|p| p.get())
            .collect();
        let endgame: Vec<u32> = t
            .choose_pieces(&[0b1100_0000], 0)
            .into_iter()
            .map(|p| p.get())
            .collect();

        assert_eq!(requestable, vec![1]);
        assert_eq!(endgame, vec![0, 1]);
    }

    #[test]
    fn peer_with_nothing_useful() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        // Peer advertises piece 0 only, which we've already got
        t.set_local(t.lengths.validate_piece(0).unwrap(), true);
        assert!(t.choose_piece(&[0b1000_0000]).is_none());
    }

    #[test]
    fn completion_tracking() {
        let mut t = PieceTracker::new(lengths(3));
        assert!(!t.is_complete());
        for i in 0..3 {
            t.set_local(t.lengths.validate_piece(i).unwrap(), true);
        }
        assert!(t.is_complete());
    }

    #[test]
    fn lazy_sorted_candidates_track_mutations() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        t.add_peer_bitfield(&[0b1100_0000]);
        let order: Vec<u32> = t
            .choose_requestable_pieces(&[0b1111_0000], 0)
            .into_iter()
            .map(|p| p.get())
            .collect();
        assert_eq!(order, vec![2, 3, 0, 1]);
        t.note_peer_has(t.lengths.validate_piece(2).unwrap());
        let order: Vec<u32> = t
            .choose_requestable_pieces(&[0b1111_0000], 0)
            .into_iter()
            .map(|p| p.get())
            .collect();
        assert_eq!(order, vec![3, 0, 1, 2]);
        t.set_local(t.lengths.validate_piece(3).unwrap(), true);
        let order: Vec<u32> = t
            .choose_requestable_pieces(&[0b1111_0000], 0)
            .into_iter()
            .map(|p| p.get())
            .collect();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn webseed_selection_includes_zero_availability_pieces() {
        let mut t = PieceTracker::new(lengths(3));
        t.add_peer_bitfield(&[0b1000_0000]);
        t.mark_in_flight(t.lengths.validate_piece(1).unwrap());

        let missing: Vec<u32> = t
            .choose_missing_pieces()
            .into_iter()
            .map(|piece| piece.get())
            .collect();
            
        assert_eq!(missing, vec![2, 0]);
    }
}
