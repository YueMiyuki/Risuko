use super::super::core::lengths::{Lengths, ValidPieceIndex};

pub struct PieceTracker {
    lengths: Lengths,
    have_local: Vec<bool>,
    wanted: Vec<bool>,
    left: u64,
    in_flight: Vec<bool>,
    availability: Vec<u32>,
    requestable: usize,
    sorted: Vec<ValidPieceIndex>,
    rebuild: bool,
    resort: bool,
    dead_prefix: usize,
    resort_deferred: u32,
}

const RESORT_EVERY_TICK_PIECES: usize = 65_536;
const MAX_RESORT_DEFERRAL: u32 = 20;

impl PieceTracker {
    pub fn new(lengths: Lengths) -> Self {
        let n = lengths.total_pieces() as usize;
        Self {
            lengths,
            have_local: vec![false; n],
            wanted: vec![true; n],
            left: lengths.total_length(),
            in_flight: vec![false; n],
            availability: vec![0; n],
            requestable: n,
            sorted: Vec::new(),
            rebuild: true,
            resort: false,
            dead_prefix: 0,
            resort_deferred: 0,
        }
    }

    pub fn lengths(&self) -> &Lengths {
        &self.lengths
    }

    fn counts_requestable(&self, i: usize) -> bool {
        self.wanted[i] && !self.have_local[i] && !self.in_flight[i]
    }

    fn settle(&mut self, i: usize, before: bool) {
        match (before, self.counts_requestable(i)) {
            (false, true) => self.requestable += 1,
            (true, false) => self.requestable -= 1,
            _ => {}
        }
    }

    fn recount_requestable(&mut self) {
        self.requestable = (0..self.have_local.len())
            .filter(|&i| self.counts_requestable(i))
            .count();
    }

    pub fn set_local(&mut self, idx: ValidPieceIndex, have: bool) {
        let i = idx.get_usize();
        let before = self.counts_requestable(i);
        if self.have_local[i] != have && self.wanted[i] {
            let len = self.lengths.piece_length_of(idx) as u64;
            if have {
                self.left -= len;
            } else {
                self.left += len;
                self.rebuild = true;
                self.dead_prefix = 0;
            }
        }
        self.have_local[i] = have;
        if have {
            self.in_flight[i] = false;
        }
        self.resort = true;
        self.settle(i, before);
    }

    pub fn has_local(&self, idx: ValidPieceIndex) -> bool {
        self.have_local[idx.get_usize()]
    }

    pub fn set_wanted(&mut self, wanted: Vec<bool>) {
        if wanted.len() == self.wanted.len() {
            self.wanted = wanted;
            self.rebuild = true;
            self.dead_prefix = 0;
            self.left = self.scan_bytes_left();
            self.recount_requestable();
        }
    }

    pub fn is_wanted(&self, idx: ValidPieceIndex) -> bool {
        self.wanted[idx.get_usize()]
    }

    pub fn is_useful(&self, idx: ValidPieceIndex) -> bool {
        let i = idx.get_usize();
        self.wanted[i] && !self.have_local[i]
    }

    pub fn is_requestable(&self, idx: ValidPieceIndex) -> bool {
        let i = idx.get_usize();
        self.wanted[i] && !self.have_local[i] && !self.in_flight[i]
    }

    pub fn requestable_remaining(&self) -> usize {
        self.requestable
    }

    pub fn bytes_left(&self) -> u64 {
        self.left
    }

    pub fn bytes_of(lengths: &Lengths, include: impl Fn(usize) -> bool) -> u64 {
        (0..lengths.total_pieces())
            .filter(|&i| include(i as usize))
            .filter_map(|i| lengths.validate_piece(i).ok())
            .map(|vpi| lengths.piece_length_of(vpi) as u64)
            .sum()
    }

    fn scan_bytes_left(&self) -> u64 {
        Self::bytes_of(&self.lengths, |i| self.wanted[i] && !self.have_local[i])
    }

    pub fn mark_in_flight(&mut self, idx: ValidPieceIndex) {
        let i = idx.get_usize();
        let before = self.counts_requestable(i);
        self.in_flight[i] = true;
        self.settle(i, before);
    }

    pub fn clear_in_flight(&mut self, idx: ValidPieceIndex) {
        let i = idx.get_usize();
        let before = self.counts_requestable(i);
        if self.in_flight[i] {
            self.dead_prefix = 0;
        }
        self.in_flight[i] = false;
        self.settle(i, before);
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

    fn bump_availability(&mut self, i: usize, up: bool) {
        let slot = &mut self.availability[i];
        *slot = if up {
            slot.saturating_add(1)
        } else {
            slot.saturating_sub(1)
        };
    }

    fn for_each_set_bit(&mut self, bitfield: &[u8], up: bool) {
        self.resort = true;
        let n = self.lengths.total_pieces() as usize;
        for (byte_idx, &b) in bitfield.iter().enumerate() {
            if b == 0 {
                continue;
            }
            for bit in 0..8 {
                if b & (1 << (7 - bit)) != 0 {
                    let i = byte_idx * 8 + bit;
                    if i >= n {
                        break;
                    }
                    self.bump_availability(i, up);
                }
            }
        }
    }

    pub fn add_peer_bitfield(&mut self, bitfield: &[u8]) {
        self.for_each_set_bit(bitfield, true);
    }

    pub fn remove_peer_bitfield(&mut self, bitfield: &[u8]) {
        self.for_each_set_bit(bitfield, false);
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
        self.bump_availability(piece, true);
        if self.wanted[piece] && !self.have_local[piece] {
            self.resort = true;
        }
        true
    }

    pub fn replace_peer_bitfield(&mut self, peer_bitfield: &mut [u8], replacement: &[u8]) {
        self.remove_peer_bitfield(peer_bitfield);
        peer_bitfield.fill(0);
        let copied = peer_bitfield.len().min(replacement.len());
        peer_bitfield[..copied].copy_from_slice(&replacement[..copied]);
        self.add_peer_bitfield(peer_bitfield);
    }

    pub fn is_complete(&self) -> bool {
        self.left == 0
    }

    pub fn any_useful(&self, peer_bitfield: &[u8]) -> bool {
        let n = self.lengths.total_pieces() as usize;
        for (byte_idx, &b) in peer_bitfield.iter().enumerate() {
            if b == 0 {
                continue;
            }
            for bit in 0..8 {
                let i = byte_idx * 8 + bit;
                if i >= n {
                    return false;
                }
                if b & (1 << (7 - bit)) != 0 && self.wanted[i] && !self.have_local[i] {
                    return true;
                }
            }
        }
        false
    }

    pub fn refresh_order(&mut self) {
        if self.rebuild {
            self.rebuild_sorted();
        } else if self.resort {
            let wait = (self.sorted.len() / RESORT_EVERY_TICK_PIECES) as u32;
            if self.resort_deferred < wait.min(MAX_RESORT_DEFERRAL) {
                self.resort_deferred += 1;
                return;
            }
            let (have_local, wanted) = (&self.have_local, &self.wanted);
            self.sorted
                .retain(|idx| wanted[idx.get_usize()] && !have_local[idx.get_usize()]);
            self.sort_by_rarity();
        }
    }

    fn rebuild_sorted(&mut self) {
        self.sorted.clear();
        let n = self.lengths.total_pieces() as usize;
        for i in 0..n {
            if self.have_local[i] || !self.wanted[i] {
                continue;
            }
            if let Ok(vpi) = self.lengths.validate_piece(i as u32) {
                self.sorted.push(vpi);
            }
        }
        self.rebuild = false;
        self.sort_by_rarity();
    }

    fn sort_by_rarity(&mut self) {
        let availability = &self.availability;
        self.sorted
            .sort_unstable_by_key(|idx| (availability[idx.get_usize()], idx.get()));
        self.resort = false;
        self.resort_deferred = 0;
        self.dead_prefix = 0;
    }

    fn peer_has(peer_bitfield: &[u8], i: usize) -> bool {
        peer_bitfield
            .get(i / 8)
            .is_some_and(|b| b & (1 << (7 - (i % 8))) != 0)
    }

    pub fn next_requestable(
        &mut self,
        peer_bitfield: &[u8],
        cursor: &mut usize,
    ) -> Option<ValidPieceIndex> {
        if self.rebuild {
            self.rebuild_sorted();
        }
        *cursor = (*cursor).max(self.dead_prefix);
        while let Some(&vpi) = self.sorted.get(*cursor) {
            let i = vpi.get_usize();
            let requestable = self.wanted[i] && !self.have_local[i] && !self.in_flight[i];
            if !requestable && *cursor == self.dead_prefix {
                self.dead_prefix += 1;
            }
            *cursor += 1;
            if requestable && Self::peer_has(peer_bitfield, i) {
                return Some(vpi);
            }
        }
        None
    }

    pub fn choose_missing_pieces(&self) -> Vec<ValidPieceIndex> {
        (0..self.lengths.total_pieces())
            .filter(|&index| {
                let idx = index as usize;
                !self.have_local[idx] && !self.in_flight[idx] && self.wanted[idx]
            })
            .filter_map(|index| self.lengths.validate_piece(index).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lengths(pieces: u32) -> Lengths {
        Lengths::new((pieces as u64) * 1024, 1024).unwrap()
    }

    fn scan(t: &mut PieceTracker, peer: &[u8]) -> Vec<u32> {
        let mut cursor = 0;
        let mut out = Vec::new();
        while let Some(p) = t.next_requestable(peer, &mut cursor) {
            out.push(p.get());
        }
        out
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

        tracker.replace_peer_bitfield(&mut peer, &[0b0100_0000]);
        assert_eq!(tracker.availability[1], 1);
    }

    #[test]
    fn rarest_first_order() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        t.add_peer_bitfield(&[0b1100_0000]);
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![2, 3, 0, 1]);

        t.set_local(t.lengths.validate_piece(2).unwrap(), true);
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![3, 0, 1]);
    }

    #[test]
    fn scan_skips_pieces_the_peer_lacks_and_in_flight_pieces() {
        let mut t = PieceTracker::new(lengths(2));
        t.add_peer_bitfield(&[0b1100_0000]);
        let first = t.lengths.validate_piece(0).unwrap();
        t.mark_in_flight(first);

        assert_eq!(scan(&mut t, &[0b1100_0000]), vec![1]);
        assert_eq!(scan(&mut t, &[0b1000_0000]), Vec::<u32>::new());
        assert_eq!(t.dead_prefix, 1);
        t.clear_in_flight(first);
        assert_eq!(scan(&mut t, &[0b1100_0000]), vec![0, 1]);
    }

    #[test]
    fn availability_changes_reorder_after_refresh() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        t.add_peer_bitfield(&[0b1100_0000]);
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![2, 3, 0, 1]);
        let mut extra = vec![0u8];
        t.update_peer_have(&mut extra, t.lengths.validate_piece(2).unwrap());
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![2, 3, 0, 1]);
        t.refresh_order();
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![3, 0, 1, 2]);
        t.set_local(t.lengths.validate_piece(3).unwrap(), true);
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![0, 1, 2]);
        t.refresh_order();
        assert_eq!(t.sorted.len(), 3);
    }

    #[test]
    fn peer_with_nothing_useful() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        t.set_local(t.lengths.validate_piece(0).unwrap(), true);
        assert!(!t.any_useful(&[0b1000_0000]));
        assert!(t.any_useful(&[0b0100_0000]));
        assert!(t.is_useful(t.lengths.validate_piece(1).unwrap()));
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
    fn requestable_count_tracks_every_transition() {
        let mut t = PieceTracker::new(lengths(4));
        let p = |t: &PieceTracker, i| t.lengths.validate_piece(i).unwrap();
        assert_eq!(t.requestable_remaining(), 4);
        t.mark_in_flight(p(&t, 0));
        t.mark_in_flight(p(&t, 0));
        assert_eq!(t.requestable_remaining(), 3);
        t.set_local(p(&t, 1), true);
        assert_eq!(t.requestable_remaining(), 2);
        t.clear_in_flight(p(&t, 0));
        assert_eq!(t.requestable_remaining(), 3);
        t.set_wanted(vec![false, true, true, true]);
        assert_eq!(t.requestable_remaining(), 2);
        t.mark_in_flight(p(&t, 2));
        t.mark_in_flight(p(&t, 3));
        assert_eq!(t.requestable_remaining(), 0);
        t.set_local(p(&t, 2), true);
        t.clear_in_flight(p(&t, 2));
        assert_eq!(t.requestable_remaining(), 0);
        let recount = (0..4).filter(|&i| t.counts_requestable(i)).count();
        assert_eq!(recount, t.requestable_remaining());
    }

    #[test]
    fn unwanted_pieces_are_never_picked_or_required() {
        let mut t = PieceTracker::new(lengths(4));
        t.add_peer_bitfield(&[0b1111_0000]);
        t.set_wanted(vec![false, true, false, true]);
        assert_eq!(scan(&mut t, &[0b1111_0000]), vec![1, 3]);
        assert_eq!(t.choose_missing_pieces().len(), 2);
        assert_eq!(t.bytes_left(), 2 * 1024);
        t.set_local(t.lengths.validate_piece(1).unwrap(), true);
        t.set_local(t.lengths.validate_piece(3).unwrap(), true);
        assert!(t.is_complete());
        assert!(!t.any_useful(&[0b1111_0000]));
        assert_eq!(t.bytes_left(), 0);
    }

    #[test]
    fn running_bytes_left_matches_full_scan() {
        let mut t = PieceTracker::new(Lengths::new(4 * 1024 + 100, 1024).unwrap());
        let piece = |t: &PieceTracker, i| t.lengths.validate_piece(i).unwrap();
        assert_eq!(t.bytes_left(), 4 * 1024 + 100);
        t.set_local(piece(&t, 4), true);
        t.set_local(piece(&t, 4), true);
        assert_eq!(t.bytes_left(), 4 * 1024);
        t.set_wanted(vec![true, false, true, false, true]);
        assert_eq!(t.bytes_left(), 2 * 1024);
        t.set_local(piece(&t, 1), true);
        assert_eq!(t.bytes_left(), 2 * 1024);
        t.set_local(piece(&t, 4), false);
        assert_eq!(t.bytes_left(), 2 * 1024 + 100);
        t.set_local(piece(&t, 0), true);
        t.set_wanted(vec![true; 5]);
        assert_eq!(t.bytes_left(), 2 * 1024 + 100);
        assert_eq!(t.bytes_left(), t.scan_bytes_left());
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

        assert_eq!(missing, vec![0, 2]);
    }

    #[test]
    fn huge_candidate_sets_defer_the_resort() {
        let n = (RESORT_EVERY_TICK_PIECES * 2) as u32;
        let mut t = PieceTracker::new(lengths(n));
        t.refresh_order();
        t.add_peer_bitfield(&[0b1000_0000]);
        t.refresh_order();
        t.refresh_order();
        assert!(t.resort);
        t.refresh_order();
        assert!(!t.resort);
        assert_eq!(t.sorted.last().map(|p| p.get()), Some(0));
        assert_eq!(t.sorted.first().map(|p| p.get()), Some(1));
    }

    #[test]
    fn lost_local_piece_rejoins_the_scan() {
        let mut t = PieceTracker::new(lengths(3));
        t.add_peer_bitfield(&[0b1110_0000]);
        let p1 = t.lengths.validate_piece(1).unwrap();
        t.set_local(p1, true);
        t.refresh_order();
        assert_eq!(scan(&mut t, &[0b1110_0000]), vec![0, 2]);
        t.set_local(p1, false);
        assert_eq!(scan(&mut t, &[0b1110_0000]), vec![0, 1, 2]);
    }
}
