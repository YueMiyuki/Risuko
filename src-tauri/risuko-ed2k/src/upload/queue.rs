use std::collections::HashMap;
use std::net::SocketAddrV4;

use tokio::time::{Duration, Instant};

use crate::types::ED2K_CHUNK_SIZE;

pub type SessionId = u64;

pub const SLOT_SESSION_BYTES: u64 = ED2K_CHUNK_SIZE + 20 * 1024;
pub const SLOT_SESSION_TIME: Duration = Duration::from_secs(60 * 60);
pub const WAITER_LIFETIME: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waiter {
    pub user: [u8; 16],
    pub file: [u8; 16],
    pub dial: Option<SocketAddrV4>,
    pub session: Option<SessionId>,
    pub seen: Instant,
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    session: Option<SessionId>,
    file: [u8; 16],
    started: Instant,
    sent: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Granted,
    Queued(u16),
    Full,
}

pub struct UploadQueue {
    max_slots: usize,
    max_waiting: usize,
    slots: HashMap<[u8; 16], Slot>,
    waiting: Vec<Waiter>,
}

impl UploadQueue {
    pub fn new(max_slots: usize, max_waiting: usize) -> Self {
        Self {
            max_slots: max_slots.max(1),
            max_waiting,
            slots: HashMap::new(),
            waiting: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn slots(&self) -> usize {
        self.slots.len()
    }

    #[cfg(test)]
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    pub fn holds_slot(&self, user: &[u8; 16], session: SessionId) -> bool {
        self.slots
            .get(user)
            .is_some_and(|slot| slot.session == Some(session))
    }

    pub fn request(&mut self, waiter: Waiter) -> Admission {
        if let Some(slot) = self.slots.get_mut(&waiter.user) {
            slot.session = waiter.session;
            slot.file = waiter.file;
            return Admission::Granted;
        }
        if let Some(existing) = self.waiting.iter_mut().find(|w| w.user == waiter.user) {
            *existing = waiter;
        } else if self.slots.len() < self.max_slots && !self.has_eligible_waiter() {
            self.slots.insert(
                waiter.user,
                Slot {
                    session: waiter.session,
                    file: waiter.file,
                    started: waiter.seen,
                    sent: 0,
                },
            );
            return Admission::Granted;
        } else if self.waiting.len() >= self.max_waiting {
            return Admission::Full;
        } else {
            self.waiting.push(waiter);
        }
        self.rank(&waiter.user)
            .map_or(Admission::Full, Admission::Queued)
    }

    pub fn rank(&self, user: &[u8; 16]) -> Option<u16> {
        self.waiting
            .iter()
            .position(|w| w.user == *user)
            .map(|i| u16::try_from(i + 1).unwrap_or(u16::MAX))
    }

    fn eligible(waiter: &Waiter) -> bool {
        waiter.session.is_some() || waiter.dial.is_some()
    }

    fn has_eligible_waiter(&self) -> bool {
        self.waiting.iter().any(Self::eligible)
    }

    pub fn next_grant(&mut self, now: Instant) -> Option<Waiter> {
        if self.slots.len() >= self.max_slots {
            return None;
        }
        let index = self.waiting.iter().position(Self::eligible)?;
        let waiter = self.waiting.remove(index);
        self.slots.insert(
            waiter.user,
            Slot {
                session: waiter.session,
                file: waiter.file,
                started: now,
                sent: 0,
            },
        );
        Some(waiter)
    }

    pub fn attach(&mut self, user: &[u8; 16], session: SessionId) -> bool {
        match self.slots.get_mut(user) {
            Some(slot) if slot.session.is_none() => {
                slot.session = Some(session);
                true
            }
            _ => false,
        }
    }

    pub fn detach(&mut self, user: &[u8; 16]) {
        if let Some(slot) = self.slots.get_mut(user) {
            slot.session = None;
        }
    }

    pub fn release(&mut self, user: &[u8; 16]) -> bool {
        self.slots.remove(user).is_some()
    }

    pub fn abandon_grant(&mut self, user: &[u8; 16]) -> bool {
        if !self
            .slots
            .get(user)
            .is_some_and(|slot| slot.session.is_none())
        {
            return false;
        }
        self.slots.remove(user);
        true
    }

    pub fn session_closed(&mut self, user: &[u8; 16], session: SessionId) -> bool {
        for waiter in &mut self.waiting {
            if waiter.user == *user && waiter.session == Some(session) {
                waiter.session = None;
            }
        }
        if self.holds_slot(user, session) {
            self.slots.remove(user);
            return true;
        }
        false
    }

    pub fn record_sent(&mut self, user: &[u8; 16], bytes: u64, now: Instant) -> bool {
        let Some(slot) = self.slots.get_mut(user) else {
            return false;
        };
        slot.sent += bytes;
        let due = slot.sent >= SLOT_SESSION_BYTES
            || now.saturating_duration_since(slot.started) >= SLOT_SESSION_TIME;
        due && self.has_eligible_waiter()
    }

    pub fn requeue(&mut self, waiter: Waiter) -> Option<u16> {
        self.slots.remove(&waiter.user);
        self.waiting.retain(|w| w.user != waiter.user);
        if self.waiting.len() >= self.max_waiting {
            return None;
        }
        self.waiting.push(waiter);
        self.rank(&waiter.user)
    }

    pub fn purge(&mut self, now: Instant) {
        self.waiting.retain(|w| {
            w.session.is_some() || now.saturating_duration_since(w.seen) < WAITER_LIFETIME
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn waiter(id: u8, session: Option<SessionId>, now: Instant) -> Waiter {
        Waiter {
            user: [id; 16],
            file: [9; 16],
            dial: None,
            session,
            seen: now,
        }
    }

    #[test]
    fn free_slots_are_granted_until_full_then_requests_queue_in_order() {
        let now = Instant::now();
        let mut q = UploadQueue::new(2, 10);
        assert_eq!(q.request(waiter(1, Some(1), now)), Admission::Granted);
        assert_eq!(q.request(waiter(2, Some(2), now)), Admission::Granted);
        assert_eq!(q.request(waiter(3, Some(3), now)), Admission::Queued(1));
        assert_eq!(q.request(waiter(4, Some(4), now)), Admission::Queued(2));
        assert_eq!(q.request(waiter(3, Some(5), now)), Admission::Queued(1));
        assert_eq!(q.request(waiter(1, Some(6), now)), Admission::Granted);
        assert!(q.holds_slot(&[1; 16], 6));
    }

    #[test]
    fn a_new_requester_does_not_jump_an_eligible_waiter() {
        let now = Instant::now();
        let mut q = UploadQueue::new(1, 10);
        assert_eq!(q.request(waiter(1, Some(1), now)), Admission::Granted);
        assert_eq!(q.request(waiter(2, Some(2), now)), Admission::Queued(1));
        assert!(q.release(&[1; 16]));
        assert_eq!(q.request(waiter(3, Some(3), now)), Admission::Queued(2));
        let grant = q.next_grant(now).unwrap();
        assert_eq!((grant.user, grant.session), ([2; 16], Some(2)));
        assert!(q.next_grant(now).is_none());
    }

    #[test]
    fn offline_waiters_without_a_dial_address_are_skipped_but_kept() {
        let now = Instant::now();
        let mut q = UploadQueue::new(1, 10);
        q.request(waiter(1, Some(1), now));
        q.request(waiter(2, Some(2), now));
        let mut dialable = waiter(3, Some(3), now);
        dialable.dial = Some(SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662));
        q.request(dialable);
        assert!(!q.session_closed(&[2; 16], 2));
        assert!(!q.session_closed(&[3; 16], 3));
        assert!(q.session_closed(&[1; 16], 1));
        let grant = q.next_grant(now).unwrap();
        assert_eq!((grant.user, grant.session), ([3; 16], None));
        assert_eq!(q.rank(&[2; 16]), Some(1));
        assert!(q.attach(&[3; 16], 7));
        assert!(q.holds_slot(&[3; 16], 7));
    }

    #[test]
    fn slots_rotate_after_a_part_only_when_someone_waits() {
        let now = Instant::now();
        let mut q = UploadQueue::new(1, 10);
        q.request(waiter(1, Some(1), now));
        assert!(!q.record_sent(&[1; 16], SLOT_SESSION_BYTES, now));
        q.request(waiter(2, Some(2), now));
        assert!(q.record_sent(&[1; 16], 1, now));
        assert_eq!(q.requeue(waiter(1, Some(1), now)), Some(2));
        assert_eq!(q.next_grant(now).map(|g| g.user), Some([2; 16]));
    }

    #[test]
    fn an_abandoned_grant_keeps_a_slot_the_user_reclaimed_inbound() {
        let now = Instant::now();
        let mut q = UploadQueue::new(1, 10);
        q.request(waiter(1, Some(1), now));
        q.detach(&[1; 16]);
        assert_eq!(q.request(waiter(1, Some(2), now)), Admission::Granted);
        assert!(!q.abandon_grant(&[1; 16]));
        assert!(q.holds_slot(&[1; 16], 2));
        q.detach(&[1; 16]);
        assert!(q.abandon_grant(&[1; 16]));
        assert_eq!(q.slots(), 0);
    }

    #[test]
    fn full_queue_and_purge() {
        let now = Instant::now();
        let mut q = UploadQueue::new(1, 1);
        q.request(waiter(1, Some(1), now));
        assert_eq!(q.request(waiter(2, Some(2), now)), Admission::Queued(1));
        assert_eq!(q.request(waiter(3, Some(3), now)), Admission::Full);
        q.session_closed(&[2; 16], 2);
        q.purge(now + WAITER_LIFETIME);
        assert_eq!(q.waiting(), 0);
        assert_eq!(q.request(waiter(3, Some(3), now)), Admission::Queued(1));
    }
}
