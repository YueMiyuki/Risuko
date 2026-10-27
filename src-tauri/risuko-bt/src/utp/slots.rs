use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Default)]
struct Counts {
    total: usize,
    per_ip: HashMap<IpAddr, usize>,
}

pub(crate) struct IpSlots {
    max_total: usize,
    max_per_ip: usize,
    counts: Mutex<Counts>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Denied {
    Total,
    PerIp,
}

pub(crate) struct IpSlot {
    slots: Arc<IpSlots>,
    ip: IpAddr,
}

impl IpSlots {
    pub(crate) fn new(max_total: usize, max_per_ip: usize) -> Arc<Self> {
        Arc::new(Self {
            max_total,
            max_per_ip,
            counts: Mutex::new(Counts::default()),
        })
    }

    pub(crate) fn try_acquire(self: &Arc<Self>, ip: IpAddr) -> Option<IpSlot> {
        self.acquire(ip).ok()
    }

    pub(crate) fn acquire(self: &Arc<Self>, ip: IpAddr) -> Result<IpSlot, Denied> {
        let ip = crate::net::host_prefix(ip.to_canonical());
        let mut counts = self.counts.lock();
        let used = counts.per_ip.get(&ip).copied().unwrap_or(0);
        if used >= self.max_per_ip {
            return Err(Denied::PerIp);
        }
        if counts.total >= self.max_total {
            return Err(Denied::Total);
        }
        counts.per_ip.insert(ip, used + 1);
        counts.total += 1;
        Ok(IpSlot {
            slots: self.clone(),
            ip,
        })
    }

    #[cfg(test)]
    pub(crate) fn in_use(&self) -> usize {
        self.counts.lock().total
    }
}

impl Drop for IpSlot {
    fn drop(&mut self) {
        let mut counts = self.slots.counts.lock();
        counts.total = counts.total.saturating_sub(1);
        if let Some(used) = counts.per_ip.get_mut(&self.ip) {
            *used = used.saturating_sub(1);
            if *used == 0 {
                counts.per_ip.remove(&self.ip);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([192, 0, 2, last])
    }

    #[test]
    fn one_address_cannot_use_more_than_its_share() {
        let slots = IpSlots::new(10, 2);
        let a1 = slots.try_acquire(ip(1)).unwrap();
        let _a2 = slots.try_acquire(ip(1)).unwrap();
        assert!(slots.try_acquire(ip(1)).is_none());
        assert!(slots.try_acquire(ip(2)).is_some());
        drop(a1);
        assert!(slots.try_acquire(ip(1)).is_some());
    }

    #[test]
    fn the_global_limit_applies_across_addresses() {
        let slots = IpSlots::new(3, 3);
        let held: Vec<_> = (1..=3).map(|i| slots.try_acquire(ip(i)).unwrap()).collect();
        assert!(slots.try_acquire(ip(4)).is_none());
        assert_eq!(slots.in_use(), 3);
        drop(held);
        assert_eq!(slots.in_use(), 0);
        assert!(slots.counts.lock().per_ip.is_empty());
        assert!(slots.try_acquire(ip(4)).is_some());
    }

    #[test]
    fn addresses_in_one_ipv6_prefix_share_a_slot_count() {
        let slots = IpSlots::new(10, 2);
        let in_prefix = |tail: &str| -> IpAddr { format!("2001:db8:1:2:{tail}").parse().unwrap() };
        let first = slots.try_acquire(in_prefix("1:2:3:4")).unwrap();
        let _second = slots.try_acquire(in_prefix("ffff:ffff:ffff:ffff")).unwrap();
        assert!(slots.try_acquire(in_prefix("0:0:0:1")).is_none());
        assert!(slots
            .try_acquire("2001:db8:1:3::1".parse().unwrap())
            .is_some());
        drop(first);
        assert!(slots.try_acquire(in_prefix("0:0:0:1")).is_some());
        assert_eq!(slots.in_use(), 1);
    }

    #[test]
    fn a_refusal_names_the_limit_that_was_hit() {
        let slots = IpSlots::new(2, 1);
        let _a = slots.acquire(ip(1)).unwrap();
        assert_eq!(slots.acquire(ip(1)).err(), Some(Denied::PerIp));
        let _b = slots.acquire(ip(2)).unwrap();
        assert_eq!(slots.acquire(ip(3)).err(), Some(Denied::Total));
        assert_eq!(slots.acquire(ip(1)).err(), Some(Denied::PerIp));
    }

    #[test]
    fn mapped_and_plain_forms_of_an_address_share_a_slot_count() {
        let slots = IpSlots::new(10, 1);
        let _held = slots.try_acquire("1.2.3.4".parse().unwrap()).unwrap();
        assert!(slots
            .try_acquire("::ffff:1.2.3.4".parse().unwrap())
            .is_none());
    }
}
