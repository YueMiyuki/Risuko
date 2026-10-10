use std::collections::{HashSet, VecDeque};
use std::net::IpAddr;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlocklistApplyResult {
    pub revision: u32,
    pub rule_count: u32,
    pub disconnected_peers: u32,
    pub removed_peers: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Prefix {
    V4 { network: u32, mask: u32 },
    V6 { network: u128, mask: u128 },
}

#[derive(Clone, Debug, Default)]
pub struct BlockList {
    exact: HashSet<IpAddr>,
    prefixes: Vec<Prefix>,
    v4_ranges: Vec<(u32, u32)>,
    v6_ranges: Vec<(u128, u128)>,
    banned: HashSet<IpAddr>,
    ban_order: VecDeque<IpAddr>,
    ban_revision: u32,
    revision: u32,
}

const MAX_BANNED: usize = 4096;

impl BlockList {
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.prefixes.is_empty() && self.banned.is_empty()
    }

    pub fn revision(&self) -> u32 {
        self.revision
    }

    pub fn ban_revision(&self) -> u32 {
        self.ban_revision
    }

    pub fn rule_count(&self) -> u32 {
        (self.exact.len() + self.prefixes.len()) as u32
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        if self.is_empty() {
            return false;
        }
        let canonical = canonicalize_ip(ip);
        if self.exact.contains(&canonical) || self.banned.contains(&canonical) {
            return true;
        }
        match ip {
            IpAddr::V4(addr) => range_contains(&self.v4_ranges, u32::from(addr)),
            IpAddr::V6(addr) => {
                if let Some(v4) = addr.to_ipv4_mapped() {
                    if range_contains(&self.v4_ranges, u32::from(v4)) {
                        return true;
                    }
                }
                range_contains(&self.v6_ranges, u128::from(addr))
            }
        }
    }

    pub fn ban(&mut self, ip: IpAddr) -> bool {
        let ip = canonicalize_ip(ip);
        if !self.banned.insert(ip) {
            return false;
        }
        self.ban_order.push_back(ip);
        if self.banned.len() > MAX_BANNED {
            if let Some(oldest) = self.ban_order.pop_front() {
                self.banned.remove(&oldest);
            }
        }
        self.ban_revision = self.ban_revision.wrapping_add(1);
        true
    }

    pub fn replace(&mut self, entries: &[String]) -> BlocklistApplyResult {
        self.replace_prepared(PreparedRules::parse(entries))
    }

    pub fn replace_prepared(&mut self, rules: PreparedRules) -> BlocklistApplyResult {
        self.exact = rules.exact;
        self.prefixes = rules.prefixes;
        self.v4_ranges = rules.v4_ranges;
        self.v6_ranges = rules.v6_ranges;
        self.revision = self.revision.wrapping_add(1);
        BlocklistApplyResult {
            revision: self.revision,
            rule_count: self.rule_count(),
            disconnected_peers: 0,
            removed_peers: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct PreparedRules {
    exact: HashSet<IpAddr>,
    prefixes: Vec<Prefix>,
    v4_ranges: Vec<(u32, u32)>,
    v6_ranges: Vec<(u128, u128)>,
}

impl PreparedRules {
    pub fn parse(entries: &[String]) -> Self {
        let mut rules = PreparedRules::default();
        for entry in entries {
            match parse_entry(entry) {
                Some(ParsedEntry::Exact(ip)) => {
                    rules.exact.insert(canonicalize_ip(ip));
                }
                Some(ParsedEntry::Prefix(prefix)) => rules.prefixes.push(prefix),
                None => {}
            }
        }
        let mut v4 = Vec::new();
        let mut v6 = Vec::new();
        for prefix in &rules.prefixes {
            match *prefix {
                Prefix::V4 { network, mask } => v4.push((network, network | !mask)),
                Prefix::V6 { network, mask } => v6.push((network, network | !mask)),
            }
        }
        rules.v4_ranges = merge_ranges(v4);
        rules.v6_ranges = merge_ranges(v6);
        rules
    }
}

fn merge_ranges<T: Copy + Ord>(mut ranges: Vec<(T, T)>) -> Vec<(T, T)> {
    ranges.sort_unstable();
    let mut out: Vec<(T, T)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match out.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => out.push((start, end)),
        }
    }
    out
}

fn range_contains<T: Copy + Ord>(ranges: &[(T, T)], value: T) -> bool {
    let idx = ranges.partition_point(|&(start, _)| start <= value);
    idx > 0 && value <= ranges[idx - 1].1
}

enum ParsedEntry {
    Exact(IpAddr),
    Prefix(Prefix),
}

fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        IpAddr::V4(_) => ip,
    }
}

fn parse_entry(raw: &str) -> Option<ParsedEntry> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some((addr, prefix_str)) = trimmed.split_once('/') {
        let prefix: u8 = prefix_str.parse().ok()?;
        let ip: IpAddr = addr.parse().ok()?;
        return Some(ParsedEntry::Prefix(parse_prefix(ip, prefix)?));
    }
    let ip: IpAddr = trimmed.parse().ok()?;
    Some(ParsedEntry::Exact(ip))
}

fn parse_prefix(ip: IpAddr, prefix: u8) -> Option<Prefix> {
    match ip {
        IpAddr::V4(addr) => {
            if prefix > 32 {
                return None;
            }
            let mask = ipv4_mask(prefix);
            Some(Prefix::V4 {
                network: u32::from(addr) & mask,
                mask,
            })
        }
        IpAddr::V6(addr) => {
            if prefix > 128 {
                return None;
            }
            let mask = ipv6_mask(prefix);
            Some(Prefix::V6 {
                network: u128::from(addr) & mask,
                mask,
            })
        }
    }
}

fn ipv4_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        !0u32 << (32 - prefix)
    }
}

fn ipv6_mask(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        !0u128 << (128 - prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_ipv4_and_mapped_v6_match() {
        let mut list = BlockList::default();
        list.replace(&["1.2.3.4".into()]);
        assert!(list.contains("1.2.3.4".parse().unwrap()));
        assert!(list.contains("::ffff:1.2.3.4".parse().unwrap()));
        assert!(!list.contains("1.2.3.5".parse().unwrap()));
        assert_eq!(list.rule_count(), 1);
        assert_eq!(list.revision(), 1);
    }

    #[test]
    fn ipv4_cidr_matches_hosts_in_range() {
        let mut list = BlockList::default();
        list.replace(&["10.0.0.0/8".into()]);
        assert!(list.contains("10.1.2.3".parse().unwrap()));
        assert!(!list.contains("11.0.0.1".parse().unwrap()));
    }

    #[test]
    fn ipv6_cidr_matches_prefix() {
        let mut list = BlockList::default();
        list.replace(&["2001:db8::/32".into()]);
        assert!(list.contains("2001:db8:1::1".parse().unwrap()));
        assert!(!list.contains("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn bans_survive_rule_replacement_and_match_mapped_addresses() {
        let mut list = BlockList::default();
        assert!(list.ban("192.0.2.7".parse().unwrap()));
        assert!(!list.ban("::ffff:192.0.2.7".parse().unwrap()));
        list.replace(&["198.51.100.0/24".to_string()]);
        assert!(list.contains("192.0.2.7".parse().unwrap()));
        assert!(list.contains("::ffff:192.0.2.7".parse().unwrap()));
        assert_eq!(list.rule_count(), 1);
    }

    #[test]
    fn ban_evicts_the_oldest_at_the_cap_and_bumps_the_ban_revision() {
        let mut list = BlockList::default();
        let ip = |n: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + n));
        for n in 0..MAX_BANNED as u32 {
            assert!(list.ban(ip(n)));
        }
        let before = list.ban_revision();
        assert!(!list.ban(ip(5)));
        assert_eq!(list.ban_revision(), before);
        assert!(list.ban(ip(MAX_BANNED as u32)));
        assert_eq!(list.ban_revision(), before.wrapping_add(1));
        assert!(!list.contains(ip(0)));
        assert!(list.contains(ip(1)));
        assert!(list.contains(ip(MAX_BANNED as u32)));
        assert_eq!(list.banned.len(), MAX_BANNED);
        assert_eq!(list.ban_order.len(), MAX_BANNED);
    }

    #[test]
    fn replace_is_full_swap_and_bumps_revision() {
        let mut list = BlockList::default();
        list.replace(&["1.1.1.1".into()]);
        list.replace(&["8.8.8.8/32".into(), "not-an-ip".into(), "".into()]);
        assert!(!list.contains("1.1.1.1".parse().unwrap()));
        assert!(list.contains("8.8.8.8".parse().unwrap()));
        assert_eq!(list.revision(), 2);
        assert_eq!(list.rule_count(), 1);
    }

    #[test]
    fn overlapping_and_nested_prefixes_match_via_merged_ranges() {
        let mut list = BlockList::default();
        list.replace(&[
            "10.0.0.0/8".into(),
            "10.5.0.0/16".into(),
            "192.168.1.0/24".into(),
            "192.168.1.128/25".into(),
            "2001:db8::/32".into(),
            "2001:db8:1::/48".into(),
        ]);
        assert!(list.contains("10.255.255.255".parse().unwrap()));
        assert!(list.contains("192.168.1.200".parse().unwrap()));
        assert!(!list.contains("192.168.2.1".parse().unwrap()));
        assert!(!list.contains("9.255.255.255".parse().unwrap()));
        assert!(list.contains("2001:db8:1::5".parse().unwrap()));
        assert!(!list.contains("2001:db7::1".parse().unwrap()));
        assert_eq!(list.rule_count(), 6);
    }

    #[test]
    fn empty_list_contains_nothing() {
        let list = BlockList::default();
        assert!(!list.contains("127.0.0.1".parse().unwrap()));
        assert!(list.is_empty());
    }

    #[test]
    fn ipv6_unspecified_cidr_does_not_block_ipv4_peers() {
        let mut list = BlockList::default();
        list.replace(&["::/0".into()]);
        assert!(list.contains("2001:db8::1".parse().unwrap()));
        assert!(!list.contains("1.2.3.4".parse().unwrap()));
        assert!(list.contains("::ffff:1.2.3.4".parse().unwrap()));
    }

    #[test]
    fn ipv4_cidr_still_matches_mapped_ipv6_peers() {
        let mut list = BlockList::default();
        list.replace(&["10.0.0.0/8".into()]);
        assert!(list.contains("10.1.2.3".parse().unwrap()));
        assert!(list.contains("::ffff:10.1.2.3".parse().unwrap()));
    }

    #[test]
    fn mapped_ipv6_cidr_keeps_ipv6_prefix_length() {
        let mut list = BlockList::default();
        list.replace(&["::ffff:10.1.2.0/120".into()]);
        assert!(list.contains("::ffff:10.1.2.1".parse().unwrap()));
        assert!(list.contains("::ffff:10.1.2.255".parse().unwrap()));
        assert!(!list.contains("::ffff:10.1.3.1".parse().unwrap()));
        assert!(!list.contains("10.1.2.1".parse().unwrap()));
        assert_eq!(list.rule_count(), 1);
    }
}
