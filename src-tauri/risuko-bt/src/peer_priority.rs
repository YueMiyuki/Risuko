use std::cmp::Reverse;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use super::core::crc32c::crc32c;
use super::external_ip::{ExternalAddresses, ExternalIp, Route};
use super::net::canonical_addr;

const V4_KEPT_FLOOR: usize = 2;
const V6_KEPT_FLOOR: usize = 6;
const HIDDEN_BITS: u8 = 0x55;

fn masked_pair_priority<const N: usize>(a: [u8; N], b: [u8; N], floor: usize) -> u32 {
    let shared = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    let kept = (shared + 1).max(floor).min(N);
    let mask = |mut bytes: [u8; N]| {
        for byte in &mut bytes[kept..] {
            *byte &= HIDDEN_BITS;
        }
        bytes
    };
    let (first, second) = {
        let (a, b) = (mask(a), mask(b));
        if a <= b {
            (a, b)
        } else {
            (b, a)
        }
    };
    let mut input = [0u8; 32];
    input[..N].copy_from_slice(&first);
    input[N..2 * N].copy_from_slice(&second);
    crc32c(&input[..2 * N])
}

fn mapped(ip: IpAddr) -> Ipv6Addr {
    match ip {
        IpAddr::V4(ip) => ip.to_ipv6_mapped(),
        IpAddr::V6(ip) => ip,
    }
}

pub(crate) fn peer_priority(ours: SocketAddr, theirs: SocketAddr) -> u32 {
    let (ours, theirs) = (canonical_addr(ours), canonical_addr(theirs));
    if ours.ip() == theirs.ip() {
        let (low, high) = if ours.port() <= theirs.port() {
            (ours.port(), theirs.port())
        } else {
            (theirs.port(), ours.port())
        };
        let mut input = [0u8; 4];
        input[..2].copy_from_slice(&low.to_be_bytes());
        input[2..].copy_from_slice(&high.to_be_bytes());
        return crc32c(&input);
    }
    match (ours.ip(), theirs.ip()) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            masked_pair_priority(a.octets(), b.octets(), V4_KEPT_FLOOR)
        }
        (a, b) => masked_pair_priority(mapped(a).octets(), mapped(b).octets(), V6_KEPT_FLOOR),
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PeerRanker {
    ours: ExternalAddresses,
    listen_port: u16,
}

impl PeerRanker {
    pub(crate) fn new(ours: ExternalAddresses, listen_port: u16) -> Self {
        Self { ours, listen_port }
    }

    pub(crate) fn observed(route: Route, listen_port: u16) -> Self {
        match route {
            Route::Direct => Self::new(ExternalIp::shared().addresses(), listen_port),
            Route::Proxied => Self::default(),
        }
    }

    pub(crate) fn priority(&self, theirs: SocketAddr) -> Option<u32> {
        let theirs = canonical_addr(theirs);
        let ours = match theirs.ip() {
            IpAddr::V4(_) => IpAddr::V4(self.ours.v4?),
            IpAddr::V6(_) => IpAddr::V6(self.ours.v6?),
        };
        Some(peer_priority(
            SocketAddr::new(ours, self.listen_port),
            theirs,
        ))
    }

    pub(crate) fn order_batch(&self, addrs: &mut [SocketAddr]) {
        if addrs.len() < 2 || self.ours == ExternalAddresses::default() {
            return;
        }
        let (slots, mut ranked): (Vec<usize>, Vec<(u32, SocketAddr)>) = addrs
            .iter()
            .enumerate()
            .filter_map(|(slot, &addr)| Some((slot, (self.priority(addr)?, addr))))
            .unzip();
        ranked.sort_by_key(|&(priority, _)| Reverse(priority));
        for (slot, (_, addr)) in slots.into_iter().zip(ranked) {
            addrs[slot] = addr;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(text: &str, port: u16) -> SocketAddr {
        SocketAddr::new(text.parse::<Ipv4Addr>().unwrap().into(), port)
    }

    fn v6(text: &str, port: u16) -> SocketAddr {
        SocketAddr::new(text.parse::<Ipv6Addr>().unwrap().into(), port)
    }

    fn elected(v4: Option<&str>, v6: Option<&str>) -> ExternalAddresses {
        ExternalAddresses {
            v4: v4.map(|text| text.parse().unwrap()),
            v6: v6.map(|text| text.parse().unwrap()),
        }
    }

    #[test]
    fn bep_40_example_across_different_slash_16_networks() {
        let priority = peer_priority(v4("123.213.32.10", 0), v4("98.76.54.32", 0));
        assert_eq!(priority, 0xec2d_7224);
    }

    #[test]
    fn bep_40_example_inside_one_slash_24_hashes_the_full_addresses() {
        let priority = peer_priority(v4("123.213.32.10", 0), v4("123.213.32.234", 0));
        assert_eq!(priority, 0x9956_8189);
    }

    #[test]
    fn priority_is_symmetric_for_v4_v6_and_equal_address_pairs() {
        let pairs = [
            (v4("123.213.32.10", 6881), v4("98.76.54.32", 51413)),
            (v4("123.213.32.10", 6881), v4("123.213.99.234", 51413)),
            (v4("123.213.32.10", 6881), v4("123.213.32.234", 51413)),
            (v4("123.213.32.10", 6881), v4("123.213.32.10", 51413)),
            (v6("2001:db8:1::1", 6881), v6("2001:db8:2::5", 51413)),
            (
                v6("2001:db8:1:100::1", 6881),
                v6("2001:db8:1:200::5", 51413),
            ),
            (v6("2001:db8:1:1::1", 6881), v6("2001:db8:1:1::2", 51413)),
            (v6("2001:db8:1::1", 6881), v6("2001:db8:1::1", 51413)),
            (v4("123.213.32.10", 6881), v6("2001:db8:1::1", 51413)),
        ];
        for (a, b) in pairs {
            assert_eq!(peer_priority(a, b), peer_priority(b, a), "{a} {b}");
        }
    }

    #[test]
    fn bits_outside_the_mask_do_not_change_the_priority() {
        let ours = v4("123.213.32.10", 0);
        let base = peer_priority(ours, v4("98.76.54.32", 0));
        assert_eq!(base, peer_priority(ours, v4("98.76.20.0", 0)));
        assert_eq!(base, peer_priority(ours, v4("98.76.190.170", 0)));
        assert_eq!(base, peer_priority(ours, v4("98.76.54.32", 9999)));
        assert_ne!(base, peer_priority(ours, v4("98.77.54.32", 0)));
        assert_ne!(base, peer_priority(ours, v4("98.76.54.33", 0)));
    }

    #[test]
    fn same_slash_16_keeps_the_third_octet_and_hides_the_low_fourth_octet_bits() {
        let ours = v4("123.213.32.10", 0);
        let base = peer_priority(ours, v4("123.213.45.0", 0));
        assert_eq!(base, peer_priority(ours, v4("123.213.45.170", 0)));
        assert_ne!(base, peer_priority(ours, v4("123.213.46.0", 0)));
        assert_ne!(base, peer_priority(ours, v4("123.213.45.1", 0)));
    }

    #[test]
    fn same_slash_24_keeps_every_bit_of_the_fourth_octet() {
        let ours = v4("123.213.32.10", 0);
        let base = peer_priority(ours, v4("123.213.32.234", 0));
        assert_ne!(base, peer_priority(ours, v4("123.213.32.235", 0)));
        assert_ne!(base, peer_priority(ours, v4("123.213.32.170", 0)));
    }

    #[test]
    fn equal_addresses_fall_back_to_the_sorted_port_pair() {
        let ip = "123.213.32.10";
        let expected = crc32c(&[0x1A, 0xE1, 0xC8, 0xD5]);
        assert_eq!(peer_priority(v4(ip, 6881), v4(ip, 51413)), expected);
        assert_eq!(peer_priority(v4(ip, 51413), v4(ip, 6881)), expected);
        assert_eq!(expected, 0x9f85_2e9f);
        assert_ne!(
            peer_priority(v4(ip, 6881), v4(ip, 51413)),
            peer_priority(v4(ip, 6881), v4(ip, 51414)),
        );
    }

    #[test]
    fn ipv6_masks_widen_one_byte_per_shared_prefix_step() {
        let ours = v6("2001:db8:1::1", 0);
        let diff_48 = peer_priority(ours, v6("2001:db8:2::5", 0));
        assert_eq!(diff_48, 0xfa54_7717);
        assert_eq!(
            peer_priority(v6("2001:db8:1:100::1", 0), v6("2001:db8:1:200::5", 0)),
            0xf8c4_1c11,
        );
        assert_eq!(
            peer_priority(v6("2001:db8:1:101::1", 0), v6("2001:db8:1:102::5", 0)),
            0x6377_f7f3,
        );
        assert_eq!(
            peer_priority(v6("2001:db8:1:1::1", 0), v6("2001:db8:1:1::ff", 0)),
            0x04ac_433d,
        );
    }

    #[test]
    fn ipv6_prefix_bits_below_the_kept_bytes_are_hidden() {
        let ours = v6("2001:db8:1::1", 0);
        assert_eq!(
            peer_priority(ours, v6("2001:db8:2::", 0)),
            peer_priority(ours, v6("2001:db8:2:aaaa:aaaa:aaaa:aaaa:aaaa", 0)),
        );
        assert_ne!(
            peer_priority(ours, v6("2001:db8:2::", 0)),
            peer_priority(ours, v6("2001:db8:3::", 0)),
        );
    }

    #[test]
    fn ipv4_mapped_addresses_rank_as_plain_ipv4() {
        let ours = v4("123.213.32.10", 6881);
        let theirs = v4("98.76.54.32", 6881);
        let mapped_theirs =
            SocketAddr::new(Ipv4Addr::new(98, 76, 54, 32).to_ipv6_mapped().into(), 6881);
        assert_eq!(
            peer_priority(ours, mapped_theirs),
            peer_priority(ours, theirs)
        );
        let ranker = PeerRanker::new(elected(Some("123.213.32.10"), None), 6881);
        assert_eq!(ranker.priority(mapped_theirs), ranker.priority(theirs));
    }

    #[test]
    fn ranker_uses_the_elected_address_of_the_candidate_family() {
        let ranker = PeerRanker::new(elected(Some("123.213.32.10"), Some("2001:db8:1::1")), 6881);
        assert_eq!(
            ranker.priority(v4("98.76.54.32", 51413)),
            Some(peer_priority(
                v4("123.213.32.10", 6881),
                v4("98.76.54.32", 51413)
            )),
        );
        assert_eq!(
            ranker.priority(v6("2001:db8:2::5", 51413)),
            Some(peer_priority(
                v6("2001:db8:1::1", 6881),
                v6("2001:db8:2::5", 51413)
            )),
        );
    }

    #[test]
    fn ranker_without_an_elected_address_for_the_family_has_no_opinion() {
        let only_v4 = PeerRanker::new(elected(Some("123.213.32.10"), None), 6881);
        assert!(only_v4.priority(v4("98.76.54.32", 6881)).is_some());
        assert_eq!(only_v4.priority(v6("2001:db8:2::5", 6881)), None);
        assert_eq!(
            PeerRanker::default().priority(v4("98.76.54.32", 6881)),
            None
        );
    }

    #[test]
    fn a_proxied_route_never_ranks_with_the_elected_address() {
        let ranker = PeerRanker::observed(Route::Proxied, 6881);
        assert_eq!(ranker.priority(v4("98.76.54.32", 6881)), None);
        assert_eq!(ranker.priority(v6("2001:db8:2::5", 6881)), None);
    }

    fn batch() -> Vec<SocketAddr> {
        (1..=40u8)
            .map(|n| v4(&format!("98.{n}.{}.{}", n.wrapping_mul(7), n), 6881))
            .collect()
    }

    #[test]
    fn batch_is_ordered_by_descending_priority() {
        let ranker = PeerRanker::new(elected(Some("123.213.32.10"), None), 6881);
        let mut addrs = batch();
        ranker.order_batch(&mut addrs);
        let priorities: Vec<u32> = addrs
            .iter()
            .map(|&addr| ranker.priority(addr).unwrap())
            .collect();
        assert!(priorities.windows(2).all(|pair| pair[0] >= pair[1]));
        let mut sorted = addrs.clone();
        let mut original = batch();
        sorted.sort();
        original.sort();
        assert_eq!(sorted, original);
        assert_ne!(addrs, batch());
    }

    #[test]
    fn batch_keeps_arrival_order_for_equal_priorities() {
        let ranker = PeerRanker::new(elected(Some("123.213.32.10"), None), 6881);
        let same_masked_a = v4("98.76.54.32", 1000);
        let same_masked_b = v4("98.76.54.32", 2000);
        let same_masked_c = v4("98.76.20.0", 3000);
        assert_eq!(
            ranker.priority(same_masked_a),
            ranker.priority(same_masked_c)
        );
        for arrival in [
            [same_masked_a, same_masked_b, same_masked_c],
            [same_masked_c, same_masked_b, same_masked_a],
        ] {
            let mut addrs = arrival;
            ranker.order_batch(&mut addrs);
            assert_eq!(addrs, arrival);
        }
    }

    #[test]
    fn batch_keeps_arrival_order_inside_each_of_many_interleaved_priority_groups() {
        const GROUPS: u8 = 12;
        const HIDDEN: [u8; 8] = [0x00, 0x02, 0x08, 0x20, 0x80, 0x0A, 0x22, 0xAA];
        let ranker = PeerRanker::new(elected(Some("123.213.32.10"), None), 6881);
        let mut arrival = Vec::new();
        for member in 0..HIDDEN.len() {
            for group in 0..GROUPS {
                let rotated = (group as usize + member * 5) % GROUPS as usize;
                let third = 0x14 | HIDDEN[member];
                let fourth = 0x41 | HIDDEN[(member + rotated) % HIDDEN.len()];
                let port = 1000 + arrival.len() as u16;
                arrival.push(v4(&format!("98.{}.{third}.{fourth}", 10 + rotated), port));
            }
        }
        let priority = |addr: &SocketAddr| ranker.priority(*addr).unwrap();
        let mut distinct: Vec<u32> = arrival.iter().map(priority).collect();
        distinct.sort_unstable_by(|a, b| b.cmp(a));
        distinct.dedup();
        assert_eq!(distinct.len(), GROUPS as usize);
        for tier in &distinct {
            let tied = arrival
                .iter()
                .filter(|addr| priority(addr) == *tier)
                .count();
            assert_eq!(tied, HIDDEN.len());
        }

        let mut ordered = arrival.clone();
        ranker.order_batch(&mut ordered);

        let expected: Vec<SocketAddr> = distinct
            .iter()
            .flat_map(|tier| arrival.iter().filter(|addr| priority(addr) == *tier))
            .copied()
            .collect();
        assert_eq!(ordered, expected);
    }

    #[test]
    fn batch_without_an_elected_address_is_left_untouched() {
        let mut addrs = batch();
        PeerRanker::default().order_batch(&mut addrs);
        assert_eq!(addrs, batch());
        let mut proxied = batch();
        PeerRanker::observed(Route::Proxied, 6881).order_batch(&mut proxied);
        assert_eq!(proxied, batch());
    }

    #[test]
    fn batch_reorders_only_the_slots_of_rankable_candidates() {
        let ranker = PeerRanker::new(elected(Some("123.213.32.10"), None), 6881);
        let unranked_first = v6("2001:db8:2::5", 6881);
        let unranked_mid = v6("2001:db8:3::5", 6881);
        let mut ranked: Vec<SocketAddr> = batch().into_iter().take(6).collect();
        let mut addrs = vec![unranked_first];
        addrs.extend(ranked.iter().take(3));
        addrs.push(unranked_mid);
        addrs.extend(ranked.iter().skip(3));
        ranker.order_batch(&mut addrs);
        assert_eq!(addrs[0], unranked_first);
        assert_eq!(addrs[4], unranked_mid);
        let mut reordered: Vec<SocketAddr> = addrs
            .iter()
            .copied()
            .filter(|addr| addr.is_ipv4())
            .collect();
        let priorities: Vec<u32> = reordered
            .iter()
            .map(|&addr| ranker.priority(addr).unwrap())
            .collect();
        assert!(priorities.windows(2).all(|pair| pair[0] >= pair[1]));
        reordered.sort();
        ranked.sort();
        assert_eq!(reordered, ranked);
    }
}
