use std::net::IpAddr;
use std::time::{Duration, Instant};

use rand::RngExt;

use crate::core::crc32c::crc32c;
use crate::core::Id20;

const IPV4_MASK: u32 = 0x030f_3fff;
const IPV6_MASK: u64 = 0x0103_070f_1f3f_7fff;
pub(super) const MIN_REGENERATION_INTERVAL: Duration = Duration::from_secs(10 * 60);

fn node_id_crc(ip: IpAddr, rand: u8) -> u32 {
    let r = rand & 7;
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let masked = (u32::from(ip) & IPV4_MASK) | (u32::from(r) << 29);
            crc32c(&masked.to_be_bytes())
        }
        IpAddr::V6(ip) => {
            let mut high = [0u8; 8];
            high.copy_from_slice(&ip.octets()[..8]);
            let masked = (u64::from_be_bytes(high) & IPV6_MASK) | (u64::from(r) << 61);
            crc32c(&masked.to_be_bytes())
        }
    }
}

fn exempt_from_node_id_check(ip: IpAddr) -> bool {
    crate::net::is_lan(ip.to_canonical())
}

pub(crate) fn derive_node_id(ip: IpAddr, entropy: [u8; 20]) -> Id20 {
    let crc = node_id_crc(ip, entropy[19]);
    let mut id = entropy;
    id[0] = (crc >> 24) as u8;
    id[1] = (crc >> 16) as u8;
    id[2] = ((crc >> 8) as u8 & 0xf8) | (entropy[2] & 0x07);
    Id20(id)
}

pub(crate) fn generate_node_id(ip: IpAddr) -> Id20 {
    let mut entropy = [0u8; 20];
    rand::rng().fill(&mut entropy);
    derive_node_id(ip, entropy)
}

pub(crate) fn node_id_valid(id: &Id20, ip: IpAddr) -> bool {
    if exempt_from_node_id_check(ip) {
        return true;
    }
    let expected = node_id_crc(ip, id.0[19]);
    id.0[0] == (expected >> 24) as u8
        && id.0[1] == (expected >> 16) as u8
        && id.0[2] & 0xf8 == (expected >> 8) as u8 & 0xf8
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdPlan {
    Keep,
    Replace(Id20),
    Wait(Duration),
}

pub(crate) fn plan_node_id(
    current: &Id20,
    elected: Option<IpAddr>,
    proxied: bool,
    last_change: Option<Instant>,
    interval: Duration,
    now: Instant,
) -> IdPlan {
    let Some(elected) = elected else {
        return IdPlan::Keep;
    };
    if proxied || node_id_valid(current, elected) {
        return IdPlan::Keep;
    }
    let remaining = last_change
        .map(|at| interval.saturating_sub(now.saturating_duration_since(at)))
        .filter(|remaining| !remaining.is_zero());
    match remaining {
        Some(remaining) => IdPlan::Wait(remaining),
        None => IdPlan::Replace(generate_node_id(elected)),
    }
}

pub(crate) fn initial_node_id(
    saved: Option<Id20>,
    elected: Option<IpAddr>,
    proxied: bool,
    random: Id20,
) -> Id20 {
    if proxied {
        return random;
    }
    match (saved, elected) {
        (Some(saved), Some(elected)) if !node_id_valid(&saved, elected) => {
            generate_node_id(elected)
        }
        (Some(saved), _) => saved,
        (None, Some(elected)) => generate_node_id(elected),
        (None, None) => random,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn id_with_rand(rand: u8) -> [u8; 20] {
        let mut entropy = [0x5au8; 20];
        entropy[19] = rand;
        entropy
    }

    fn fixed_bits(id: &Id20) -> (u8, u8, u8, u8) {
        (id.0[0], id.0[1], id.0[2] & 0xf8, id.0[19])
    }

    #[test]
    fn bep_42_test_vectors_produce_the_documented_prefixes() {
        let vectors: [(&str, u8, [u8; 3]); 5] = [
            ("124.31.75.21", 1, [0x5f, 0xbf, 0xb8]),
            ("21.75.31.124", 86, [0x5a, 0x3c, 0xe8]),
            ("65.23.51.170", 22, [0xa5, 0xd4, 0x30]),
            ("84.124.73.14", 65, [0x1b, 0x03, 0x20]),
            ("43.213.53.83", 90, [0xe5, 0x6f, 0x68]),
        ];
        for (address, rand, prefix) in vectors {
            let id = derive_node_id(ip(address), id_with_rand(rand));
            assert_eq!(
                fixed_bits(&id),
                (prefix[0], prefix[1], prefix[2], rand),
                "{address}"
            );
            assert!(node_id_valid(&id, ip(address)), "{address}");
        }
    }

    #[test]
    fn documented_example_ids_validate_for_their_addresses() {
        let examples = [
            ("124.31.75.21", "5fbfbff10c5d6a4ec8a88e4c6ab4c28b95eee401"),
            ("21.75.31.124", "5a3ce9c14e7a08645677bbd1cfe7d8f956d53256"),
            ("65.23.51.170", "a5d43220bc8f112a3d426c84764f8c2a1150e616"),
            ("84.124.73.14", "1b0321dd1bb1fe518101ceef99462b947a01ff41"),
        ];
        for (address, hex) in examples {
            let id: Id20 = hex.parse().unwrap();
            assert!(node_id_valid(&id, ip(address)), "{address}");
            assert!(!node_id_valid(&id, ip("8.8.8.8")), "{address}");
        }
    }

    #[test]
    fn only_the_low_three_bits_of_the_last_byte_feed_the_hash() {
        let address = ip("124.31.75.21");
        let low = derive_node_id(address, id_with_rand(1));
        let high = derive_node_id(address, id_with_rand(0xf9));
        assert_eq!(low.0[..2], high.0[..2]);
        assert_eq!(low.0[2] & 0xf8, high.0[2] & 0xf8);
        assert_eq!(high.0[19], 0xf9);
        assert!(node_id_valid(&high, address));
    }

    #[test]
    fn derived_ids_validate_for_every_random_byte_and_family() {
        for address in ["203.0.113.9", "198.51.100.200", "2a00:1450:4001:81b::200e"] {
            let address = ip(address);
            for rand in 0..=255u8 {
                let id = derive_node_id(address, id_with_rand(rand));
                assert!(node_id_valid(&id, address));
                assert_eq!(id.0[19], rand);
            }
        }
    }

    #[test]
    fn generated_ids_are_random_beyond_the_fixed_bits() {
        let address = ip("124.31.75.21");
        let first = generate_node_id(address);
        let second = generate_node_id(address);
        assert!(node_id_valid(&first, address));
        assert!(node_id_valid(&second, address));
        assert_ne!(first.0[3..19], second.0[3..19]);
    }

    #[test]
    fn ipv6_ids_depend_only_on_the_masked_leading_sixty_four_bits() {
        let address = ip("2001:db8:85a3:8d3:1319:8a2e:370:7348");
        let sibling = ip("2001:db8:85a3:8d3:ffff:ffff:ffff:ffff");
        let other = ip("2001:db8:85a3:8d4:1319:8a2e:370:7348");
        for rand in [0u8, 3, 7, 200] {
            let id = derive_node_id(address, id_with_rand(rand));
            assert!(node_id_valid(&id, address));
            assert!(node_id_valid(&id, sibling));
        }
        let valid_elsewhere = (0..=255u8)
            .filter(|rand| node_id_valid(&derive_node_id(address, id_with_rand(*rand)), other))
            .count();
        assert_eq!(valid_elsewhere, 0);
    }

    #[test]
    fn ipv6_masking_ignores_bits_outside_the_mask() {
        let id = derive_node_id(ip("2001:db8:85a3:8d3::1"), id_with_rand(5));
        let masked_out = ip("2001:db8:85a3:8d3::1").to_canonical();
        let flipped = match masked_out {
            IpAddr::V6(v6) => {
                let mut octets = v6.octets();
                octets[0] ^= 0xfe;
                octets[1] ^= 0xfc;
                IpAddr::V6(octets.into())
            }
            other => other,
        };
        assert!(node_id_valid(&id, flipped));
    }

    #[test]
    fn mismatched_ids_fail_for_public_addresses() {
        let id = Id20([0x11; 20]);
        assert!(!node_id_valid(&id, ip("8.8.8.8")));
        assert!(!node_id_valid(&id, ip("2606:4700:4700::1111")));
    }

    #[test]
    fn local_and_private_addresses_are_exempt() {
        let id = Id20([0x11; 20]);
        for address in [
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.50",
            "169.254.7.7",
            "127.0.0.1",
            "127.200.1.1",
            "::1",
            "fe80::1",
            "fd00::5",
            "fc12:3456::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(node_id_valid(&id, ip(address)), "{address}");
        }
        for address in ["172.32.0.1", "192.169.0.1", "11.0.0.1", "100.64.0.1"] {
            assert!(!node_id_valid(&id, ip(address)), "{address}");
        }
    }

    #[test]
    fn mapped_ipv6_addresses_validate_as_their_ipv4_form() {
        let id = derive_node_id(ip("124.31.75.21"), id_with_rand(1));
        assert!(node_id_valid(&id, ip("::ffff:124.31.75.21")));
    }

    #[test]
    fn election_replaces_an_id_that_does_not_validate() {
        let now = Instant::now();
        let elected = ip("203.0.113.77");
        let current = Id20([0x42; 20]);
        let IdPlan::Replace(next) = plan_node_id(
            &current,
            Some(elected),
            false,
            None,
            MIN_REGENERATION_INTERVAL,
            now,
        ) else {
            panic!("expected a replacement");
        };
        assert!(node_id_valid(&next, elected));
        assert_ne!(next, current);
    }

    #[test]
    fn a_valid_id_is_kept_when_the_address_is_confirmed_again() {
        let now = Instant::now();
        let elected = ip("203.0.113.77");
        let current = generate_node_id(elected);
        assert_eq!(
            plan_node_id(
                &current,
                Some(elected),
                false,
                None,
                MIN_REGENERATION_INTERVAL,
                now
            ),
            IdPlan::Keep
        );
    }

    #[test]
    fn nothing_changes_before_an_address_is_elected() {
        let current = Id20([0x42; 20]);
        assert_eq!(
            plan_node_id(
                &current,
                None,
                false,
                None,
                MIN_REGENERATION_INTERVAL,
                Instant::now()
            ),
            IdPlan::Keep
        );
    }

    #[test]
    fn an_address_change_regenerates_exactly_once() {
        let now = Instant::now();
        let first = ip("203.0.113.77");
        let second = ip("198.51.100.20");
        let mut id = generate_node_id(first);
        let mut changed = None;
        for step in 0..3 {
            match plan_node_id(
                &id,
                Some(second),
                false,
                changed,
                MIN_REGENERATION_INTERVAL,
                now,
            ) {
                IdPlan::Replace(next) => {
                    assert_eq!(step, 0);
                    id = next;
                    changed = Some(now);
                }
                plan => assert_eq!(plan, IdPlan::Keep),
            }
        }
        assert!(node_id_valid(&id, second));
    }

    #[test]
    fn a_flapping_election_waits_out_the_regeneration_interval() {
        let start = Instant::now();
        let second = ip("198.51.100.20");
        let id = generate_node_id(ip("203.0.113.77"));
        let soon = start + Duration::from_secs(60);
        let IdPlan::Wait(remaining) = plan_node_id(
            &id,
            Some(second),
            false,
            Some(start),
            MIN_REGENERATION_INTERVAL,
            soon,
        ) else {
            panic!("expected a wait");
        };
        assert_eq!(
            remaining,
            MIN_REGENERATION_INTERVAL - Duration::from_secs(60)
        );
        let later = start + MIN_REGENERATION_INTERVAL;
        assert!(matches!(
            plan_node_id(
                &id,
                Some(second),
                false,
                Some(start),
                MIN_REGENERATION_INTERVAL,
                later
            ),
            IdPlan::Replace(_)
        ));
    }

    #[test]
    fn a_proxied_dht_never_derives_an_id_from_the_elected_address() {
        let elected = ip("203.0.113.77");
        let current = Id20([0x42; 20]);
        assert_eq!(
            plan_node_id(
                &current,
                Some(elected),
                true,
                None,
                MIN_REGENERATION_INTERVAL,
                Instant::now()
            ),
            IdPlan::Keep
        );
    }

    #[test]
    fn a_proxied_start_ignores_a_derived_saved_id() {
        let elected = ip("203.0.113.77");
        let saved = generate_node_id(elected);
        let random = Id20([0x77; 20]);
        assert_eq!(
            initial_node_id(Some(saved), Some(elected), true, random),
            random
        );
        assert_eq!(initial_node_id(None, Some(elected), true, random), random);
    }

    #[test]
    fn a_direct_start_keeps_a_saved_id_that_validates() {
        let elected = ip("203.0.113.77");
        let saved = generate_node_id(elected);
        let random = Id20([0x77; 20]);
        assert_eq!(
            initial_node_id(Some(saved), Some(elected), false, random),
            saved
        );
    }

    #[test]
    fn a_direct_start_replaces_a_saved_id_for_another_address() {
        let saved = generate_node_id(ip("198.51.100.20"));
        let elected = ip("203.0.113.77");
        let id = initial_node_id(Some(saved), Some(elected), false, Id20([0x77; 20]));
        assert_ne!(id, saved);
        assert!(node_id_valid(&id, elected));
    }

    #[test]
    fn a_direct_start_keeps_the_saved_id_until_an_address_is_elected() {
        let saved = Id20([0x33; 20]);
        assert_eq!(
            initial_node_id(Some(saved), None, false, Id20([0x77; 20])),
            saved
        );
        assert_eq!(
            initial_node_id(None, None, false, Id20([0x77; 20])),
            Id20([0x77; 20])
        );
    }

    #[test]
    fn a_direct_start_with_an_elected_address_and_no_saved_id_derives_one() {
        let elected = ip("203.0.113.77");
        let id = initial_node_id(None, Some(elected), false, Id20([0x77; 20]));
        assert!(node_id_valid(&id, elected));
    }
}
