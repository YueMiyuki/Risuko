use crate::engine::adc::{is_adc_uri, parse_adc_hub_uri, parse_dchub_file_uri, types::HubDialect};
use crate::engine::g2::{is_g2_uri, parse_g2_uri};
use crate::engine::gift::{extract_gift_name, is_gift_uri, parse_gift_uri};
use crate::engine::gnutella::{is_gnutella_uri, parse_gnutella_uri};

#[test]
fn adc_scheme_detection_covers_all_dialects() {
    for s in [
        "adc://h",
        "adcs://h",
        "dchub://h",
        "nmdc://h",
        "ADC://H",
        "AdCs://H",
    ] {
        assert!(is_adc_uri(s), "should detect: {s}");
    }
    for s in ["http://h", "magnet:?", "ed2k://", ""] {
        assert!(!is_adc_uri(s), "should reject: {s}");
    }
}

#[test]
fn adc_default_ports_per_dialect() {
    let h = parse_adc_hub_uri("adc://hub").unwrap();
    assert_eq!(h.port, 411);
    assert!(!h.tls);
    assert_eq!(h.dialect, HubDialect::Adc);

    let h = parse_adc_hub_uri("adcs://hub").unwrap();
    assert_eq!(h.port, 412);
    assert!(h.tls);
    assert_eq!(h.dialect, HubDialect::Adc);

    let h = parse_adc_hub_uri("dchub://hub").unwrap();
    assert_eq!(h.port, 411);
    assert!(!h.tls);
    assert_eq!(h.dialect, HubDialect::Nmdc);

    let h = parse_adc_hub_uri("nmdc://hub").unwrap();
    assert_eq!(h.port, 411);
    assert_eq!(h.dialect, HubDialect::Nmdc);
}

#[test]
fn adc_hub_uri_explicit_port_overrides_default() {
    let h = parse_adc_hub_uri("adcs://hub.example.com:7777/path?ignored").unwrap();
    assert_eq!(h.host, "hub.example.com");
    assert_eq!(h.port, 7777);
    assert!(h.tls);
}

#[test]
fn adc_hub_uri_rejects_unknown_scheme_and_bad_port() {
    assert!(parse_adc_hub_uri("ftp://h").is_err());
    assert!(parse_adc_hub_uri("adc://h:notaport").is_err());
    assert!(parse_adc_hub_uri("adc://").is_err());
}

#[test]
fn dchub_file_uri_decodes_dn_xl_tth() {
    let f = parse_dchub_file_uri(
        "dchub://hub:411/?TTH=PLSTQHKO5F2F5OJG6DNCKEXNV6YLQ47APLSTQHK&xl=2048&dn=movie%20name.mkv",
    )
    .unwrap();
    assert_eq!(f.file_name, "movie name.mkv");
    assert_eq!(f.file_size, 2048);
    assert_eq!(
        f.tth.as_deref(),
        Some("PLSTQHKO5F2F5OJG6DNCKEXNV6YLQ47APLSTQHK")
    );
}

#[test]
fn dchub_file_uri_handles_plus_as_space_and_missing_size() {
    let f = parse_dchub_file_uri("dchub://hub/?dn=hello+world").unwrap();
    assert_eq!(f.file_name, "hello world");
    assert_eq!(f.file_size, 0);
    assert!(f.tth.is_none());
}

#[test]
fn dchub_file_uri_returns_none_when_no_query() {
    assert!(parse_dchub_file_uri("dchub://hub.example.com/").is_none());
}

#[test]
fn gnutella_scheme_detection() {
    assert!(is_gnutella_uri("gnutella://h"));
    assert!(is_gnutella_uri("gnet://h"));
    assert!(is_gnutella_uri("GNET://h"));
    assert!(!is_gnutella_uri("g2://h"));
}

#[test]
fn gnutella_uri_default_port_is_6346() {
    let l = parse_gnutella_uri("gnutella://peer.example.com/uri-res/N2R?urn:sha1:ABC").unwrap();
    assert_eq!(l.host, "peer.example.com");
    assert_eq!(l.port, 6346);
    assert_eq!(l.urn.as_deref(), Some("urn:sha1:ABC"));
}

#[test]
fn gnutella_uri_extracts_dn_and_xl() {
    let l = parse_gnutella_uri(
        "gnutella://peer:6346/uri-res/N2R?urn:sha1:PLSTQHKO5F2F5OJG6DNCKEXNV6YLQ47A&dn=test%2Eiso&xl=4096",
    )
    .unwrap();
    assert_eq!(l.file_name, "test.iso");
    assert_eq!(l.file_size, 4096);
}

#[test]
fn gnutella_uri_recognises_bitprint_urn() {
    let l = parse_gnutella_uri("gnutella://h:6346/uri-res/N2R?urn:bitprint:ABCDEF.XYZ").unwrap();
    assert!(l.urn.unwrap().starts_with("urn:bitprint:"));
}

#[test]
fn g2_scheme_detection() {
    assert!(is_g2_uri("g2://h:6346/sha1/ABC"));
    assert!(is_g2_uri("G2://h"));
    assert!(!is_g2_uri("gnutella://h"));
}

#[test]
fn g2_uri_default_port_is_6346() {
    let l = parse_g2_uri("g2://peer.example.com/sha1/ABCDEF?xl=42&dn=foo.bin").unwrap();
    assert_eq!(l.port, 6346);
    assert_eq!(l.host, "peer.example.com");
    assert_eq!(l.urn.as_deref(), Some("urn:sha1:ABCDEF"));
    assert_eq!(l.file_name, "foo.bin");
    assert_eq!(l.file_size, 42);
}

#[test]
fn g2_uri_decodes_percent_encoded_name() {
    let l = parse_g2_uri("g2://h:6346/sha1/ABC?xl=10&dn=video%20clip%2Emp4").unwrap();
    assert_eq!(l.file_name, "video clip.mp4");
}

#[test]
fn g2_uri_returns_none_for_non_g2_scheme() {
    assert!(parse_g2_uri("gnutella://h/").is_none());
    assert!(parse_g2_uri("g2://h:notaport/sha1/A").is_none());
}

#[test]
fn gift_scheme_detection() {
    assert!(is_gift_uri("gift://Gnutella/sha1/ABC"));
    assert!(is_gift_uri("GIFT://OpenFT/file"));
    assert!(!is_gift_uri("gnutella://h"));
}

#[test]
fn gift_uri_preserves_inner_payload_verbatim() {
    let l = parse_gift_uri("gift://Gnutella/sha1/ABC?dn=foo.bin&xl=99").unwrap();
    assert_eq!(l.inner, "Gnutella/sha1/ABC?dn=foo.bin&xl=99");
}

#[test]
fn gift_extract_name_strips_query_and_path() {
    assert_eq!(extract_gift_name("Gnutella/sha1/ABC?dn=foo.bin"), "foo.bin");
    assert_eq!(extract_gift_name("OpenFT/file?xl=1&dn=a%20b"), "a b");
    assert_eq!(extract_gift_name("Gnutella/sha1/ABC?xl=1"), "ABC");
    assert_eq!(extract_gift_name("OpenFT/dir/sub/file.zip"), "file.zip");
    assert_eq!(extract_gift_name("Gnutella/"), "gift-download");
    assert_eq!(extract_gift_name(""), "gift-download");
}
