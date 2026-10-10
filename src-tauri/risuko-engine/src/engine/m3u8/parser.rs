use url::Url;

#[derive(Clone)]
pub enum ParsedPlaylist {
    Master {
        variants: Vec<Variant>,
    },
    Media {
        segments: Vec<Segment>,
        media_sequence: u64,
        end_list: bool,
    },
}

#[derive(Clone)]
pub struct Variant {
    pub bandwidth: u64,
    pub url: String,
    pub separate_audio: bool,
}

#[derive(Clone)]
pub struct Segment {
    pub url: String,
    pub byte_range: Option<ByteRange>,
    pub encryption: Option<EncryptionInfo>,
}

#[derive(Clone)]
pub struct ByteRange {
    pub length: u64,
    pub offset: u64,
}

#[derive(Clone)]
pub struct EncryptionInfo {
    pub method: String,
    pub key_uri: String,
    pub iv: Option<Vec<u8>>,
}

pub fn is_m3u8_uri(uri: &str) -> bool {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return false;
    }

    if let Ok(parsed) = Url::parse(trimmed) {
        let path = parsed.path().to_lowercase();
        return path.ends_with(".m3u8") || path.ends_with(".m3u");
    }

    let lower = trimmed.to_lowercase();
    let path = lower.split('?').next().unwrap_or(&lower);
    let path = path.split('#').next().unwrap_or(path);
    path.ends_with(".m3u8") || path.ends_with(".m3u")
}

pub fn resolve_segment_url(base_url: &str, segment_uri: &str) -> Result<String, String> {
    let base = Url::parse(base_url).map_err(|e| format!("Invalid base URL: {e}"))?;
    let resolved = base
        .join(segment_uri)
        .map_err(|e| format!("Failed to resolve segment URL: {e}"))?;
    Ok(resolved.to_string())
}

const PLAYLIST_FETCH_ATTEMPTS: u32 = 3;
const MAX_PLAYLIST_BYTES: usize = 4 * 1024 * 1024;
const PLAYLIST_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub async fn fetch_and_parse_playlist(
    url: &str,
    client: &risuko_http::Client,
) -> Result<ParsedPlaylist, String> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match fetch_playlist_bytes(url, client).await {
            Ok(bytes) => return parse_playlist_bytes(&bytes, url),
            Err(e) if attempt >= PLAYLIST_FETCH_ATTEMPTS => return Err(e),
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(500 * u64::from(attempt)))
                    .await;
            }
        }
    }
}

async fn fetch_playlist_bytes(url: &str, client: &risuko_http::Client) -> Result<Vec<u8>, String> {
    let fetch = async {
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("Failed to fetch playlist: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!(
                "Playlist fetch failed with status {}",
                resp.status()
            ));
        }

        resp.bytes_limited(MAX_PLAYLIST_BYTES)
            .await
            .map(|b| b.to_vec())
            .map_err(|e| format!("Failed to read playlist body: {e}"))
    };
    tokio::time::timeout(PLAYLIST_FETCH_TIMEOUT, fetch)
        .await
        .map_err(|_| "Playlist fetch timed out".to_string())?
}

fn parse_playlist_bytes(bytes: &[u8], base_url: &str) -> Result<ParsedPlaylist, String> {
    let (_, playlist) = m3u8_rs::parse_playlist(bytes)
        .map_err(|e| format!("Failed to parse M3U8 playlist: {e:?}"))?;

    match playlist {
        m3u8_rs::Playlist::MasterPlaylist(master) => {
            let alternatives = master.alternatives;
            let variants = master
                .variants
                .into_iter()
                .filter(|v| !v.is_i_frame)
                .map(|v| {
                    let separate_audio = v.audio.as_ref().is_some_and(|group| {
                        alternatives.iter().any(|alt| {
                            alt.media_type == m3u8_rs::AlternativeMediaType::Audio
                                && &alt.group_id == group
                                && alt.uri.is_some()
                        })
                    });
                    let url = resolve_segment_url(base_url, &v.uri).unwrap_or(v.uri);
                    Variant {
                        bandwidth: v.bandwidth,
                        url,
                        separate_audio,
                    }
                })
                .collect();
            Ok(ParsedPlaylist::Master { variants })
        }
        m3u8_rs::Playlist::MediaPlaylist(media) => {
            if media.segments.iter().any(|seg| seg.map.is_some()) {
                return Err("fMP4/CMAF HLS playlists (EXT-X-MAP) are not supported".to_string());
            }
            check_supported_keys(&media.segments)?;
            let media_sequence = media.media_sequence;
            let end_list = media.end_list;
            let mut current_encryption: Option<EncryptionInfo> = None;
            let mut byte_range_offset: u64 = 0;
            let mut byte_range_url = String::new();

            let segments = media
                .segments
                .into_iter()
                .map(|seg| {
                    if let Some(ref key) = seg.key {
                        current_encryption = parse_key_tag(key, base_url);
                    }

                    let url = resolve_segment_url(base_url, &seg.uri).unwrap_or(seg.uri);

                    if byte_range_url != url {
                        byte_range_offset = 0;
                        byte_range_url = url.clone();
                    }
                    let byte_range = seg.byte_range.map(|br| {
                        let length = br.length;
                        let offset = br.offset.unwrap_or(byte_range_offset);
                        byte_range_offset = offset + length;
                        ByteRange { length, offset }
                    });

                    Segment {
                        url,
                        byte_range,
                        encryption: current_encryption.clone(),
                    }
                })
                .collect();

            Ok(ParsedPlaylist::Media {
                segments,
                media_sequence,
                end_list,
            })
        }
    }
}

fn check_supported_keys(segments: &[m3u8_rs::MediaSegment]) -> Result<(), String> {
    for key in segments.iter().filter_map(|seg| seg.key.as_ref()) {
        let format = key.keyformat.as_deref().unwrap_or("identity");
        match &key.method {
            m3u8_rs::KeyMethod::None => {}
            m3u8_rs::KeyMethod::AES128 => {
                if !format.eq_ignore_ascii_case("identity") {
                    return Err(format!(
                        "Unsupported HLS encryption AES-128 with KEYFORMAT {format}"
                    ));
                }
                if key.uri.is_none() {
                    return Err("Unsupported HLS encryption: AES-128 key has no URI".to_string());
                }
            }
            m3u8_rs::KeyMethod::SampleAES => {
                return Err(format!(
                    "Unsupported HLS encryption SAMPLE-AES with KEYFORMAT {format}"
                ));
            }
            m3u8_rs::KeyMethod::Other(method) => {
                return Err(format!("Unsupported HLS encryption {method}"));
            }
        }
    }
    Ok(())
}

fn parse_key_tag(key: &m3u8_rs::Key, base_url: &str) -> Option<EncryptionInfo> {
    let method = match key.method {
        m3u8_rs::KeyMethod::None => return None,
        m3u8_rs::KeyMethod::AES128 => "AES-128".to_string(),
        m3u8_rs::KeyMethod::SampleAES => "SAMPLE-AES".to_string(),
        _ => return None,
    };

    let key_uri = key.uri.as_ref()?;
    let resolved_uri = resolve_segment_url(base_url, key_uri).unwrap_or_else(|_| key_uri.clone());

    let iv = key.iv.as_ref().and_then(|iv_str| parse_hex_iv(iv_str));

    Some(EncryptionInfo {
        method,
        key_uri: resolved_uri,
        iv,
    })
}

fn parse_hex_iv(iv_str: &str) -> Option<Vec<u8>> {
    let hex = iv_str
        .strip_prefix("0x")
        .or_else(|| iv_str.strip_prefix("0X"))
        .unwrap_or(iv_str);
    if hex.len() != 32 {
        return None;
    }
    hex::decode(hex).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn serve_body(len: usize) -> String {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&vec![b'x'; len]).await;
            }
        });
        format!("http://127.0.0.1:{port}/")
    }

    #[tokio::test]
    async fn oversized_playlists_are_rejected() {
        let url = serve_body(MAX_PLAYLIST_BYTES + 1).await;
        let client = risuko_http::Client::builder().build().unwrap();
        let error = fetch_playlist_bytes(&url, &client).await.unwrap_err();
        assert!(error.contains("Failed to read playlist body"), "{error}");
    }

    #[test]
    fn test_is_m3u8_uri() {
        assert!(is_m3u8_uri("https://example.com/video.m3u8"));
        assert!(is_m3u8_uri("https://example.com/video.m3u8?token=abc"));
        assert!(is_m3u8_uri("https://example.com/path/index.M3U8"));
        assert!(is_m3u8_uri("https://example.com/video.m3u"));
        assert!(is_m3u8_uri("  https://example.com/video.m3u8  "));
        assert!(!is_m3u8_uri("https://example.com/video.mp4"));
        assert!(!is_m3u8_uri("https://example.com/m3u8/notaplaylist"));
        assert!(!is_m3u8_uri(""));
    }

    #[test]
    fn test_resolve_segment_url_absolute() {
        let result = resolve_segment_url(
            "https://cdn.example.com/hls/master.m3u8",
            "https://other.com/seg0.ts",
        );
        assert_eq!(result.unwrap(), "https://other.com/seg0.ts");
    }

    #[test]
    fn test_resolve_segment_url_relative() {
        let result = resolve_segment_url("https://cdn.example.com/hls/master.m3u8", "seg0.ts");
        assert_eq!(result.unwrap(), "https://cdn.example.com/hls/seg0.ts");
    }

    #[test]
    fn test_resolve_segment_url_absolute_path() {
        let result =
            resolve_segment_url("https://cdn.example.com/hls/master.m3u8", "/videos/seg0.ts");
        assert_eq!(result.unwrap(), "https://cdn.example.com/videos/seg0.ts");
    }

    #[test]
    fn test_parse_hex_iv() {
        let iv = parse_hex_iv("0x00000000000000000000000000000001");
        assert!(iv.is_some());
        let bytes = iv.unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[15], 1);
        assert_eq!(bytes[0], 0);
    }

    #[test]
    fn test_parse_playlist_master() {
        let data = b"#EXTM3U\n\
            #EXT-X-STREAM-INF:BANDWIDTH=1280000,RESOLUTION=720x480\n\
            low.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=2560000,RESOLUTION=1280x720\n\
            mid.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=7680000,RESOLUTION=1920x1080\n\
            high.m3u8\n";

        let result = parse_playlist_bytes(data, "https://example.com/hls/master.m3u8");
        assert!(result.is_ok());
        if let ParsedPlaylist::Master { variants } = result.unwrap() {
            assert_eq!(variants.len(), 3);
            assert_eq!(variants[0].bandwidth, 1280000);
            assert_eq!(variants[0].url, "https://example.com/hls/low.m3u8");
            assert_eq!(variants[2].bandwidth, 7680000);
        } else {
            panic!("Expected master playlist");
        }
    }

    #[test]
    fn master_skips_iframe_variants_and_flags_separate_audio() {
        let data = b"#EXTM3U\n\
            #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"en\",DEFAULT=YES,URI=\"audio.m3u8\"\n\
            #EXT-X-STREAM-INF:BANDWIDTH=1000,AUDIO=\"aud\"\n\
            v.m3u8\n\
            #EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=9999999,URI=\"iframe.m3u8\"\n";
        let Ok(ParsedPlaylist::Master { variants }) =
            parse_playlist_bytes(data, "https://e.com/m.m3u8")
        else {
            panic!("expected master playlist");
        };
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].url, "https://e.com/v.m3u8");
        assert!(variants[0].separate_audio);
    }

    #[test]
    fn rejects_unsupported_encryption() {
        for key in [
            "#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"k\"",
            "#EXT-X-KEY:METHOD=AES-128,URI=\"skd://x\",KEYFORMAT=\"com.apple.streamingkeydelivery\"",
            "#EXT-X-KEY:METHOD=AES-128",
        ] {
            let pl = format!(
                "#EXTM3U\n#EXT-X-TARGETDURATION:4\n{key}\n#EXTINF:4.0,\ns.ts\n#EXT-X-ENDLIST\n"
            );
            let err = parse_playlist_bytes(pl.as_bytes(), "https://e.com/a.m3u8").err();
            assert!(
                err.is_some_and(|e| e.contains("Unsupported HLS encryption")),
                "{key}"
            );
        }
    }

    #[test]
    fn byte_range_offset_resets_for_a_new_resource() {
        let pl = b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n\
            #EXTINF:4.0,\n#EXT-X-BYTERANGE:100@0\na.ts\n\
            #EXTINF:4.0,\n#EXT-X-BYTERANGE:50\na.ts\n\
            #EXTINF:4.0,\n#EXT-X-BYTERANGE:70\nb.ts\n#EXT-X-ENDLIST\n";
        let Ok(ParsedPlaylist::Media { segments, .. }) =
            parse_playlist_bytes(pl, "https://e.com/a.m3u8")
        else {
            panic!("expected media playlist");
        };
        let offsets: Vec<u64> = segments
            .iter()
            .map(|s| s.byte_range.as_ref().map(|r| r.offset).unwrap_or(u64::MAX))
            .collect();
        assert_eq!(offsets, [0, 100, 0]);
    }

    #[test]
    fn rejects_playlist_with_ext_x_map() {
        let pl = b"#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4.0,\nseg1.m4s\n#EXT-X-ENDLIST\n";
        let err = parse_playlist_bytes(pl, "https://example.com/a.m3u8").err();
        assert!(err.is_some_and(|e| e.contains("EXT-X-MAP")));
    }

    #[test]
    fn test_parse_playlist_media() {
        let data = b"#EXTM3U\n\
            #EXT-X-TARGETDURATION:10\n\
            #EXT-X-MEDIA-SEQUENCE:0\n\
            #EXTINF:9.009,\n\
            seg0.ts\n\
            #EXTINF:9.009,\n\
            seg1.ts\n\
            #EXTINF:3.003,\n\
            seg2.ts\n\
            #EXT-X-ENDLIST\n";

        let result = parse_playlist_bytes(data, "https://example.com/hls/playlist.m3u8");
        assert!(result.is_ok());
        if let ParsedPlaylist::Media {
            segments, end_list, ..
        } = result.unwrap()
        {
            assert!(end_list);
            assert_eq!(segments.len(), 3);
            assert_eq!(segments[0].url, "https://example.com/hls/seg0.ts");
        } else {
            panic!("Expected media playlist");
        }
    }
}
