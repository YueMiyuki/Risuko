use super::hasher::{Algo, WholeChecksum};
use quick_xml::de::from_str;
use serde::Deserialize;
use std::path::Path;

const LOWEST_PRIORITY: u32 = 999_999;

#[derive(Debug, Deserialize)]
struct Metalink {
    #[serde(rename = "file", default)]
    files: Vec<RawFile>,
}

#[derive(Debug, Deserialize)]
struct RawFile {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "url", default)]
    urls: Vec<RawUrl>,
    #[serde(rename = "hash", default)]
    hashes: Vec<RawHash>,
}

#[derive(Debug, Deserialize)]
struct RawUrl {
    #[serde(rename = "@priority")]
    priority: Option<u32>,
    #[serde(rename = "$text")]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawHash {
    #[serde(rename = "@type")]
    kind: String,
    #[serde(rename = "$text")]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MetalinkV3 {
    #[serde(rename = "files")]
    files: Option<FilesV3>,
}

#[derive(Debug, Deserialize)]
struct FilesV3 {
    #[serde(rename = "file", default)]
    file: Vec<RawFileV3>,
}

#[derive(Debug, Deserialize)]
struct RawFileV3 {
    #[serde(rename = "@name")]
    name: Option<String>,
    resources: Option<ResourcesV3>,
    verification: Option<VerificationV3>,
}

#[derive(Debug, Deserialize)]
struct ResourcesV3 {
    #[serde(rename = "url", default)]
    url: Vec<UrlV3>,
}

#[derive(Debug, Deserialize)]
struct UrlV3 {
    #[serde(rename = "@type")]
    kind: Option<String>,
    #[serde(rename = "@preference")]
    preference: Option<u32>,
    #[serde(rename = "$text")]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VerificationV3 {
    #[serde(rename = "hash", default)]
    hash: Vec<RawHash>,
}

#[derive(Debug, Clone)]
pub struct MetalinkFile {
    pub name: String,
    pub uris: Vec<String>,
    pub checksum: Option<WholeChecksum>,
}

fn url_scheme(v: &str) -> String {
    v.split_once(':')
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_default()
}

fn is_http_url(v: &str) -> bool {
    matches!(url_scheme(v).as_str(), "http" | "https")
}

fn is_file_transfer_url(v: &str) -> bool {
    matches!(url_scheme(v).as_str(), "ftp" | "ftps" | "sftp")
}

fn keep_fetchable(uris: Vec<String>) -> Vec<String> {
    if uris.iter().any(|u| is_http_url(u)) {
        uris.into_iter().filter(|u| is_http_url(u)).collect()
    } else {
        uris.into_iter()
            .filter(|u| is_file_transfer_url(u))
            .collect()
    }
}

pub fn parse(xml: &str) -> Result<Vec<MetalinkFile>, String> {
    parse_v4(xml).or_else(|_| parse_v3(xml))
}

fn parse_v4(xml: &str) -> Result<Vec<MetalinkFile>, String> {
    let doc: Metalink = from_str(xml).map_err(|e| format!("metalink parse error: {e}"))?;

    let files: Vec<MetalinkFile> = doc
        .files
        .into_iter()
        .map(|f| {
            let mut urls: Vec<(u32, String)> = f
                .urls
                .into_iter()
                .filter_map(|u| {
                    let v = u.value?.trim().to_string();
                    (!v.is_empty()).then_some((u.priority.unwrap_or(LOWEST_PRIORITY), v))
                })
                .collect();
            urls.sort_by_key(|(p, _)| *p);
            MetalinkFile {
                name: sanitize_name(&f.name),
                uris: keep_fetchable(urls.into_iter().map(|(_, v)| v).collect()),
                checksum: pick_strongest(&f.hashes),
            }
        })
        .filter(|f| !f.uris.is_empty())
        .collect();

    if files.is_empty() {
        return Err("metalink has no downloadable <url> entries".into());
    }
    Ok(files)
}

fn parse_v3(xml: &str) -> Result<Vec<MetalinkFile>, String> {
    let doc: MetalinkV3 = from_str(xml).map_err(|e| format!("metalink v3 parse error: {e}"))?;

    let files: Vec<MetalinkFile> = doc
        .files
        .map(|f| f.file)
        .unwrap_or_default()
        .into_iter()
        .map(|f| {
            let mut urls: Vec<(i64, String)> = f
                .resources
                .map(|r| r.url)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|u| {
                    let v = u.value?.trim().to_string();
                    let typed_http = u.kind.as_deref().is_none_or(|k| {
                        matches!(
                            k.trim().to_ascii_lowercase().as_str(),
                            "http" | "https" | ""
                        )
                    });
                    (typed_http && is_http_url(&v))
                        .then_some((-(u.preference.unwrap_or(0) as i64), v))
                })
                .collect();
            urls.sort_by_key(|(k, _)| *k);
            MetalinkFile {
                name: sanitize_name(f.name.as_deref().unwrap_or("")),
                uris: keep_fetchable(urls.into_iter().map(|(_, v)| v).collect()),
                checksum: pick_strongest(&f.verification.map(|v| v.hash).unwrap_or_default()),
            }
        })
        .filter(|f| !f.uris.is_empty())
        .collect();

    if files.is_empty() {
        return Err("metalink v3 has no downloadable <url> entries".into());
    }
    Ok(files)
}

pub fn url_hints_metalink(uri: &str) -> bool {
    let path = uri.split(['?', '#']).next().unwrap_or(uri);
    let last = path.rsplit('/').next().unwrap_or("").to_ascii_lowercase();
    last.ends_with(".meta4") || last.ends_with(".metalink") || last == "metalink"
}

fn sanitize_name(name: &str) -> String {
    let base = Path::new(name)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    crate::engine::util::safe_filename(&base, "download")
}

fn pick_strongest(hashes: &[RawHash]) -> Option<WholeChecksum> {
    fn rank(a: Algo) -> u8 {
        match a {
            Algo::Sha256 => 3,
            Algo::Sha1 => 2,
            Algo::Md5 => 1,
        }
    }
    let mut best: Option<WholeChecksum> = None;
    for h in hashes {
        let Some(algo) = Algo::parse(&h.kind) else {
            continue;
        };
        let Some(hex) = h.value.as_ref().map(|v| v.trim().to_ascii_lowercase()) else {
            continue;
        };
        if hex.len() != algo.hex_len() || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if best.as_ref().is_none_or(|b| rank(algo) > rank(b.algo)) {
            best = Some(WholeChecksum { algo, hex });
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<metalink xmlns="urn:ietf:params:xml:ns:metalink">
  <file name="../../etc/evil.iso">
    <size>14471447</size>
    <hash type="md5">a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6</hash>
    <hash type="sha-256">e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</hash>
    <url priority="2">http://b.example/evil.iso</url>
    <url priority="1">http://a.example/evil.iso</url>
    <url>http://c.example/evil.iso</url>
  </file>
  <file name="notes.txt">
    <url>ftp://mirror.example/notes.txt</url>
  </file>
</metalink>"#;

    #[test]
    fn parses_priority_hash_and_name() {
        let files = parse(SAMPLE).unwrap();
        assert_eq!(files.len(), 2);

        let f0 = &files[0];

        assert_eq!(f0.name, "evil.iso");

        assert_eq!(
            f0.uris,
            vec![
                "http://a.example/evil.iso",
                "http://b.example/evil.iso",
                "http://c.example/evil.iso",
            ]
        );
        let c = f0.checksum.as_ref().unwrap();
        assert_eq!(c.algo, Algo::Sha256);

        assert_eq!(files[1].uris, vec!["ftp://mirror.example/notes.txt"]);
        assert!(files[1].checksum.is_none());
    }

    #[test]
    fn rejects_when_no_urls() {
        let xml = r#"<metalink><file name="x"><size>1</size></file></metalink>"#;
        assert!(parse(xml).is_err());
    }

    #[test]
    fn parses_v3_files_resources_and_preference() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<metalink version="3.0" xmlns="http://www.metalinker.org/">
  <files>
    <file name="Fedora.iso">
      <verification>
        <hash type="md5">a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6</hash>
        <hash type="sha256">e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</hash>
      </verification>
      <resources>
        <url protocol="https" preference="50">https://b/Fedora.iso</url>
        <url protocol="https" preference="100">https://a/Fedora.iso</url>
        <url protocol="ftp">ftp://c/Fedora.iso</url>
      </resources>
    </file>
  </files>
</metalink>"#;
        let files = parse(xml).unwrap();
        assert_eq!(files.len(), 1);
        let f = &files[0];
        assert_eq!(f.name, "Fedora.iso");

        assert_eq!(
            f.uris,
            vec!["https://a/Fedora.iso", "https://b/Fedora.iso",]
        );
        assert_eq!(f.checksum.as_ref().unwrap().algo, Algo::Sha256);
    }

    #[test]
    fn url_hints() {
        assert!(url_hints_metalink("https://x/foo.meta4"));
        assert!(url_hints_metalink("https://x/foo.metalink?a=b"));

        assert!(url_hints_metalink(
            "https://mirrors.fedoraproject.org/metalink?repo=fedora-40&arch=x86_64"
        ));

        assert!(!url_hints_metalink("https://x/metalink.zip"));
        assert!(!url_hints_metalink("https://x/big.iso"));
    }

    #[test]
    fn drops_non_http_mirrors() {
        let v4 = r#"<metalink xmlns="urn:ietf:params:xml:ns:metalink"><file name="a.bin">
            <url priority="1">rsync://h/a.bin</url><url priority="2">ftp://h/a.bin</url>
            <url priority="3">magnet:?xt=urn:btih:abc</url><url priority="4">https://h/a.bin</url>
            </file></metalink>"#;
        let files = parse(v4).unwrap();
        assert_eq!(files[0].uris, vec!["https://h/a.bin".to_string()]);

        let v3 = r#"<metalink version="3.0"><files><file name="a.bin"><resources>
            <url type="bittorrent" preference="100">http://h/a.torrent</url>
            <url type="ftp" preference="90">ftp://h/a.bin</url>
            <url type="http" preference="10">http://h/a.bin</url>
            </resources></file></files></metalink>"#;
        let files = parse(v3).unwrap();
        assert_eq!(files[0].uris, vec!["http://h/a.bin".to_string()]);
    }
}
