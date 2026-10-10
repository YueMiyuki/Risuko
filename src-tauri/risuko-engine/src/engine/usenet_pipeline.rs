use crate::engine::archive_safety::ArchiveLimits;
use crate::engine::usenet::NzbSegment;
use crate::traits::{cleanup_stale_atomic_write_files, write_file_atomically};
use futures_util::{stream::FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

const RESUME_VERSION: u8 = 2;
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const CHECKPOINT_BYTE_INTERVAL: u64 = 256 * 1024 * 1024;
const CHECKPOINT_TIME_INTERVAL: Duration = Duration::from_secs(5);
const FETCH_CONCURRENCY: usize = 4;
const MAX_FETCH_WINDOW: usize = 128;
const RESUME_TEMP_PRUNE_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YencAssemblyLimits {
    max_file_bytes: u64,
    max_task_bytes: u64,
}

impl YencAssemblyLimits {
    pub fn new(max_file_bytes: u64, max_task_bytes: u64) -> Result<Self, String> {
        if max_file_bytes == 0 || max_task_bytes == 0 {
            return Err("yEnc assembly limits must be finite and non-zero".into());
        }
        Ok(Self {
            max_file_bytes,
            max_task_bytes,
        })
    }

    pub const fn platform_defaults() -> Self {
        let archive = if cfg!(target_os = "android") {
            ArchiveLimits::android_defaults()
        } else {
            ArchiveLimits::desktop_defaults()
        };
        Self {
            max_file_bytes: archive.max_entry_bytes,
            max_task_bytes: archive.max_expanded_bytes,
        }
    }

    pub const fn max_file_bytes(self) -> u64 {
        self.max_file_bytes
    }

    pub const fn max_task_bytes(self) -> u64 {
        self.max_task_bytes
    }

    fn validate_file_size(self, size: u64) -> Result<(), String> {
        if size > self.max_file_bytes {
            return Err(format!(
                "yEnc file size {size} exceeds the per-file limit of {} bytes",
                self.max_file_bytes
            ));
        }
        Ok(())
    }

    fn reserve_file(self, budget: &mut YencAssemblyBudget, size: u64) -> Result<(), String> {
        self.validate_file_size(size)?;
        let total = budget
            .reserved_bytes
            .checked_add(size)
            .ok_or_else(|| "yEnc task size overflowed its finite limit".to_string())?;
        if total > self.max_task_bytes {
            return Err(format!(
                "yEnc task size {total} exceeds the task limit of {} bytes",
                self.max_task_bytes
            ));
        }
        budget.reserved_bytes = total;
        Ok(())
    }
}

impl Default for YencAssemblyLimits {
    fn default() -> Self {
        Self::platform_defaults()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct YencAssemblyBudget {
    reserved_bytes: u64,
}

impl YencAssemblyBudget {
    pub const fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResumeSegment {
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResumeSidecar {
    pub version: u8,
    pub manifest_sha256: String,
    #[serde(default)]
    pub completed_segments: BTreeSet<u32>,
    #[serde(default)]
    pub segment_receipts: BTreeMap<u32, ResumeSegment>,
    pub completed_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_size: Option<u64>,
    #[serde(default)]
    pub repaired: bool,
}

impl ResumeSidecar {
    pub fn new(manifest_sha256: String) -> Self {
        Self {
            version: RESUME_VERSION,
            manifest_sha256,
            completed_segments: BTreeSet::new(),
            segment_receipts: BTreeMap::new(),
            completed_bytes: 0,
            expected_size: None,
            repaired: false,
        }
    }

    pub async fn load(path: &Path, manifest_sha256: &str) -> Result<Self, String> {
        prune_stale_resume_temps(path).await;
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::new(manifest_sha256.to_string()))
            }
            Err(error) => return Err(format!("read resume metadata: {error}")),
        };
        let parsed: Self = match serde_json::from_slice(&bytes) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(Self::new(manifest_sha256.to_string())),
        };
        if parsed.version != RESUME_VERSION || parsed.manifest_sha256 != manifest_sha256 {
            return Ok(Self::new(manifest_sha256.to_string()));
        }
        Ok(parsed)
    }

    pub async fn save_atomic(&self, path: &Path) -> Result<(), String> {
        let payload = serde_json::to_vec(self)
            .map_err(|error| format!("serialize resume metadata: {error}"))?;
        let target = path.to_path_buf();
        tokio::task::spawn_blocking(move || {
            write_file_atomically(&target, &payload)
                .map_err(|error| format!("persist resume metadata: {error}"))
        })
        .await
        .map_err(|error| format!("resume metadata persistence task failed: {error}"))?
    }
}

async fn prune_stale_resume_temps(path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };
    if !should_prune_resume_temps(parent) {
        return;
    }
    let parent = parent.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || cleanup_stale_atomic_write_files(&parent)).await;
}

fn should_prune_resume_temps(parent: &Path) -> bool {
    static LAST_PRUNE: OnceLock<Mutex<HashMap<PathBuf, Instant>>> = OnceLock::new();

    let now = Instant::now();
    let mut last_prune = LAST_PRUNE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    last_prune.retain(|_, last| now.saturating_duration_since(*last) < RESUME_TEMP_PRUNE_INTERVAL);
    last_prune.insert(parent.to_path_buf(), now).is_none()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArticleFetchError {
    Unavailable(String),
    Failed(String),
}

impl fmt::Display for ArticleFetchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(message) => write!(formatter, "article unavailable: {message}"),
            Self::Failed(message) => formatter.write_str(message),
        }
    }
}

pub trait ArticleSource: Send + Sync {
    fn fetch<'a>(
        &'a self,
        message_id: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DecodedYencPart, ArticleFetchError>>
                + Send
                + 'a,
        >,
    >;

    fn concurrency(&self) -> usize {
        FETCH_CONCURRENCY
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YencRange {
    pub start: u64,
    pub end: u64,
}

impl YencRange {
    fn len(self) -> Option<u64> {
        self.end.checked_sub(self.start)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedYencPart {
    pub data: Vec<u8>,
    pub range: YencRange,
    pub file_size: Option<u64>,
    pub has_explicit_range: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyReport {
    pub output: PathBuf,
    pub part_path: PathBuf,
    pub sidecar_path: PathBuf,
    pub manifest_sha256: String,
    pub expected_size: Option<u64>,
    pub completed_bytes: u64,
    pub unavailable_segments: Vec<u32>,
    pub complete: bool,
}

impl AssemblyReport {
    pub fn source_path(&self) -> &Path {
        if self.complete {
            &self.output
        } else {
            &self.part_path
        }
    }
}

pub fn partial_path(output: &Path) -> PathBuf {
    output.with_extension(format!(
        "{}.part",
        output
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("download")
    ))
}

pub fn resume_sidecar_path(output: &Path) -> PathBuf {
    output.with_extension(format!(
        "{}.resume.json",
        output
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("part")
    ))
}

pub async fn resume_sidecar_matches(
    output: &Path,
    segments: &[NzbSegment],
) -> Result<bool, String> {
    let sidecar_path = resume_sidecar_path(output);
    let bytes = match tokio::fs::read(&sidecar_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("read resume metadata: {error}")),
    };
    let sidecar: ResumeSidecar = match serde_json::from_slice(&bytes) {
        Ok(sidecar) => sidecar,
        Err(_) => return Ok(false),
    };
    Ok(sidecar.version == RESUME_VERSION && sidecar.manifest_sha256 == manifest_sha256(segments))
}

pub fn manifest_sha256(segments: &[NzbSegment]) -> String {
    let mut hasher = Sha256::new();
    for segment in segments {
        hasher.update(segment.number.to_be_bytes());
        hasher.update(segment.bytes.to_be_bytes());
        hasher.update(segment.message_id.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

pub fn decode_yenc_part(input: &[u8]) -> Result<DecodedYencPart, String> {
    let mut begin_header = None;
    let mut part_header = None;
    let mut end_header = None;
    let mut in_payload = false;
    let mut output = Vec::new();
    let mut decoded_cap = None;

    for line in input
        .split(|byte| *byte == b'\r' || *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        if !in_payload {
            if line.starts_with(b"=ybegin ") {
                let header = parse_yenc_header(line, "=ybegin")?;
                decoded_cap = header_u64(&header, "size")?;
                begin_header = Some(header);
                in_payload = true;
            }
            continue;
        }
        if line.starts_with(b"=ypart ") {
            if part_header.is_some() {
                return Err("multiple =ypart headers in one yEnc article".into());
            }
            let header = parse_yenc_header(line, "=ypart")?;
            if let Some(end_inclusive) = header_u64(&header, "end")? {
                decoded_cap = Some(match decoded_cap {
                    Some(cap) => cap.min(end_inclusive),
                    None => end_inclusive,
                });
            }
            part_header = Some(header);
            continue;
        }
        if line.starts_with(b"=yend ") {
            end_header = Some(parse_yenc_header(line, "=yend")?);
            break;
        }
        decode_yenc_line(line, &mut output)?;
        if let Some(cap) = decoded_cap {
            if output.len() as u64 > cap {
                return Err("yEnc payload exceeds the declared file size".into());
            }
        }
    }

    let begin_header = begin_header.ok_or_else(|| "article is not yEnc encoded".to_string())?;
    let end_header = end_header.ok_or_else(|| "yEnc article has no =yend header".to_string())?;
    let declared_total = header_u64(&begin_header, "size")?;
    let declared_part_size = header_u64(&end_header, "size")?
        .ok_or_else(|| "yEnc =yend header has no size".to_string())?;
    if declared_part_size != output.len() as u64 {
        return Err(format!(
            "yEnc size mismatch: expected {declared_part_size}, got {}",
            output.len()
        ));
    }
    if let Some(expected_crc) = end_header.get("pcrc32") {
        validate_crc32(&output, expected_crc, "pcrc32")?;
    }

    let multipart = begin_header.contains_key("part")
        || begin_header.contains_key("total")
        || part_header.is_some();
    let (range, has_explicit_range) = match part_header {
        Some(header) => {
            if let (Some(begin_part), Some(part_part)) = (
                header_u64(&begin_header, "part")?,
                header_u64(&header, "part")?,
            ) {
                if begin_part != part_part {
                    return Err("yEnc =ypart part number disagrees with =ybegin".into());
                }
            }
            let begin = header_u64(&header, "begin")?
                .ok_or_else(|| "yEnc =ypart header has no begin".to_string())?;
            let end_inclusive = header_u64(&header, "end")?
                .ok_or_else(|| "yEnc =ypart header has no end".to_string())?;
            if begin == 0 || end_inclusive < begin {
                return Err("invalid yEnc =ypart range".into());
            }
            let range = YencRange {
                start: begin - 1,
                end: end_inclusive,
            };
            if range.len() != Some(output.len() as u64) {
                return Err(format!(
                    "yEnc =ypart range does not match decoded payload of {} bytes",
                    output.len()
                ));
            }
            (range, true)
        }
        None if multipart => return Err("multipart yEnc article is missing =ypart".into()),
        None => (
            YencRange {
                start: 0,
                end: output.len() as u64,
            },
            false,
        ),
    };
    if multipart && declared_total.is_none() {
        return Err("multipart yEnc article has no declared total size".into());
    }
    if let Some(size) = declared_total {
        if range.end > size {
            return Err("yEnc part exceeds declared file size".into());
        }
        if !has_explicit_range && size != output.len() as u64 {
            return Err("single-part yEnc size does not match decoded payload".into());
        }
        if range.start == 0 && range.end == size {
            if let Some(expected_crc) = end_header.get("crc32") {
                validate_crc32(&output, expected_crc, "crc32")?;
            }
        }
    }

    Ok(DecodedYencPart {
        data: output,
        range,
        file_size: declared_total,
        has_explicit_range,
    })
}

fn parse_yenc_header(line: &[u8], prefix: &str) -> Result<BTreeMap<String, String>, String> {
    let text = std::str::from_utf8(line).map_err(|_| "yEnc header is not ASCII".to_string())?;
    let remainder = text
        .strip_prefix(prefix)
        .ok_or_else(|| "invalid yEnc header".to_string())?;
    let mut fields = BTreeMap::new();
    let name_at = remainder
        .match_indices("name=")
        .map(|(index, _)| index)
        .find(|&index| index == 0 || remainder.as_bytes()[index - 1].is_ascii_whitespace());
    let (keyed, name) = match name_at {
        Some(index) => (&remainder[..index], Some(remainder[index + 5..].trim())),
        None => (remainder, None),
    };
    for token in keyed.split_ascii_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        if key.is_empty() {
            return Err("invalid yEnc header field".into());
        }
        if value.is_empty() {
            continue;
        }
        fields
            .entry(key.to_ascii_lowercase())
            .or_insert_with(|| value.to_string());
    }
    if let Some(name) = name.filter(|name| !name.is_empty()) {
        fields.insert("name".into(), name.to_string());
    }
    Ok(fields)
}

fn header_u64(header: &BTreeMap<String, String>, key: &str) -> Result<Option<u64>, String> {
    header
        .get(key)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| format!("invalid yEnc {key} value"))
        })
        .transpose()
}

fn validate_crc32(data: &[u8], encoded: &str, field: &str) -> Result<(), String> {
    if encoded.len() != 8 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("invalid yEnc {field}"));
    }
    let expected = u32::from_str_radix(encoded, 16).map_err(|_| format!("invalid yEnc {field}"))?;
    let actual = crc32(data);
    if actual != expected {
        return Err(format!(
            "yEnc {field} mismatch: expected {expected:08x}, got {actual:08x}"
        ));
    }
    Ok(())
}

fn decode_yenc_line(line: &[u8], output: &mut Vec<u8>) -> Result<(), String> {
    crate::engine::archive_pipeline::decode_yenc_line(line, output)
        .map_err(|error| error.to_string())
}

const fn build_crc32_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][index] = crc;
        index += 1;
    }
    let mut index = 0;
    while index < 256 {
        let mut slice = 1;
        while slice < 8 {
            let previous = tables[slice - 1][index];
            tables[slice][index] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
            slice += 1;
        }
        index += 1;
    }
    tables
}

static CRC32_TABLES: [[u32; 256]; 8] = build_crc32_tables();

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let tables = &CRC32_TABLES;
    let mut crc = 0xffff_ffffu32;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let low = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) ^ crc;
        let high = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = tables[7][(low & 0xff) as usize]
            ^ tables[6][((low >> 8) & 0xff) as usize]
            ^ tables[5][((low >> 16) & 0xff) as usize]
            ^ tables[4][(low >> 24) as usize]
            ^ tables[3][(high & 0xff) as usize]
            ^ tables[2][((high >> 8) & 0xff) as usize]
            ^ tables[1][((high >> 16) & 0xff) as usize]
            ^ tables[0][(high >> 24) as usize];
    }
    for byte in chunks.remainder() {
        crc = (crc >> 8) ^ tables[0][((crc ^ *byte as u32) & 0xff) as usize];
    }
    !crc
}

async fn validate_existing_receipts(
    part_path: &Path,
    sidecar: &mut ResumeSidecar,
    limits: YencAssemblyLimits,
) -> Result<(), String> {
    let metadata = match tokio::fs::metadata(part_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            sidecar.completed_segments.clear();
            sidecar.segment_receipts.clear();
            sidecar.completed_bytes = 0;
            return Ok(());
        }
        Err(error) => return Err(format!("read assembled part metadata: {error}")),
    };
    if !metadata.is_file() {
        return Err("assembled part is not a regular file".into());
    }
    limits.validate_file_size(metadata.len())?;
    let file_len = metadata.len();
    let path = part_path.to_path_buf();
    let receipts = sidecar.segment_receipts.clone();
    let valid = tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(&path)
            .map_err(|error| format!("open assembled part for validation: {error}"))?;
        let mut buffer = vec![0u8; HASH_BUFFER_BYTES];
        let mut valid = BTreeMap::new();
        for (number, receipt) in receipts {
            let Some(range) = receipt_range(&receipt) else {
                continue;
            };
            if range.end > file_len {
                continue;
            }
            if hash_file_range(&mut file, &mut buffer, receipt.offset, receipt.length)?
                == receipt.sha256
            {
                valid.insert(number, receipt);
            }
        }
        Ok::<_, String>(valid)
    })
    .await
    .map_err(|error| format!("resume validation worker failed: {error}"))??;
    sidecar.completed_segments = valid.keys().copied().collect();
    sidecar.completed_bytes = completed_receipt_bytes(&valid)?;
    sidecar.segment_receipts = valid;
    Ok(())
}

fn hash_file_range(
    file: &mut std::fs::File,
    buffer: &mut [u8],
    offset: u64,
    length: u64,
) -> Result<String, String> {
    use std::io::{Read as _, Seek as _};
    file.seek(std::io::SeekFrom::Start(offset))
        .map_err(|error| format!("seek assembled part for validation: {error}"))?;
    let mut hasher = Sha256::new();
    let mut remaining = length;
    while remaining > 0 {
        let wanted = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..wanted])
            .map_err(|error| format!("read assembled part for validation: {error}"))?;
        hasher.update(&buffer[..wanted]);
        remaining -= wanted as u64;
    }
    Ok(hex::encode(hasher.finalize()))
}

fn ranges_overlap(left: YencRange, right: YencRange) -> bool {
    left.start < right.end && right.start < left.end
}

fn overlaps_occupied(
    occupied: &BTreeMap<u64, (u64, u32)>,
    invalid_receipts: &[u32],
    range: YencRange,
    number: u32,
) -> bool {
    if invalid_receipts.iter().any(|&invalid| invalid != number) {
        return true;
    }
    let overlaps =
        |(&start, &(end, _)): (&u64, &(u64, u32))| ranges_overlap(range, YencRange { start, end });
    let before = occupied
        .range(..=range.start)
        .rev()
        .find(|(_, (_, owner))| *owner != number);
    let after = occupied
        .range(range.start..)
        .find(|(_, (_, owner))| *owner != number);
    before.is_some_and(overlaps) || after.is_some_and(overlaps)
}

fn receipt_range(receipt: &ResumeSegment) -> Option<YencRange> {
    receipt
        .offset
        .checked_add(receipt.length)
        .map(|end| YencRange {
            start: receipt.offset,
            end,
        })
}

fn completed_receipt_bytes(receipts: &BTreeMap<u32, ResumeSegment>) -> Result<u64, String> {
    receipts.values().try_fold(0u64, |total, receipt| {
        total
            .checked_add(receipt.length)
            .ok_or_else(|| "resume receipt byte count overflowed".to_string())
    })
}

fn has_complete_receipt_coverage(
    segments: &[NzbSegment],
    sidecar: &ResumeSidecar,
    file_len: u64,
) -> bool {
    let Some(expected_size) = sidecar.expected_size else {
        return false;
    };
    if file_len != expected_size
        || sidecar.completed_segments.len() != segments.len()
        || sidecar.segment_receipts.len() != segments.len()
        || !segments.iter().all(|segment| {
            sidecar.completed_segments.contains(&segment.number)
                && sidecar.segment_receipts.contains_key(&segment.number)
        })
    {
        return false;
    }

    let mut ranges = Vec::with_capacity(sidecar.segment_receipts.len());
    for receipt in sidecar.segment_receipts.values() {
        let Some(range) = receipt_range(receipt) else {
            return false;
        };
        ranges.push(range);
    }
    ranges.sort_unstable_by_key(|range| range.start);
    let mut covered_to = 0u64;
    for range in ranges {
        if range.start != covered_to {
            return false;
        }
        covered_to = range.end;
    }
    covered_to == expected_size
}

fn assembly_report(
    output: &Path,
    part_path: &Path,
    sidecar_path: &Path,
    manifest_sha256: &str,
    sidecar: &ResumeSidecar,
    unavailable_segments: Vec<u32>,
    complete: bool,
) -> AssemblyReport {
    AssemblyReport {
        output: output.to_path_buf(),
        part_path: part_path.to_path_buf(),
        sidecar_path: sidecar_path.to_path_buf(),
        manifest_sha256: manifest_sha256.to_string(),
        expected_size: sidecar.expected_size,
        completed_bytes: sidecar.completed_bytes,
        unavailable_segments,
        complete,
    }
}

async fn resize_assembled_part(
    file: &mut tokio::fs::File,
    expected_size: u64,
    limits: YencAssemblyLimits,
) -> Result<(), String> {
    limits.validate_file_size(expected_size)?;
    file.set_len(expected_size)
        .await
        .map_err(|error| format!("size assembled part: {error}"))
}

fn validate_decoded_part(
    decoded: &DecodedYencPart,
    limits: YencAssemblyLimits,
) -> Result<u64, String> {
    let length = decoded
        .range
        .len()
        .ok_or_else(|| "yEnc part range is invalid".to_string())?;
    if length != decoded.data.len() as u64 {
        return Err("yEnc part range does not match the decoded payload".into());
    }
    limits.validate_file_size(decoded.range.end)?;
    if let Some(size) = decoded.file_size {
        limits.validate_file_size(size)?;
        if decoded.range.end > size {
            return Err("yEnc part exceeds the declared file size".into());
        }
    }
    Ok(length)
}

#[cfg(test)]
pub async fn assemble_file_with_report<S: ArticleSource>(
    output: &Path,
    segments: &[NzbSegment],
    source: &S,
    cancel: &CancellationToken,
    progress: Option<&AtomicU64>,
) -> Result<AssemblyReport, String> {
    let mut budget = YencAssemblyBudget::default();
    assemble_file_with_report_with_limits(
        output,
        segments,
        source,
        cancel,
        progress,
        YencAssemblyLimits::default(),
        &mut budget,
    )
    .await
}

#[cfg(test)]
pub async fn assemble_file_with_report_with_limits<S: ArticleSource>(
    output: &Path,
    segments: &[NzbSegment],
    source: &S,
    cancel: &CancellationToken,
    progress: Option<&AtomicU64>,
    limits: YencAssemblyLimits,
    budget: &mut YencAssemblyBudget,
) -> Result<AssemblyReport, String> {
    assemble_file_with_report_with_limits_at_offset(
        output, segments, source, cancel, progress, 0, limits, budget,
    )
    .await
}

pub(crate) async fn assemble_file_with_report_with_limits_at_offset<S: ArticleSource>(
    output: &Path,
    segments: &[NzbSegment],
    source: &S,
    cancel: &CancellationToken,
    progress: Option<&AtomicU64>,
    progress_base: u64,
    limits: YencAssemblyLimits,
    budget: &mut YencAssemblyBudget,
) -> Result<AssemblyReport, String> {
    if segments.is_empty() {
        return Err("cannot assemble a file with no segments".into());
    }
    let mut ordered = segments.to_vec();
    ordered.sort_by_key(|segment| segment.number);
    if ordered
        .windows(2)
        .any(|pair| pair[0].number == pair[1].number)
    {
        return Err("NZB file contains duplicate segment numbers".into());
    }
    let fingerprint = manifest_sha256(&ordered);
    let sidecar_path = resume_sidecar_path(output);
    let part_path = partial_path(output);
    let mut sidecar = ResumeSidecar::load(&sidecar_path, &fingerprint).await?;
    let mut size_reserved = false;
    if let Some(size) = sidecar.expected_size {
        limits.reserve_file(budget, size)?;
        size_reserved = true;
    }

    match tokio::fs::metadata(output).await {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err("finalized Usenet output is not a regular file".into());
            }
            limits.validate_file_size(metadata.len())?;
            if sidecar.repaired {
                if sidecar.expected_size != Some(metadata.len()) {
                    return Err("repaired Usenet output does not match its resume metadata".into());
                }
                publish_assembly_progress(progress, progress_base, total_article_bytes(&ordered)?);
                return Ok(assembly_report(
                    output,
                    &part_path,
                    &sidecar_path,
                    &fingerprint,
                    &sidecar,
                    Vec::new(),
                    true,
                ));
            }

            validate_existing_receipts(output, &mut sidecar, limits).await?;
            if has_complete_receipt_coverage(&ordered, &sidecar, metadata.len()) {
                publish_assembly_progress(
                    progress,
                    progress_base,
                    completed_article_bytes(&ordered, &sidecar.completed_segments)?,
                );
                return Ok(assembly_report(
                    output,
                    &part_path,
                    &sidecar_path,
                    &fingerprint,
                    &sidecar,
                    Vec::new(),
                    true,
                ));
            }
            match tokio::fs::symlink_metadata(&part_path).await {
                Ok(part_metadata) if part_metadata.file_type().is_file() => {
                    tokio::fs::remove_file(output)
                        .await
                        .map_err(|error| format!("discard uncommitted Usenet output: {error}"))?;
                }
                Ok(_) => return Err("assembled part is not a regular file".into()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(
                        "finalized Usenet output does not match its resume receipts and has no resumable part"
                            .into(),
                    );
                }
                Err(error) => return Err(format!("read assembled part metadata: {error}")),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("read finalized Usenet output metadata: {error}")),
    }

    sidecar.repaired = false;
    if let Some(parent) = part_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("create output directory: {error}"))?;
    }
    validate_existing_receipts(&part_path, &mut sidecar, limits).await?;
    if !size_reserved {
        if let Some(size) = sidecar.expected_size {
            limits.reserve_file(budget, size)?;
        }
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&part_path)
        .await
        .map_err(|error| format!("open assembled part: {error}"))?;
    if let Some(size) = sidecar.expected_size {
        resize_assembled_part(&mut file, size, limits).await?;
    }
    let mut resized_expected_size = sidecar.expected_size;
    let mut bytes_since_checkpoint = 0u64;
    let mut last_checkpoint = Instant::now();
    let mut completed_receipt_bytes = sidecar.completed_bytes;
    let mut completed_article_bytes =
        completed_article_bytes(&ordered, &sidecar.completed_segments)?;
    publish_assembly_progress(progress, progress_base, completed_article_bytes);

    let mut unavailable_segments = Vec::new();

    async fn checkpoint(
        file: &mut tokio::fs::File,
        sidecar: &ResumeSidecar,
        sidecar_path: &Path,
    ) -> Result<(), String> {
        file.sync_data()
            .await
            .map_err(|error| format!("flush assembled part: {error}"))?;
        sidecar.save_atomic(sidecar_path).await
    }

    macro_rules! return_after_checkpoint {
        ($error:expr) => {{
            checkpoint(&mut file, &sidecar, &sidecar_path).await?;
            return Err($error);
        }};
    }

    let pending_segments = ordered
        .iter()
        .filter(|segment| !sidecar.completed_segments.contains(&segment.number))
        .collect::<Vec<_>>();
    let window = source.concurrency().clamp(1, MAX_FETCH_WINDOW);
    let mut queue = pending_segments.into_iter();
    let mut fetches = FuturesUnordered::new();
    let mut occupied = BTreeMap::<u64, (u64, u32)>::new();
    let mut invalid_receipts = Vec::new();
    for (&number, receipt) in &sidecar.segment_receipts {
        match receipt_range(receipt) {
            Some(range) => {
                occupied.insert(range.start, (range.end, number));
            }
            None => invalid_receipts.push(number),
        }
    }
    loop {
        while fetches.len() < window {
            let Some(segment) = queue.next() else {
                break;
            };
            let segment = segment.clone();
            let message_id = segment.message_id.clone();
            fetches.push(async move {
                let result = source.fetch(&message_id).await;
                (segment, result)
            });
        }
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return_after_checkpoint!("Download cancelled".into());
            }
            next = fetches.next() => next,
        };
        let Some((segment, fetch_result)) = next else {
            break;
        };
        {
            if cancel.is_cancelled() {
                return_after_checkpoint!("Download cancelled".into());
            }
            let decoded = match fetch_result {
                Ok(decoded) => decoded,
                Err(ArticleFetchError::Unavailable(_)) => {
                    unavailable_segments.push(segment.number);
                    continue;
                }
                Err(error) => return_after_checkpoint!(error.to_string()),
            };
            if ordered.len() > 1 && !decoded.has_explicit_range {
                return_after_checkpoint!(
                    "multipart NZB file has yEnc data without =ypart offsets".into()
                );
            }
            if ordered.len() > 1 && decoded.file_size.is_none() {
                return_after_checkpoint!(
                    "multipart NZB file has yEnc data without a declared total size".into()
                );
            }
            let receipt_length = match validate_decoded_part(&decoded, limits) {
                Ok(length) => length,
                Err(error) => return_after_checkpoint!(error),
            };
            let expected_size = match (sidecar.expected_size, decoded.file_size) {
                (Some(existing), Some(size)) if existing != size => {
                    return_after_checkpoint!("yEnc parts disagree about total file size".into())
                }
                (Some(existing), _) => existing,
                (None, Some(size)) => {
                    sidecar.expected_size = Some(size);
                    if let Err(error) = limits.reserve_file(budget, size) {
                        return_after_checkpoint!(error);
                    }
                    size
                }
                (None, None) if ordered.len() == 1 && decoded.range.start == 0 => {
                    let size = decoded.range.end;
                    sidecar.expected_size = Some(size);
                    if let Err(error) = limits.reserve_file(budget, size) {
                        return_after_checkpoint!(error);
                    }
                    size
                }
                (None, None) => {
                    return_after_checkpoint!(
                        "yEnc file size is ambiguous without a declared total size".into()
                    )
                }
            };
            if decoded.range.end > expected_size {
                return_after_checkpoint!("yEnc part exceeds the assembled file size".into());
            }
            let candidate_receipt = ResumeSegment {
                offset: decoded.range.start,
                length: receipt_length,
                sha256: hex::encode(Sha256::digest(&decoded.data)),
            };
            if overlaps_occupied(&occupied, &invalid_receipts, decoded.range, segment.number) {
                return_after_checkpoint!("yEnc part overlaps a completed segment".into());
            }
            if resized_expected_size != Some(expected_size) {
                if let Err(error) = resize_assembled_part(&mut file, expected_size, limits).await {
                    return_after_checkpoint!(error);
                }
                resized_expected_size = Some(expected_size);
            }
            if let Err(error) = file
                .seek(std::io::SeekFrom::Start(decoded.range.start))
                .await
            {
                return_after_checkpoint!(format!("seek assembled part: {error}"));
            }
            if let Err(error) = file.write_all(&decoded.data).await {
                return_after_checkpoint!(format!("write assembled part: {error}"));
            }
            occupied.insert(decoded.range.start, (decoded.range.end, segment.number));
            sidecar.completed_segments.insert(segment.number);
            sidecar
                .segment_receipts
                .insert(segment.number, candidate_receipt.clone());
            completed_receipt_bytes =
                match completed_receipt_bytes.checked_add(candidate_receipt.length) {
                    Some(bytes) => bytes,
                    None => return_after_checkpoint!("resume receipt byte count overflowed".into()),
                };
            completed_article_bytes = match completed_article_bytes.checked_add(segment.bytes) {
                Some(bytes) => bytes,
                None => return_after_checkpoint!("NZB article byte count overflowed".into()),
            };
            sidecar.completed_bytes = completed_receipt_bytes;
            bytes_since_checkpoint =
                bytes_since_checkpoint.saturating_add(candidate_receipt.length);
            if bytes_since_checkpoint >= CHECKPOINT_BYTE_INTERVAL
                || last_checkpoint.elapsed() >= CHECKPOINT_TIME_INTERVAL
            {
                checkpoint(&mut file, &sidecar, &sidecar_path).await?;
                bytes_since_checkpoint = 0;
                last_checkpoint = Instant::now();
            }
            publish_assembly_progress(progress, progress_base, completed_article_bytes);
        }
    }
    checkpoint(&mut file, &sidecar, &sidecar_path).await?;
    file.sync_all()
        .await
        .map_err(|error| format!("flush assembled part: {error}"))?;
    drop(file);

    let part_len = tokio::fs::metadata(&part_path)
        .await
        .map_err(|error| format!("read assembled part metadata: {error}"))?
        .len();
    let complete = has_complete_receipt_coverage(&ordered, &sidecar, part_len);
    if complete {
        tokio::fs::rename(&part_path, output)
            .await
            .map_err(|error| format!("finalize assembled file: {error}"))?;
    }
    Ok(assembly_report(
        output,
        &part_path,
        &sidecar_path,
        &fingerprint,
        &sidecar,
        unavailable_segments,
        complete,
    ))
}

fn publish_assembly_progress(
    progress: Option<&AtomicU64>,
    progress_base: u64,
    file_completed: u64,
) {
    if let Some(counter) = progress {
        counter.store(
            progress_base.saturating_add(file_completed),
            Ordering::Relaxed,
        );
    }
}

fn total_article_bytes(segments: &[NzbSegment]) -> Result<u64, String> {
    segments.iter().try_fold(0u64, |total, segment| {
        total
            .checked_add(segment.bytes)
            .ok_or_else(|| "NZB article byte count overflowed".to_string())
    })
}

fn completed_article_bytes(
    segments: &[NzbSegment],
    completed_segments: &BTreeSet<u32>,
) -> Result<u64, String> {
    segments
        .iter()
        .filter(|segment| completed_segments.contains(&segment.number))
        .try_fold(0u64, |total, segment| {
            total
                .checked_add(segment.bytes)
                .ok_or_else(|| "NZB article byte count overflowed".to_string())
        })
}

pub async fn mark_par2_repaired(report: &AssemblyReport) -> Result<(), String> {
    let metadata = tokio::fs::metadata(&report.output)
        .await
        .map_err(|error| format!("read repaired output metadata: {error}"))?;
    if !metadata.is_file() {
        return Err("repaired output is not a regular file".into());
    }
    let mut sidecar = ResumeSidecar::load(&report.sidecar_path, &report.manifest_sha256).await?;
    sidecar.expected_size = Some(metadata.len());
    sidecar.completed_segments.clear();
    sidecar.segment_receipts.clear();
    sidecar.completed_bytes = metadata.len();
    sidecar.repaired = true;
    sidecar.save_atomic(&report.sidecar_path).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn yenc_name_runs_to_end_of_line_and_empty_tokens_are_ignored() {
        let header = parse_yenc_header(
            b"=ybegin part=1 size=10 line= name=My Show part=2 size=3.mkv",
            "=ybegin",
        )
        .unwrap();
        assert_eq!(header.get("name").unwrap(), "My Show part=2 size=3.mkv");
        assert_eq!(header.get("part").unwrap(), "1");
        assert_eq!(header.get("size").unwrap(), "10");
        assert!(!header.contains_key("line"));
    }

    use super::*;
    use crate::traits::process_may_be_running;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::SystemTime;

    #[derive(Default)]
    struct FakeSource {
        responses: BTreeMap<String, Result<Vec<u8>, ArticleFetchError>>,
    }

    impl ArticleSource for FakeSource {
        fn fetch<'a>(
            &'a self,
            message_id: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<DecodedYencPart, ArticleFetchError>>
                    + Send
                    + 'a,
            >,
        > {
            let response =
                self.responses.get(message_id).cloned().unwrap_or_else(|| {
                    Err(ArticleFetchError::Failed("unexpected message ID".into()))
                });
            Box::pin(async move {
                response.and_then(|article| {
                    decode_yenc_part(&article).map_err(ArticleFetchError::Failed)
                })
            })
        }
    }

    struct StaticDecodedSource {
        part: DecodedYencPart,
    }

    impl ArticleSource for StaticDecodedSource {
        fn fetch<'a>(
            &'a self,
            _message_id: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<DecodedYencPart, ArticleFetchError>>
                    + Send
                    + 'a,
            >,
        > {
            let part = self.part.clone();
            Box::pin(async move { Ok(part) })
        }
    }

    struct ProgressInspectingSource<'progress> {
        responses: BTreeMap<String, Result<Vec<u8>, ArticleFetchError>>,
        progress: &'progress AtomicU64,
        progress_before_second_fetch: AtomicU64,
    }

    impl ArticleSource for ProgressInspectingSource<'_> {
        fn fetch<'a>(
            &'a self,
            message_id: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<DecodedYencPart, ArticleFetchError>>
                    + Send
                    + 'a,
            >,
        > {
            if message_id == "two" {
                self.progress_before_second_fetch
                    .store(self.progress.load(Ordering::Relaxed), Ordering::Relaxed);
            }
            let response =
                self.responses.get(message_id).cloned().unwrap_or_else(|| {
                    Err(ArticleFetchError::Failed("unexpected message ID".into()))
                });
            Box::pin(async move {
                response.and_then(|article| {
                    decode_yenc_part(&article).map_err(ArticleFetchError::Failed)
                })
            })
        }
    }

    struct ConcurrencyTrackingSource {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        part_size: u64,
    }

    impl ArticleSource for ConcurrencyTrackingSource {
        fn fetch<'a>(
            &'a self,
            message_id: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<DecodedYencPart, ArticleFetchError>>
                    + Send
                    + 'a,
            >,
        > {
            let index = message_id.parse::<u64>().unwrap();
            let active = Arc::clone(&self.active);
            let max_active = Arc::clone(&self.max_active);
            let part_size = self.part_size;
            Box::pin(async move {
                let current = active.fetch_add(1, Ordering::Relaxed) + 1;
                let mut observed = max_active.load(Ordering::Relaxed);
                while current > observed {
                    match max_active.compare_exchange_weak(
                        observed,
                        current,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(next) => observed = next,
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                active.fetch_sub(1, Ordering::Relaxed);
                let start = index * part_size;
                Ok(DecodedYencPart {
                    data: vec![b'A' + index as u8; part_size as usize],
                    range: YencRange {
                        start,
                        end: start + part_size,
                    },
                    file_size: Some(part_size * 8),
                    has_explicit_range: true,
                })
            })
        }
    }

    fn encode_yenc_part(payload: &[u8], begin: u64, total_size: u64) -> Vec<u8> {
        let end = begin + payload.len() as u64 - 1;
        let mut encoded = format!(
            "=ybegin part=1 total=3 line=128 size={total_size} name=x\r\n=ypart begin={begin} end={end}\r\n"
        )
        .into_bytes();
        for &byte in payload {
            let value = byte.wrapping_add(42);
            if matches!(value, 0 | 10 | 13 | 61) {
                encoded.push(b'=');
                encoded.push(value.wrapping_add(64));
            } else {
                encoded.push(value);
            }
        }
        encoded.extend_from_slice(
            format!(
                "\r\n=yend size={} pcrc32={:08x}\r\n",
                payload.len(),
                crc32(payload)
            )
            .as_bytes(),
        );
        encoded
    }

    #[test]
    fn decodes_yenc_escapes() {
        let payload = b"a\n=\r\0z";
        let encoded = encode_yenc_part(payload, 1, payload.len() as u64);
        assert_eq!(decode_yenc_part(&encoded).unwrap().data, payload);
    }

    #[test]
    fn rejects_multipart_yenc_without_placement() {
        let article = b"=ybegin part=1 total=2 size=2 name=x\r\nkl\r\n=yend size=2\r\n";
        assert!(decode_yenc_part(article).is_err());
    }

    #[test]
    fn rejects_multipart_yenc_without_a_declared_total_size() {
        let article = b"=ybegin part=1 total=2 line=128 name=x\r\n=ypart begin=1 end=2\r\nkl\r\n=yend size=2\r\n";
        assert!(decode_yenc_part(article).is_err());
    }

    #[test]
    fn manifest_is_stable() {
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 3,
            message_id: "<a>".into(),
        }];
        assert_eq!(manifest_sha256(&segments).len(), 64);
    }

    #[tokio::test]
    async fn resume_sidecar_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.json");
        let mut state = ResumeSidecar::new("abc".into());
        state.completed_segments.insert(2);
        state.segment_receipts.insert(
            2,
            ResumeSegment {
                offset: 0,
                length: 9,
                sha256: "x".into(),
            },
        );
        state.completed_bytes = 9;
        state.save_atomic(&path).await.unwrap();
        assert_eq!(ResumeSidecar::load(&path, "abc").await.unwrap(), state);
        assert_eq!(
            ResumeSidecar::load(&path, "changed")
                .await
                .unwrap()
                .completed_bytes,
            0
        );
    }

    #[tokio::test]
    async fn failed_resume_save_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("existing-directory");
        std::fs::create_dir(&target).unwrap();

        let state = ResumeSidecar::new("abc".into());
        assert!(state.save_atomic(&target).await.is_err());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name())
            .collect();
        assert_eq!(leftovers, [std::ffi::OsString::from("existing-directory")]);
    }

    #[test]
    fn resume_prune_is_throttled_per_directory() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        assert!(should_prune_resume_temps(first.path()));
        assert!(should_prune_resume_temps(second.path()));
        assert!(!should_prune_resume_temps(first.path()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn loading_a_resume_sidecar_prunes_only_dead_process_temps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.json");
        let mut dead_process = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let dead_pid = dead_process.id();
        dead_process.wait().unwrap();
        assert!(!process_may_be_running(dead_pid));
        let stale = dir.path().join(format!(".risuko-atomic-{dead_pid}-a.tmp"));
        let recent = dir.path().join(format!(".risuko-atomic-{dead_pid}-b.tmp"));
        let active = dir
            .path()
            .join(format!(".risuko-atomic-{}-c.tmp", std::process::id()));
        let unrelated = dir.path().join("resume.json.backup.tmp");
        for file in [&stale, &recent, &active, &unrelated] {
            tokio::fs::write(file, b"x").await.unwrap();
        }
        let stale_time = std::fs::FileTimes::new()
            .set_modified(SystemTime::now() - Duration::from_secs(25 * 60 * 60));
        for candidate in [&stale, &active] {
            std::fs::File::open(candidate)
                .unwrap()
                .set_times(stale_time)
                .unwrap();
        }

        ResumeSidecar::load(&path, "manifest").await.unwrap();

        assert!(!stale.exists());
        assert!(recent.exists());
        assert!(active.exists());
        assert!(unrelated.exists());
    }

    #[tokio::test]
    async fn malformed_resume_metadata_starts_a_fresh_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.json");
        tokio::fs::write(&path, b"{truncated").await.unwrap();

        assert_eq!(
            ResumeSidecar::load(&path, "manifest").await.unwrap(),
            ResumeSidecar::new("manifest".into())
        );
    }

    #[tokio::test]
    async fn keeps_sparse_holes_when_a_middle_article_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let mut source = FakeSource::default();
        source
            .responses
            .insert("one".into(), Ok(encode_yenc_part(b"AB", 1, 6)));
        source.responses.insert(
            "two".into(),
            Err(ArticleFetchError::Unavailable("430".into())),
        );
        source
            .responses
            .insert("three".into(), Ok(encode_yenc_part(b"EF", 5, 6)));
        let segments = vec![
            NzbSegment {
                number: 1,
                bytes: 2,
                message_id: "one".into(),
            },
            NzbSegment {
                number: 2,
                bytes: 2,
                message_id: "two".into(),
            },
            NzbSegment {
                number: 3,
                bytes: 2,
                message_id: "three".into(),
            },
        ];

        let report =
            assemble_file_with_report(&output, &segments, &source, &CancellationToken::new(), None)
                .await
                .unwrap();

        assert!(!report.complete);
        assert_eq!(report.unavailable_segments, vec![2]);
        assert_eq!(
            tokio::fs::read(report.part_path).await.unwrap(),
            b"AB\0\0EF"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn publishes_nzb_article_bytes_at_the_task_progress_offset() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let progress = AtomicU64::new(7);
        let mut responses = BTreeMap::new();
        responses.insert("one".into(), Ok(encode_yenc_part(b"AB", 1, 4)));
        responses.insert("two".into(), Ok(encode_yenc_part(b"CD", 3, 4)));
        let source = ProgressInspectingSource {
            responses,
            progress: &progress,
            progress_before_second_fetch: AtomicU64::new(0),
        };
        let segments = vec![
            NzbSegment {
                number: 1,
                bytes: 17,
                message_id: "one".into(),
            },
            NzbSegment {
                number: 2,
                bytes: 19,
                message_id: "two".into(),
            },
        ];
        let mut budget = YencAssemblyBudget::default();

        let report = assemble_file_with_report_with_limits_at_offset(
            &output,
            &segments,
            &source,
            &CancellationToken::new(),
            Some(&progress),
            7,
            YencAssemblyLimits::new(4, 4).unwrap(),
            &mut budget,
        )
        .await
        .unwrap();

        assert!(report.complete);
        assert_eq!(
            source.progress_before_second_fetch.load(Ordering::Relaxed),
            24
        );
        assert_eq!(progress.load(Ordering::Relaxed), 43);
    }

    #[tokio::test]
    async fn bounds_fetches_when_manifest_counts_are_too_small() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let source = ConcurrencyTrackingSource {
            active: Arc::clone(&active),
            max_active: Arc::clone(&max_active),
            part_size: 1024,
        };
        let segments = (0..8)
            .map(|index| NzbSegment {
                number: index as u32 + 1,
                bytes: 1,
                message_id: index.to_string(),
            })
            .collect::<Vec<_>>();

        let report =
            assemble_file_with_report(&output, &segments, &source, &CancellationToken::new(), None)
                .await
                .unwrap();

        assert!(report.complete);
        assert_eq!(tokio::fs::metadata(&output).await.unwrap().len(), 8 * 1024);
        let max_active = max_active.load(Ordering::Relaxed);
        assert!(max_active > 1);
        assert!(max_active <= FETCH_CONCURRENCY);
    }

    #[tokio::test]
    async fn restores_durable_article_progress_at_the_task_offset() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let segments = vec![
            NzbSegment {
                number: 1,
                bytes: 17,
                message_id: "one".into(),
            },
            NzbSegment {
                number: 2,
                bytes: 19,
                message_id: "two".into(),
            },
        ];
        tokio::fs::write(partial_path(&output), b"AB\0\0")
            .await
            .unwrap();
        let mut sidecar = ResumeSidecar::new(manifest_sha256(&segments));
        sidecar.expected_size = Some(4);
        sidecar.completed_segments.insert(1);
        sidecar.segment_receipts.insert(
            1,
            ResumeSegment {
                offset: 0,
                length: 2,
                sha256: hex::encode(Sha256::digest(b"AB")),
            },
        );
        sidecar.completed_bytes = 2;
        sidecar
            .save_atomic(&resume_sidecar_path(&output))
            .await
            .unwrap();

        let progress = AtomicU64::new(7);
        let mut responses = BTreeMap::new();
        responses.insert("two".into(), Ok(encode_yenc_part(b"CD", 3, 4)));
        let source = ProgressInspectingSource {
            responses,
            progress: &progress,
            progress_before_second_fetch: AtomicU64::new(0),
        };
        let mut budget = YencAssemblyBudget::default();

        let report = assemble_file_with_report_with_limits_at_offset(
            &output,
            &segments,
            &source,
            &CancellationToken::new(),
            Some(&progress),
            7,
            YencAssemblyLimits::new(4, 4).unwrap(),
            &mut budget,
        )
        .await
        .unwrap();

        assert!(report.complete);
        assert_eq!(
            source.progress_before_second_fetch.load(Ordering::Relaxed),
            24
        );
        assert_eq!(progress.load(Ordering::Relaxed), 43);
    }

    #[tokio::test]
    async fn does_not_finalize_when_every_segment_receipt_leaves_a_sparse_gap() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let mut source = FakeSource::default();
        source
            .responses
            .insert("one".into(), Ok(encode_yenc_part(b"AB", 1, 6)));
        source
            .responses
            .insert("two".into(), Ok(encode_yenc_part(b"EF", 5, 6)));
        let segments = vec![
            NzbSegment {
                number: 1,
                bytes: 2,
                message_id: "one".into(),
            },
            NzbSegment {
                number: 2,
                bytes: 2,
                message_id: "two".into(),
            },
        ];

        let report =
            assemble_file_with_report(&output, &segments, &source, &CancellationToken::new(), None)
                .await
                .unwrap();

        assert!(!report.complete);
        assert!(report.unavailable_segments.is_empty());
        assert_eq!(
            tokio::fs::read(report.part_path).await.unwrap(),
            b"AB\0\0EF"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn rejects_an_oversized_declared_file_before_resizing_a_sparse_part() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let source = StaticDecodedSource {
            part: DecodedYencPart {
                data: b"A".to_vec(),
                range: YencRange { start: 0, end: 1 },
                file_size: Some(7),
                has_explicit_range: false,
            },
        };
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 1,
            message_id: "one".into(),
        }];
        let limits = YencAssemblyLimits::new(6, 12).unwrap();
        let mut budget = YencAssemblyBudget::default();

        let error = assemble_file_with_report_with_limits(
            &output,
            &segments,
            &source,
            &CancellationToken::new(),
            None,
            limits,
            &mut budget,
        )
        .await
        .unwrap_err();

        assert!(error.contains("per-file limit"));
        assert_eq!(
            tokio::fs::metadata(partial_path(&output))
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn rejects_an_oversized_yenc_offset_before_resizing_a_sparse_part() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let source = StaticDecodedSource {
            part: DecodedYencPart {
                data: b"A".to_vec(),
                range: YencRange { start: 6, end: 7 },
                file_size: None,
                has_explicit_range: true,
            },
        };
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 1,
            message_id: "one".into(),
        }];
        let limits = YencAssemblyLimits::new(6, 12).unwrap();
        let mut budget = YencAssemblyBudget::default();

        let error = assemble_file_with_report_with_limits(
            &output,
            &segments,
            &source,
            &CancellationToken::new(),
            None,
            limits,
            &mut budget,
        )
        .await
        .unwrap_err();

        assert!(error.contains("per-file limit"));
        assert_eq!(
            tokio::fs::metadata(partial_path(&output))
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn shared_budget_rejects_files_that_exceed_the_task_limit() {
        let dir = tempfile::tempdir().unwrap();
        let first_output = dir.path().join("first.bin");
        let second_output = dir.path().join("second.bin");
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 4,
            message_id: "one".into(),
        }];
        let limits = YencAssemblyLimits::new(6, 6).unwrap();
        let mut budget = YencAssemblyBudget::default();
        let first_source = StaticDecodedSource {
            part: DecodedYencPart {
                data: b"ABCD".to_vec(),
                range: YencRange { start: 0, end: 4 },
                file_size: Some(4),
                has_explicit_range: false,
            },
        };
        let second_source = StaticDecodedSource {
            part: DecodedYencPart {
                data: b"EFGH".to_vec(),
                range: YencRange { start: 0, end: 4 },
                file_size: Some(4),
                has_explicit_range: false,
            },
        };

        assemble_file_with_report_with_limits(
            &first_output,
            &segments,
            &first_source,
            &CancellationToken::new(),
            None,
            limits,
            &mut budget,
        )
        .await
        .unwrap();
        let error = assemble_file_with_report_with_limits(
            &second_output,
            &segments,
            &second_source,
            &CancellationToken::new(),
            None,
            limits,
            &mut budget,
        )
        .await
        .unwrap_err();

        assert!(error.contains("task limit"));
        assert_eq!(budget.reserved_bytes(), 4);
    }

    #[tokio::test]
    async fn normally_finalized_output_is_recognized_without_refetching_segments() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 2,
            message_id: "one".into(),
        }];
        let mut source = FakeSource::default();
        source
            .responses
            .insert("one".into(), Ok(encode_yenc_part(b"OK", 1, 2)));

        let initial =
            assemble_file_with_report(&output, &segments, &source, &CancellationToken::new(), None)
                .await
                .unwrap();
        assert!(initial.complete);

        let resumed = assemble_file_with_report(
            &output,
            &segments,
            &FakeSource::default(),
            &CancellationToken::new(),
            None,
        )
        .await
        .unwrap();

        assert!(resumed.complete);
    }

    #[tokio::test]
    async fn stale_promoted_output_reenters_the_repairable_partial_state() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 2,
            message_id: "one".into(),
        }];
        let sidecar = ResumeSidecar::new(manifest_sha256(&segments));
        sidecar
            .save_atomic(&resume_sidecar_path(&output))
            .await
            .unwrap();
        tokio::fs::write(&output, b"OK").await.unwrap();
        tokio::fs::write(partial_path(&output), b"\0\0")
            .await
            .unwrap();
        let mut source = FakeSource::default();
        source.responses.insert(
            "one".into(),
            Err(ArticleFetchError::Unavailable("430".into())),
        );

        let report =
            assemble_file_with_report(&output, &segments, &source, &CancellationToken::new(), None)
                .await
                .unwrap();

        assert!(!report.complete);
        assert_eq!(report.unavailable_segments, vec![1]);
        assert!(!output.exists());
        assert_eq!(
            tokio::fs::read(partial_path(&output)).await.unwrap(),
            b"\0\0"
        );
    }

    #[tokio::test]
    async fn repaired_output_is_recognized_without_refetching_segments() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("sample.bin");
        let segments = vec![NzbSegment {
            number: 1,
            bytes: 17,
            message_id: "one".into(),
        }];
        let initial = AssemblyReport {
            output: output.clone(),
            part_path: partial_path(&output),
            sidecar_path: resume_sidecar_path(&output),
            manifest_sha256: manifest_sha256(&segments),
            expected_size: Some(2),
            completed_bytes: 0,
            unavailable_segments: vec![1],
            complete: false,
        };
        let mut stale = ResumeSidecar::new(initial.manifest_sha256.clone());
        stale.completed_segments.insert(1);
        stale.segment_receipts.insert(
            1,
            ResumeSegment {
                offset: 0,
                length: 2,
                sha256: hex::encode(Sha256::digest(b"OK")),
            },
        );
        stale.save_atomic(&initial.sidecar_path).await.unwrap();
        tokio::fs::write(&output, b"OK").await.unwrap();
        mark_par2_repaired(&initial).await.unwrap();
        let persisted = ResumeSidecar::load(&initial.sidecar_path, &initial.manifest_sha256)
            .await
            .unwrap();
        assert!(persisted.completed_segments.is_empty());
        assert!(persisted.segment_receipts.is_empty());
        let progress = AtomicU64::new(0);
        let report = assemble_file_with_report(
            &output,
            &segments,
            &FakeSource::default(),
            &CancellationToken::new(),
            Some(&progress),
        )
        .await
        .unwrap();
        assert!(report.complete);
        assert_eq!(progress.load(Ordering::Relaxed), 17);
    }
}
