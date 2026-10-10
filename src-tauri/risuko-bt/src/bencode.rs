use std::collections::{BTreeMap, HashSet};
use std::io::{self, Write};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unexpected end of input at byte {0}")]
    UnexpectedEof(usize),
    #[error("unexpected byte {byte:#x} at position {pos}")]
    UnexpectedByte { byte: u8, pos: usize },
    #[error("integer parse error at position {pos}: {reason}")]
    BadInteger { pos: usize, reason: &'static str },
    #[error("byte string length parse error at position {pos}: {reason}")]
    BadLength { pos: usize, reason: &'static str },
    #[error("byte string too long at position {pos}: len={len}")]
    StringTooLong { pos: usize, len: u64 },
    #[error("dict keys out of order or duplicated at position {0}")]
    BadDictOrder(usize),
    #[error("dict key is not a byte string at position {0}")]
    NonStringDictKey(usize),
    #[error("trailing bytes after decoded value")]
    TrailingBytes,
    #[error("bencode input exceeds limit: len={len}, max={max}")]
    InputLimit { len: usize, max: usize },
    #[error("bencode nesting exceeds limit at position {pos}: max depth={max}")]
    DepthLimit { pos: usize, max: usize },
    #[error("bencode value count exceeds limit: max values={max}")]
    ValueLimit { max: usize },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(Vec<(Vec<u8>, Value)>),
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        if let Value::Int(v) = self {
            Some(*v)
        } else {
            None
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Value::Bytes(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }
    pub fn as_list(&self) -> Option<&[Value]> {
        if let Value::List(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_dict(&self) -> Option<&[(Vec<u8>, Value)]> {
        if let Value::Dict(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.as_dict()
            .and_then(|items| items.iter().find(|(k, _)| k == key).map(|(_, v)| v))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_input_len: usize,
    pub max_depth: usize,
    pub max_values: usize,
}

impl DecodeLimits {
    pub const fn new(max_input_len: usize, max_depth: usize, max_values: usize) -> Self {
        Self {
            max_input_len,
            max_depth,
            max_values,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Decoded {
    pub value: Value,
    pub span: std::ops::Range<usize>,
}

pub fn decode(input: &[u8]) -> Result<Decoded, Error> {
    let mut p = Parser::strict(input);
    p.parse_value(0)
}

pub fn decode_all(input: &[u8]) -> Result<Value, Error> {
    let d = decode(input)?;
    if d.span.end != input.len() {
        return Err(Error::TrailingBytes);
    }
    Ok(d.value)
}

pub fn decode_external(input: &[u8], limits: DecodeLimits) -> Result<Decoded, Error> {
    if input.len() > limits.max_input_len {
        return Err(Error::InputLimit {
            len: input.len(),
            max: limits.max_input_len,
        });
    }
    let mut p = Parser::external(input, limits);
    p.parse_value(0)
}

pub fn decode_all_external(input: &[u8], limits: DecodeLimits) -> Result<Value, Error> {
    let d = decode_external(input, limits)?;
    if !input[d.span.end..]
        .iter()
        .all(|byte| byte.is_ascii_whitespace())
    {
        return Err(Error::TrailingBytes);
    }
    Ok(d.value)
}

const MAX_STRICT_DEPTH: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq)]
enum DictMode {
    Canonical,
    Relaxed,
}

struct Parser<'a> {
    buf: &'a [u8],
    pos: usize,
    dict_mode: DictMode,
    limits: Option<DecodeLimits>,
    values: usize,
}

impl<'a> Parser<'a> {
    fn strict(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            dict_mode: DictMode::Canonical,
            limits: None,
            values: 0,
        }
    }

    fn external(buf: &'a [u8], limits: DecodeLimits) -> Self {
        Self {
            buf,
            pos: 0,
            dict_mode: DictMode::Relaxed,
            limits: Some(limits),
            values: 0,
        }
    }

    fn peek(&self) -> Result<u8, Error> {
        self.buf
            .get(self.pos)
            .copied()
            .ok_or(Error::UnexpectedEof(self.pos))
    }

    fn note_value(&mut self, depth: usize) -> Result<(), Error> {
        if let Some(limits) = self.limits {
            if depth > limits.max_depth {
                return Err(Error::DepthLimit {
                    pos: self.pos,
                    max: limits.max_depth,
                });
            }
            if self.values >= limits.max_values {
                return Err(Error::ValueLimit {
                    max: limits.max_values,
                });
            }
        } else if depth > MAX_STRICT_DEPTH {
            return Err(Error::DepthLimit {
                pos: self.pos,
                max: MAX_STRICT_DEPTH,
            });
        }
        self.values = self.values.saturating_add(1);
        Ok(())
    }

    fn parse_value(&mut self, depth: usize) -> Result<Decoded, Error> {
        self.note_value(depth)?;
        let start = self.pos;
        let b = self.peek()?;
        let value = match b {
            b'i' => self.parse_int()?,
            b'l' => self.parse_list(depth)?,
            b'd' => self.parse_dict(depth)?,
            b'0'..=b'9' => self.parse_bytes()?,
            other => {
                return Err(Error::UnexpectedByte {
                    byte: other,
                    pos: self.pos,
                });
            }
        };
        Ok(Decoded {
            value,
            span: start..self.pos,
        })
    }

    fn parse_int(&mut self) -> Result<Value, Error> {
        debug_assert_eq!(self.buf[self.pos], b'i');
        self.pos += 1;
        let start = self.pos;
        while self.peek()? != b'e' {
            self.pos += 1;
        }
        let digits = &self.buf[start..self.pos];
        self.pos += 1;
        if digits.is_empty() {
            return Err(Error::BadInteger {
                pos: start,
                reason: "empty",
            });
        }
        if digits[0] == b'+' {
            return Err(Error::BadInteger {
                pos: start,
                reason: "leading plus",
            });
        }
        if digits == b"-0" {
            return Err(Error::BadInteger {
                pos: start,
                reason: "negative zero",
            });
        }
        if digits.len() > 1 {
            let leading = if digits[0] == b'-' {
                &digits[1..]
            } else {
                digits
            };
            if leading.starts_with(b"0") {
                return Err(Error::BadInteger {
                    pos: start,
                    reason: "leading zero",
                });
            }
        }
        let s = std::str::from_utf8(digits).map_err(|_| Error::BadInteger {
            pos: start,
            reason: "non-ascii",
        })?;
        let n: i64 = s.parse().map_err(|_| Error::BadInteger {
            pos: start,
            reason: "out of range",
        })?;
        Ok(Value::Int(n))
    }

    fn parse_bytes(&mut self) -> Result<Value, Error> {
        let start = self.pos;
        while self.peek()? != b':' {
            self.pos += 1;
        }
        let digits = &self.buf[start..self.pos];
        self.pos += 1;
        if digits.len() > 1 && digits[0] == b'0' {
            return Err(Error::BadLength {
                pos: start,
                reason: "leading zero",
            });
        }
        let s = std::str::from_utf8(digits).map_err(|_| Error::BadLength {
            pos: start,
            reason: "non-ascii",
        })?;
        let len: u64 = s.parse().map_err(|_| Error::BadLength {
            pos: start,
            reason: "parse",
        })?;
        if len > (isize::MAX as u64) {
            return Err(Error::StringTooLong { pos: start, len });
        }
        let len = len as usize;
        let end = self.pos.checked_add(len).ok_or(Error::StringTooLong {
            pos: start,
            len: len as u64,
        })?;
        if end > self.buf.len() {
            return Err(Error::UnexpectedEof(self.buf.len()));
        }
        let bytes = self.buf[self.pos..end].to_vec();
        self.pos = end;
        Ok(Value::Bytes(bytes))
    }

    fn parse_list(&mut self, depth: usize) -> Result<Value, Error> {
        debug_assert_eq!(self.buf[self.pos], b'l');
        self.pos += 1;
        let mut items = Vec::new();
        while self.peek()? != b'e' {
            items.push(self.parse_value(depth.saturating_add(1))?.value);
        }
        self.pos += 1;
        Ok(Value::List(items))
    }

    fn parse_dict(&mut self, depth: usize) -> Result<Value, Error> {
        debug_assert_eq!(self.buf[self.pos], b'd');
        self.pos += 1;
        let mut items: Vec<(Vec<u8>, Value)> = Vec::new();
        let mut seen: Option<HashSet<Vec<u8>>> = None;
        while self.peek()? != b'e' {
            let key_pos = self.pos;
            let key_val = self.parse_value(depth.saturating_add(1))?.value;
            let key = match key_val {
                Value::Bytes(b) => b,
                _ => return Err(Error::NonStringDictKey(key_pos)),
            };
            if self.dict_mode == DictMode::Canonical {
                if let Some(set) = seen.as_mut() {
                    if !set.insert(key.clone()) {
                        return Err(Error::BadDictOrder(key_pos));
                    }
                } else if let Some((prev, _)) = items.last() {
                    if prev.as_slice() >= key.as_slice() {
                        let mut set: HashSet<Vec<u8>> =
                            items.iter().map(|(k, _)| k.clone()).collect();
                        if !set.insert(key.clone()) {
                            return Err(Error::BadDictOrder(key_pos));
                        }
                        seen = Some(set);
                    }
                }
            }
            let value = self.parse_value(depth.saturating_add(1))?.value;
            items.push((key, value));
        }
        self.pos += 1;
        Ok(Value::Dict(items))
    }
}

pub fn top_level_field_span(input: &[u8], key: &[u8]) -> Option<std::ops::Range<usize>> {
    fn skip(input: &[u8], pos: usize) -> Option<usize> {
        match *input.get(pos)? {
            b'i' => Some(pos + input.get(pos..)?.iter().position(|&b| b == b'e')? + 1),
            b'l' | b'd' => {
                let mut p = pos + 1;
                while *input.get(p)? != b'e' {
                    p = skip(input, p)?;
                }
                Some(p + 1)
            }
            b'0'..=b'9' => {
                let colon = pos + input.get(pos..)?.iter().position(|&b| b == b':')?;
                let len: usize = std::str::from_utf8(&input[pos..colon]).ok()?.parse().ok()?;
                let end = (colon + 1).checked_add(len)?;
                (end <= input.len()).then_some(end)
            }
            _ => None,
        }
    }
    if *input.first()? != b'd' {
        return None;
    }
    let mut p = 1;
    while *input.get(p)? != b'e' {
        let key_end = skip(input, p)?;
        let colon = p + input.get(p..)?.iter().position(|&b| b == b':')?;
        let found = input.get(colon + 1..key_end)? == key;
        let val_end = skip(input, key_end)?;
        if found {
            return Some(key_end..val_end);
        }
        p = val_end;
    }
    None
}

#[cfg(test)]
pub fn decode_dict_field_raw<'a>(
    input: &'a [u8],
    key: &[u8],
) -> Result<Option<(Value, &'a [u8])>, Error> {
    let mut p = Parser::strict(input);
    if p.peek()? != b'd' {
        return Err(Error::UnexpectedByte {
            byte: input[0],
            pos: 0,
        });
    }
    p.pos += 1;
    while p.peek()? != b'e' {
        let k = match p.parse_value(1)?.value {
            Value::Bytes(b) => b,
            _ => return Err(Error::NonStringDictKey(p.pos)),
        };
        let val_start = p.pos;
        let v = p.parse_value(1)?;
        if k == key {
            let raw = &input[val_start..v.span.end];
            return Ok(Some((v.value, raw)));
        }
    }
    Ok(None)
}

pub fn encode_to_vec(v: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    encode_to_writer(v, &mut out).expect("Vec write is infallible");
    out
}

pub fn encode_to_writer<W: Write>(v: &Value, w: &mut W) -> Result<(), Error> {
    match v {
        Value::Int(n) => {
            write!(w, "i{n}e")?;
        }
        Value::Bytes(b) => {
            write!(w, "{}:", b.len())?;
            w.write_all(b)?;
        }
        Value::List(items) => {
            w.write_all(b"l")?;
            for item in items {
                encode_to_writer(item, w)?;
            }
            w.write_all(b"e")?;
        }
        Value::Dict(items) => {
            let mut sorted: BTreeMap<&[u8], &Value> = BTreeMap::new();
            for (k, vv) in items {
                if sorted.insert(k.as_slice(), vv).is_some() {
                    return Err(Error::BadDictOrder(0));
                }
            }
            w.write_all(b"d")?;
            for (k, vv) in sorted {
                write!(w, "{}:", k.len())?;
                w.write_all(k)?;
                encode_to_writer(vv, w)?;
            }
            w.write_all(b"e")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_int() {
        assert_eq!(decode_all(b"i42e").unwrap(), Value::Int(42));
        assert_eq!(decode_all(b"i-7e").unwrap(), Value::Int(-7));
        assert_eq!(decode_all(b"i0e").unwrap(), Value::Int(0));
        assert!(decode_all(b"i-0e").is_err());
        assert!(decode_all(b"i01e").is_err());
        assert!(decode_all(b"ie").is_err());
    }

    #[test]
    fn decode_bytes() {
        assert_eq!(
            decode_all(b"4:spam").unwrap(),
            Value::Bytes(b"spam".to_vec())
        );
        assert_eq!(decode_all(b"0:").unwrap(), Value::Bytes(vec![]));
        assert!(decode_all(b"5:abc").is_err());
    }

    #[test]
    fn decode_list_and_dict() {
        let v = decode_all(b"l4:spami42ee").unwrap();
        assert_eq!(
            v,
            Value::List(vec![Value::Bytes(b"spam".to_vec()), Value::Int(42)])
        );
        let d = decode_all(b"d3:bar4:spam3:fooi42ee").unwrap();
        assert_eq!(
            d,
            Value::Dict(vec![
                (b"bar".to_vec(), Value::Bytes(b"spam".to_vec())),
                (b"foo".to_vec(), Value::Int(42)),
            ])
        );
    }

    #[test]
    fn reject_duplicate_keys_accept_unsorted() {
        assert!(decode_all(b"d3:fooi1e3:fooi2ee").is_err());
        assert!(decode_all(b"d3:fooi1e3:bari2e3:fooi3ee").is_err());
        let v = decode_all(b"d3:fooi1e3:bari2ee").unwrap();
        assert_eq!(v.get(b"bar").and_then(Value::as_int), Some(2));
    }

    #[test]
    fn external_decode_accepts_unsorted_and_duplicate_keys_first_wins() {
        let limits = DecodeLimits::new(128, 8, 32);
        let value = decode_all_external(b"d3:fooi1e3:bari2e3:fooi3ee", limits).unwrap();
        let items = value.as_dict().unwrap();

        assert_eq!(items.len(), 3);
        assert_eq!(value.get(b"foo").and_then(Value::as_int), Some(1));
    }

    #[test]
    fn external_decode_allows_only_whitespace_tail() {
        let limits = DecodeLimits::new(128, 8, 32);

        assert_eq!(
            decode_all_external(b"i7e \t\n", limits).unwrap(),
            Value::Int(7)
        );
        assert!(matches!(
            decode_all_external(b"i7ejunk", limits),
            Err(Error::TrailingBytes)
        ));
    }

    #[test]
    fn strict_decode_rejects_deep_nesting() {
        let deep = [b"l".repeat(100_000), b"e".repeat(100_000)].concat();
        assert!(matches!(decode_all(&deep), Err(Error::DepthLimit { .. })));
        let ok = [b"l".repeat(200), b"e".repeat(200)].concat();
        assert!(decode_all(&ok).is_ok());
    }

    #[test]
    fn external_decode_enforces_input_depth_and_value_limits() {
        assert!(matches!(
            decode_external(b"4:spam", DecodeLimits::new(5, 8, 8)),
            Err(Error::InputLimit { len: 6, max: 5 })
        ));
        assert!(matches!(
            decode_all_external(b"lli1eee", DecodeLimits::new(16, 1, 8)),
            Err(Error::DepthLimit { max: 1, .. })
        ));
        assert!(matches!(
            decode_all_external(b"li1ei2ee", DecodeLimits::new(16, 8, 2)),
            Err(Error::ValueLimit { max: 2 })
        ));
    }

    #[test]
    fn external_decode_reports_consumed_header_before_binary_tail() {
        let limits = DecodeLimits::new(128, 8, 32);
        let decoded = decode_external(b"d1:ai1ee\0\xff", limits).unwrap();

        assert_eq!(decoded.span, 0..8);
        assert_eq!(decoded.value.get(b"a").and_then(Value::as_int), Some(1));
    }

    #[test]
    fn encode_round_trip_sorts_dicts() {
        let v = Value::Dict(vec![
            (b"zeta".to_vec(), Value::Int(1)),
            (b"alpha".to_vec(), Value::Int(2)),
        ]);
        let encoded = encode_to_vec(&v);
        assert_eq!(encoded, b"d5:alphai2e4:zetai1ee");
        let round = decode_all(&encoded).unwrap();
        assert_eq!(
            round,
            Value::Dict(vec![
                (b"alpha".to_vec(), Value::Int(2)),
                (b"zeta".to_vec(), Value::Int(1)),
            ])
        );
    }

    #[test]
    fn raw_field_span() {
        let doc = b"d4:infod6:pieces4:aaaae4:name4:abcde";
        let (v, raw) = decode_dict_field_raw(doc, b"info").unwrap().unwrap();
        assert!(matches!(v, Value::Dict(_)));
        assert_eq!(raw, b"d6:pieces4:aaaae");
    }

    #[test]
    fn decode_records_spans() {
        let input = b"l4:spami42ee";
        let d = decode(input).unwrap();
        assert_eq!(d.span, 0..input.len());
    }

    #[test]
    fn field_span_matches_raw_decode() {
        let input = b"d3:bar4:spam4:infod1:ai1e1:bl3:xyzee3:zzzi5ee";
        let span = top_level_field_span(input, b"info").unwrap();
        let (_, raw) = decode_dict_field_raw(input, b"info").unwrap().unwrap();
        assert_eq!(&input[span], raw);
        assert!(top_level_field_span(input, b"nope").is_none());
    }
}
