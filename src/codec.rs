//! Wire codecs for [`Message`].
//!
//! `Text` is the original whitespace-separated line format with percent
//! encoding for lock names. `Json` and `Msgpack` are serde-backed line formats;
//! MessagePack is hex-encoded so it can still ride on the line-oriented TCP
//! transport.

use crate::{Fence, Lamport, Message, NodeId, RequestId};

/// A line-oriented wire codec supported by the TCP transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireCodec {
    Text,
    Json,
    Msgpack,
}

impl WireCodec {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            "msgpack" | "messagepack" => Some(Self::Msgpack),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
            Self::Msgpack => "msgpack",
        }
    }

    pub fn encode_line(self, msg: &Message) -> String {
        match self {
            Self::Text => encode(msg),
            Self::Json => encode_json(msg),
            Self::Msgpack => hex_encode(&encode_msgpack(msg)),
        }
    }

    pub fn decode_line(self, line: &str) -> Option<Message> {
        match self {
            Self::Text => decode(line),
            Self::Json => decode_json(line),
            Self::Msgpack => decode_msgpack(&hex_decode(line)?),
        }
    }
}

/// Encode a message with the original whitespace-separated text format.
pub fn encode(msg: &Message) -> String {
    fn req(r: &RequestId) -> String {
        format!("{} {}", r.ts, r.node)
    }
    match msg {
        Message::Request { lock, req: q } => format!("REQUEST {} {}", encode_lock(lock), req(q)),
        Message::Grant {
            lock,
            req: q,
            fence,
        } => format!("GRANT {} {} {fence}", encode_lock(lock), req(q)),
        Message::Inquire { lock, req: q } => format!("INQUIRE {} {}", encode_lock(lock), req(q)),
        Message::Yield { lock, req: q } => format!("YIELD {} {}", encode_lock(lock), req(q)),
        Message::Confirm {
            lock,
            req: q,
            fence,
        } => format!("CONFIRM {} {} {fence}", encode_lock(lock), req(q)),
        Message::Release {
            lock,
            req: q,
            fence,
        } => format!("RELEASE {} {} {fence}", encode_lock(lock), req(q)),
        Message::Renew { lock, req: q } => format!("RENEW {} {}", encode_lock(lock), req(q)),
        Message::Revoked { lock, req: q } => format!("REVOKED {} {}", encode_lock(lock), req(q)),
    }
}

/// Decode the original text format, or `None` if it is malformed.
pub fn decode(line: &str) -> Option<Message> {
    let mut it = line.split_whitespace();
    let tag = it.next()?;
    let lock = decode_lock(it.next()?)?;
    let ts: Lamport = it.next()?.parse().ok()?;
    let node: NodeId = it.next()?.parse().ok()?;
    let req = RequestId { ts, node };
    let fence = |it: &mut std::str::SplitWhitespace| -> Option<Fence> { it.next()?.parse().ok() };
    let msg = match tag {
        "REQUEST" => Message::Request { lock, req },
        "GRANT" => Message::Grant {
            lock,
            req,
            fence: fence(&mut it)?,
        },
        "INQUIRE" => Message::Inquire { lock, req },
        "YIELD" => Message::Yield { lock, req },
        "CONFIRM" => Message::Confirm {
            lock,
            req,
            fence: fence(&mut it)?,
        },
        "RELEASE" => Message::Release {
            lock,
            req,
            fence: fence(&mut it)?,
        },
        "RENEW" => Message::Renew { lock, req },
        "REVOKED" => Message::Revoked { lock, req },
        _ => return None,
    };
    if it.next().is_some() {
        return None;
    }
    Some(msg)
}

/// Encode a message as one compact JSON value.
pub fn encode_json(msg: &Message) -> String {
    serde_json::to_string(msg).expect("protocol messages should always serialize")
}

/// Decode one JSON value into a message, or `None` if it is malformed.
pub fn decode_json(line: &str) -> Option<Message> {
    serde_json::from_str(line).ok()
}

/// Encode a message as MessagePack bytes.
pub fn encode_msgpack(msg: &Message) -> Vec<u8> {
    rmp_serde::to_vec(msg).expect("protocol messages should always serialize")
}

/// Decode MessagePack bytes into a message, or `None` if they are malformed.
pub fn decode_msgpack(bytes: &[u8]) -> Option<Message> {
    rmp_serde::from_slice(bytes).ok()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn encode_lock(lock: &str) -> String {
    let mut out = String::with_capacity(lock.len());
    for &byte in lock.as_bytes() {
        if byte.is_ascii_graphic() && byte != b'%' {
            out.push(byte as char);
        } else {
            push_hex_escape(&mut out, byte);
        }
    }
    out
}

fn decode_lock(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_value(bytes[i + 1])?;
            let lo = hex_value(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn push_hex_escape(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('%');
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0x0f) as usize] as char);
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in s.as_bytes().chunks_exact(2) {
        let hi = hex_value(pair[0])?;
        let lo = hex_value(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variants() -> Vec<Message> {
        let req = RequestId { ts: 7, node: 3 };
        vec![
            Message::Request {
                lock: "A".into(),
                req,
            },
            Message::Grant {
                lock: "lock-1".into(),
                req,
                fence: 42,
            },
            Message::Inquire {
                lock: "A".into(),
                req,
            },
            Message::Yield {
                lock: "A".into(),
                req,
            },
            Message::Confirm {
                lock: "A".into(),
                req,
                fence: 9,
            },
            Message::Release {
                lock: "A".into(),
                req,
                fence: 100,
            },
            Message::Renew {
                lock: "A".into(),
                req,
            },
            Message::Revoked {
                lock: "A".into(),
                req,
            },
        ]
    }

    #[test]
    fn all_variants_roundtrip_in_all_wire_codecs() {
        for codec in [WireCodec::Text, WireCodec::Json, WireCodec::Msgpack] {
            for msg in variants() {
                assert_eq!(codec.decode_line(&codec.encode_line(&msg)), Some(msg));
            }
        }
    }

    #[test]
    fn all_codecs_preserve_whitespace_in_lock_names() {
        let msg = Message::Request {
            lock: "tenant 7 / café % report A".into(),
            req: RequestId { ts: 1, node: 2 },
        };
        for codec in [WireCodec::Text, WireCodec::Json, WireCodec::Msgpack] {
            assert_eq!(
                codec.decode_line(&codec.encode_line(&msg)),
                Some(msg.clone())
            );
        }
    }

    #[test]
    fn malformed_payloads_are_none() {
        assert_eq!(decode(""), None);
        assert_eq!(decode("WAT A 1 2"), None);
        assert_eq!(decode("GRANT A 1 2"), None);
        assert_eq!(decode("REQUEST A 1 2 trailing"), None);
        assert_eq!(decode("REQUEST bad%xx 1 2"), None);
        assert_eq!(decode("REQUEST bad% 1 2"), None);
        assert_eq!(
            decode_json(r#"{"Request":{"lock":"A","req":{"ts":1,"node":2}}} trailing"#),
            None
        );
        assert_eq!(decode_msgpack(&[0xC1]), None);
        assert_eq!(WireCodec::Msgpack.decode_line("abc"), None);
        assert_eq!(WireCodec::Msgpack.decode_line("not-hex"), None);
    }

    #[test]
    fn parses_codec_names() {
        assert_eq!(WireCodec::parse("text"), Some(WireCodec::Text));
        assert_eq!(WireCodec::parse("TEXT"), Some(WireCodec::Text));
        assert_eq!(WireCodec::parse("json"), Some(WireCodec::Json));
        assert_eq!(WireCodec::parse("msgpack"), Some(WireCodec::Msgpack));
        assert_eq!(WireCodec::parse("messagepack"), Some(WireCodec::Msgpack));
        assert_eq!(WireCodec::parse("yaml"), None);
    }
}
