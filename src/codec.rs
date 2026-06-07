//! A tiny, dependency-free line codec for [`Message`] — one message per line,
//! whitespace-separated. Lock names must not contain whitespace.
//!
//! Format: `<TAG> <lock> <ts> <node> [fence]`

use crate::{Fence, Lamport, Message, NodeId, RequestId};

/// Encode a message as a single line (no trailing newline).
pub fn encode(msg: &Message) -> String {
    fn req(r: &RequestId) -> String {
        format!("{} {}", r.ts, r.node)
    }
    match msg {
        Message::Request { lock, req: q } => format!("REQUEST {lock} {}", req(q)),
        Message::Grant { lock, req: q, fence } => format!("GRANT {lock} {} {fence}", req(q)),
        Message::Inquire { lock, req: q } => format!("INQUIRE {lock} {}", req(q)),
        Message::Yield { lock, req: q } => format!("YIELD {lock} {}", req(q)),
        Message::Confirm { lock, req: q, fence } => format!("CONFIRM {lock} {} {fence}", req(q)),
        Message::Release { lock, req: q, fence } => format!("RELEASE {lock} {} {fence}", req(q)),
        Message::Renew { lock, req: q } => format!("RENEW {lock} {}", req(q)),
        Message::Revoked { lock, req: q } => format!("REVOKED {lock} {}", req(q)),
    }
}

/// Decode a line into a message, or `None` if it is malformed.
pub fn decode(line: &str) -> Option<Message> {
    let mut it = line.split_whitespace();
    let tag = it.next()?;
    let lock = it.next()?.to_string();
    let ts: Lamport = it.next()?.parse().ok()?;
    let node: NodeId = it.next()?.parse().ok()?;
    let req = RequestId { ts, node };
    let fence = |it: &mut std::str::SplitWhitespace| -> Option<Fence> { it.next()?.parse().ok() };
    let msg = match tag {
        "REQUEST" => Message::Request { lock, req },
        "GRANT" => Message::Grant { lock, req, fence: fence(&mut it)? },
        "INQUIRE" => Message::Inquire { lock, req },
        "YIELD" => Message::Yield { lock, req },
        "CONFIRM" => Message::Confirm { lock, req, fence: fence(&mut it)? },
        "RELEASE" => Message::Release { lock, req, fence: fence(&mut it)? },
        "RENEW" => Message::Renew { lock, req },
        "REVOKED" => Message::Revoked { lock, req },
        _ => return None,
    };
    Some(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(m: Message) {
        assert_eq!(decode(&encode(&m)), Some(m));
    }

    #[test]
    fn all_variants_roundtrip() {
        let req = RequestId { ts: 7, node: 3 };
        roundtrip(Message::Request { lock: "A".into(), req });
        roundtrip(Message::Grant { lock: "lock-1".into(), req, fence: 42 });
        roundtrip(Message::Inquire { lock: "A".into(), req });
        roundtrip(Message::Yield { lock: "A".into(), req });
        roundtrip(Message::Confirm { lock: "A".into(), req, fence: 9 });
        roundtrip(Message::Release { lock: "A".into(), req, fence: 100 });
        roundtrip(Message::Renew { lock: "A".into(), req });
        roundtrip(Message::Revoked { lock: "A".into(), req });
    }

    #[test]
    fn garbage_is_none() {
        assert_eq!(decode(""), None);
        assert_eq!(decode("WAT A 1 2"), None);
        assert_eq!(decode("GRANT A 1 2"), None); // missing fence
    }
}
