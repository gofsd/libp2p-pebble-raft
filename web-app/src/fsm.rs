//! Byte-compatible reimplementation of `pkg/kvfsm`'s raft log command
//! encoding and `pkg/store`'s snapshot dump/restore framing -- what lets
//! this crate's FSM apply the exact same log entries a Go voter's FSM
//! would, and restore from the exact same `InstallSnapshot` byte stream a
//! Go leader sends.

/// Matches `pkg/kvfsm.OpType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Set = 1,
    Del = 2,
}

impl Op {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Op::Set),
            2 => Some(Op::Del),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub op: Op,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Decodes a raft `Log.Data` payload produced by `pkg/kvfsm.EncodeCommand`:
/// `[1 byte op][4 byte big-endian key len][key][4 byte big-endian value
/// len][value]`.
pub fn decode_command(data: &[u8]) -> Result<Command, &'static str> {
    if data.len() < 5 {
        return Err("fsm: command too short");
    }
    let op = Op::from_byte(data[0]).ok_or("fsm: unknown op")?;
    let klen = u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize;
    let mut off = 5;
    if data.len() < off + klen {
        return Err("fsm: truncated key");
    }
    let key = data[off..off + klen].to_vec();
    off += klen;
    if data.len() < off + 4 {
        return Err("fsm: missing value length");
    }
    let vlen = u32::from_be_bytes(data[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    if data.len() < off + vlen {
        return Err("fsm: truncated value");
    }
    let value = data[off..off + vlen].to_vec();
    Ok(Command { op, key, value })
}

/// Matches `pkg/kvfsm.EncodeCommand` -- not needed by the learner itself
/// (it never originates log entries), but kept for symmetry and unit
/// testing against the same round trip the Go side relies on.
#[cfg(test)]
pub fn encode_command(op: Op, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    buf.push(op as u8);
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    buf
}

/// One key/value pair as read from a `pkg/store.DumpAll`-formatted
/// snapshot stream (an `InstallSnapshotRequest`'s raw body): `[4-byte
/// big-endian key length][key][4-byte big-endian value length][value]`,
/// repeated until the stream (bounded by `InstallSnapshotRequest.Size`) is
/// exhausted.
pub fn decode_snapshot_entries(mut data: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, &'static str> {
    let mut out = Vec::new();
    while !data.is_empty() {
        if data.len() < 4 {
            return Err("fsm: truncated snapshot key length");
        }
        let klen = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        data = &data[4..];
        if data.len() < klen {
            return Err("fsm: truncated snapshot key");
        }
        let key = data[..klen].to_vec();
        data = &data[klen..];
        if data.len() < 4 {
            return Err("fsm: truncated snapshot value length");
        }
        let vlen = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        data = &data[4..];
        if data.len() < vlen {
            return Err("fsm: truncated snapshot value");
        }
        let value = data[..vlen].to_vec();
        data = &data[vlen..];
        out.push((key, value));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_roundtrip() {
        let encoded = encode_command(Op::Set, b"hello", b"world");
        let decoded = decode_command(&encoded).unwrap();
        assert_eq!(
            decoded,
            Command {
                op: Op::Set,
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
            }
        );
    }

    #[test]
    fn command_empty_value() {
        let encoded = encode_command(Op::Del, b"gone", b"");
        let decoded = decode_command(&encoded).unwrap();
        assert_eq!(decoded.op, Op::Del);
        assert_eq!(decoded.key, b"gone");
        assert!(decoded.value.is_empty());
    }

    #[test]
    fn command_rejects_truncated() {
        assert!(decode_command(&[1, 0, 0, 0, 5]).is_err());
        assert!(decode_command(&[]).is_err());
    }

    #[test]
    fn snapshot_entries_roundtrip() {
        let mut buf = Vec::new();
        for (k, v) in [(&b"a"[..], &b"1"[..]), (&b"bb"[..], &b""[..])] {
            buf.extend_from_slice(&(k.len() as u32).to_be_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
            buf.extend_from_slice(v);
        }
        let entries = decode_snapshot_entries(&buf).unwrap();
        assert_eq!(
            entries,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"bb".to_vec(), b"".to_vec()),
            ]
        );
    }
}
