//! Byte-for-byte port of `pkg/ipcproto/proto.go`'s fixed-size wire messages.
//! Used for the shmring-backed main-thread/Worker channel (see
//! `shmring_ipc.rs`), mirroring `pkg/ipc/ipc_android.go`'s in-process
//! Call/Serve pattern -- not used to talk to a Go daemon directly (that hop
//! speaks `raft_wire`'s msgpack framing instead, over `ClientProtocolID`).

pub const KEY_SIZE: usize = 256;
pub const VALUE_SIZE: usize = 256;
const ID_SIZE: usize = 8;

pub const REQUEST_SIZE: usize = ID_SIZE + 1 + KEY_SIZE + VALUE_SIZE;
pub const RESPONSE_SIZE: usize = ID_SIZE + 1 + VALUE_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Repurposed on the web client from "join the raft cluster as a
    /// voter" to "connect to the target node and join as a non-voting
    /// learner" -- see `learner.rs`. `Key` carries the target node's
    /// multiaddr, same Key-carries-a-multiaddr convention
    /// `mobile/kvmobile.go`'s `Start` already relies on for its own
    /// build-time-baked leader address.
    Add = 1,
    Set = 2,
    Get = 3,
}

impl Action {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Action::Add),
            2 => Some(Action::Set),
            3 => Some(Action::Get),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok = 0,
    Error = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub id: u64,
    pub action: Action,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub id: u64,
    pub status: Status,
    pub value: String,
}

fn put_string(dst: &mut [u8], s: &str) {
    let b = s.as_bytes();
    let n = b.len().min(dst.len());
    dst[..n].copy_from_slice(&b[..n]);
    for byte in &mut dst[n..] {
        *byte = 0;
    }
}

fn get_string(src: &[u8]) -> String {
    let end = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    String::from_utf8_lossy(&src[..end]).into_owned()
}

impl Request {
    pub fn new(action: Action, key: &str, value: &str) -> Self {
        Request {
            id: 0,
            action,
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    pub fn encode(&self) -> [u8; REQUEST_SIZE] {
        let mut buf = [0u8; REQUEST_SIZE];
        buf[0..ID_SIZE].copy_from_slice(&self.id.to_be_bytes());
        buf[ID_SIZE] = self.action as u8;
        put_string(&mut buf[ID_SIZE + 1..ID_SIZE + 1 + KEY_SIZE], &self.key);
        put_string(&mut buf[ID_SIZE + 1 + KEY_SIZE..], &self.value);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, &'static str> {
        if buf.len() < REQUEST_SIZE {
            return Err("ipcproto: short request");
        }
        let id = u64::from_be_bytes(buf[0..ID_SIZE].try_into().unwrap());
        let action = Action::from_byte(buf[ID_SIZE]).ok_or("ipcproto: unknown action")?;
        let key = get_string(&buf[ID_SIZE + 1..ID_SIZE + 1 + KEY_SIZE]);
        let value = get_string(&buf[ID_SIZE + 1 + KEY_SIZE..ID_SIZE + 1 + KEY_SIZE + VALUE_SIZE]);
        Ok(Request {
            id,
            action,
            key,
            value,
        })
    }
}

impl Response {
    pub fn new(status: Status, value: &str) -> Self {
        Response {
            id: 0,
            status,
            value: value.to_string(),
        }
    }

    pub fn encode(&self) -> [u8; RESPONSE_SIZE] {
        let mut buf = [0u8; RESPONSE_SIZE];
        buf[0..ID_SIZE].copy_from_slice(&self.id.to_be_bytes());
        buf[ID_SIZE] = self.status as u8;
        put_string(&mut buf[ID_SIZE + 1..], &self.value);
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, &'static str> {
        if buf.len() < RESPONSE_SIZE {
            return Err("ipcproto: short response");
        }
        let id = u64::from_be_bytes(buf[0..ID_SIZE].try_into().unwrap());
        let status = match buf[ID_SIZE] {
            0 => Status::Ok,
            _ => Status::Error,
        };
        let value = get_string(&buf[ID_SIZE + 1..ID_SIZE + 1 + VALUE_SIZE]);
        Ok(Response { id, status, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let mut req = Request::new(Action::Set, "hello", "world");
        req.id = 0xdeadbeefcafebabe;
        let encoded = req.encode();
        assert_eq!(encoded.len(), REQUEST_SIZE);
        let decoded = Request::decode(&encoded).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn response_roundtrip() {
        let mut resp = Response::new(Status::Error, "boom");
        resp.id = 42;
        let encoded = resp.encode();
        assert_eq!(encoded.len(), RESPONSE_SIZE);
        let decoded = Response::decode(&encoded).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncates_long_values() {
        let long = "x".repeat(KEY_SIZE + 50);
        let req = Request::new(Action::Get, &long, "");
        let encoded = req.encode();
        let decoded = Request::decode(&encoded).unwrap();
        assert_eq!(decoded.key.len(), KEY_SIZE);
    }
}
