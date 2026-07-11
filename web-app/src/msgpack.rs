//! Minimal msgpack primitives matching the wire behavior of
//! `github.com/hashicorp/go-msgpack/v2/codec`'s `MsgpackHandle{}` defaults,
//! which is what `hashicorp/raft`'s `NetworkTransport` encodes/decodes RPCs
//! with (see `raft_wire.rs`): a Go struct becomes a msgpack map keyed by
//! field name, an anonymous/embedded field's own fields are promoted into
//! the same map (matching Go's field-promotion semantics, not nested under
//! a key), and `[]byte` is written as the msgpack raw/str type rather than
//! `bin` (there is no Go `string`-typed field in any raft RPC struct, so
//! this crate never needs to distinguish the two).
//!
//! Map/array key *order* is irrelevant here: msgpack maps are unordered and
//! any conformant decoder -- Go's included -- accepts any order, so nothing
//! in this module tries to replicate the specific order Go's own encoder
//! would pick.

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    UnexpectedEof,
    BadTag(u8),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Writer { buf: Vec::new() }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn write_nil(&mut self) {
        self.buf.push(0xc0);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 0xc3 } else { 0xc2 });
    }

    pub fn write_map_header(&mut self, len: u32) {
        if len < 16 {
            self.buf.push(0x80 | len as u8);
        } else if len <= u16::MAX as u32 {
            self.buf.push(0xde);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            self.buf.push(0xdf);
            self.buf.extend_from_slice(&len.to_be_bytes());
        }
    }

    pub fn write_array_header(&mut self, len: u32) {
        if len < 16 {
            self.buf.push(0x90 | len as u8);
        } else if len <= u16::MAX as u32 {
            self.buf.push(0xdc);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            self.buf.push(0xdd);
            self.buf.extend_from_slice(&len.to_be_bytes());
        }
    }

    /// Encodes `b` as msgpack raw/str -- the wire type both genuine strings
    /// (map keys, the RPC response error string) and every `[]byte`-typed
    /// RPC field use by default under `MsgpackHandle{}`.
    pub fn write_raw(&mut self, b: &[u8]) {
        let len = b.len();
        if len < 32 {
            self.buf.push(0xa0 | len as u8);
        } else if len <= u8::MAX as usize {
            self.buf.push(0xd9);
            self.buf.push(len as u8);
        } else if len <= u16::MAX as usize {
            self.buf.push(0xda);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            self.buf.push(0xdb);
            self.buf.extend_from_slice(&(len as u32).to_be_bytes());
        }
        self.buf.extend_from_slice(b);
    }

    pub fn write_str(&mut self, s: &str) {
        self.write_raw(s.as_bytes());
    }

    /// Encodes an optional `[]byte` field: `None` as msgpack nil (matching
    /// Go encoding a nil slice), `Some` as raw/str.
    pub fn write_bytes_field(&mut self, b: &Option<Vec<u8>>) {
        match b {
            None => self.write_nil(),
            Some(v) => self.write_raw(v),
        }
    }

    pub fn write_uint(&mut self, v: u64) {
        if v < 0x80 {
            self.buf.push(v as u8);
        } else if v <= u8::MAX as u64 {
            self.buf.push(0xcc);
            self.buf.push(v as u8);
        } else if v <= u16::MAX as u64 {
            self.buf.push(0xcd);
            self.buf.extend_from_slice(&(v as u16).to_be_bytes());
        } else if v <= u32::MAX as u64 {
            self.buf.push(0xce);
            self.buf.extend_from_slice(&(v as u32).to_be_bytes());
        } else {
            self.buf.push(0xcf);
            self.buf.extend_from_slice(&v.to_be_bytes());
        }
    }

    pub fn write_int(&mut self, v: i64) {
        if v >= 0 {
            self.write_uint(v as u64);
            return;
        }
        if v >= -32 {
            self.buf.push(v as i8 as u8);
        } else if v >= i8::MIN as i64 {
            self.buf.push(0xd0);
            self.buf.push(v as i8 as u8);
        } else if v >= i16::MIN as i64 {
            self.buf.push(0xd1);
            self.buf.extend_from_slice(&(v as i16).to_be_bytes());
        } else if v >= i32::MIN as i64 {
            self.buf.push(0xd2);
            self.buf.extend_from_slice(&(v as i32).to_be_bytes());
        } else {
            self.buf.push(0xd3);
            self.buf.extend_from_slice(&v.to_be_bytes());
        }
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(Error::UnexpectedEof);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn peek(&self) -> Result<u8> {
        self.buf.get(self.pos).copied().ok_or(Error::UnexpectedEof)
    }

    pub fn read_nil(&mut self) -> Result<()> {
        match self.byte()? {
            0xc0 => Ok(()),
            b => Err(Error::BadTag(b)),
        }
    }

    pub fn is_nil(&self) -> Result<bool> {
        Ok(self.peek()? == 0xc0)
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        match self.byte()? {
            0xc3 => Ok(true),
            0xc2 => Ok(false),
            b => Err(Error::BadTag(b)),
        }
    }

    pub fn read_map_header(&mut self) -> Result<u32> {
        match self.byte()? {
            b @ 0x80..=0x8f => Ok((b & 0x0f) as u32),
            0xde => {
                let s = self.take(2)?;
                Ok(u16::from_be_bytes([s[0], s[1]]) as u32)
            }
            0xdf => {
                let s = self.take(4)?;
                Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
            }
            b => Err(Error::BadTag(b)),
        }
    }

    pub fn read_array_header(&mut self) -> Result<u32> {
        match self.byte()? {
            b @ 0x90..=0x9f => Ok((b & 0x0f) as u32),
            0xdc => {
                let s = self.take(2)?;
                Ok(u16::from_be_bytes([s[0], s[1]]) as u32)
            }
            0xdd => {
                let s = self.take(4)?;
                Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
            }
            b => Err(Error::BadTag(b)),
        }
    }

    pub fn read_raw(&mut self) -> Result<&'a [u8]> {
        let b = self.byte()?;
        let len = match b {
            0xa0..=0xbf => (b & 0x1f) as usize,
            0xd9 => self.byte()? as usize,
            0xda => {
                let s = self.take(2)?;
                u16::from_be_bytes([s[0], s[1]]) as usize
            }
            0xdb => {
                let s = self.take(4)?;
                u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize
            }
            _ => return Err(Error::BadTag(b)),
        };
        self.take(len)
    }

    pub fn read_str(&mut self) -> Result<String> {
        Ok(String::from_utf8_lossy(self.read_raw()?).into_owned())
    }

    /// Reads a `[]byte` field that may be msgpack nil (-> `None`) or
    /// raw/str (-> `Some`).
    pub fn read_bytes_field(&mut self) -> Result<Option<Vec<u8>>> {
        if self.is_nil()? {
            self.read_nil()?;
            return Ok(None);
        }
        Ok(Some(self.read_raw()?.to_vec()))
    }

    pub fn read_uint(&mut self) -> Result<u64> {
        match self.byte()? {
            b @ 0x00..=0x7f => Ok(b as u64),
            0xcc => Ok(self.byte()? as u64),
            0xcd => {
                let s = self.take(2)?;
                Ok(u16::from_be_bytes([s[0], s[1]]) as u64)
            }
            0xce => {
                let s = self.take(4)?;
                Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as u64)
            }
            0xcf => {
                let s = self.take(8)?;
                Ok(u64::from_be_bytes(s.try_into().unwrap()))
            }
            b => Err(Error::BadTag(b)),
        }
    }

    pub fn read_int(&mut self) -> Result<i64> {
        match self.byte()? {
            b @ 0x00..=0x7f => Ok(b as i64),
            b @ 0xe0..=0xff => Ok(b as i8 as i64),
            0xcc => Ok(self.byte()? as i64),
            0xcd => {
                let s = self.take(2)?;
                Ok(u16::from_be_bytes([s[0], s[1]]) as i64)
            }
            0xce => {
                let s = self.take(4)?;
                Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as i64)
            }
            0xcf => {
                let s = self.take(8)?;
                Ok(u64::from_be_bytes(s.try_into().unwrap()) as i64)
            }
            0xd0 => Ok(self.byte()? as i8 as i64),
            0xd1 => {
                let s = self.take(2)?;
                Ok(i16::from_be_bytes([s[0], s[1]]) as i64)
            }
            0xd2 => {
                let s = self.take(4)?;
                Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]) as i64)
            }
            0xd3 => {
                let s = self.take(8)?;
                Ok(i64::from_be_bytes(s.try_into().unwrap()))
            }
            b => Err(Error::BadTag(b)),
        }
    }

    /// Skips one arbitrary msgpack value -- used to ignore a map key this
    /// crate doesn't recognize, so decoding stays forward-compatible with a
    /// future raft version that adds a field.
    pub fn skip_value(&mut self) -> Result<()> {
        match self.peek()? {
            0xc0 | 0xc2 | 0xc3 => {
                self.byte()?;
            }
            0x00..=0x7f | 0xe0..=0xff => {
                self.byte()?;
            }
            0xcc | 0xd0 => {
                self.take(2)?;
            }
            0xcd | 0xd1 => {
                self.take(3)?;
            }
            0xce | 0xd2 => {
                self.take(5)?;
            }
            0xcf | 0xd3 => {
                self.take(9)?;
            }
            0xa0..=0xbf | 0xd9 | 0xda | 0xdb => {
                self.read_raw()?;
            }
            0x90..=0x9f | 0xdc | 0xdd => {
                let n = self.read_array_header()?;
                for _ in 0..n {
                    self.skip_value()?;
                }
            }
            0x80..=0x8f | 0xde | 0xdf => {
                let n = self.read_map_header()?;
                for _ in 0..n {
                    self.skip_value()?;
                    self.skip_value()?;
                }
            }
            b => return Err(Error::BadTag(b)),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_primitives() {
        let mut w = Writer::new();
        w.write_uint(0);
        w.write_uint(127);
        w.write_uint(128);
        w.write_uint(70000);
        w.write_uint(u64::MAX);
        w.write_bool(true);
        w.write_bool(false);
        w.write_nil();
        w.write_raw(b"hello");
        let bytes = w.into_bytes();

        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_uint().unwrap(), 0);
        assert_eq!(r.read_uint().unwrap(), 127);
        assert_eq!(r.read_uint().unwrap(), 128);
        assert_eq!(r.read_uint().unwrap(), 70000);
        assert_eq!(r.read_uint().unwrap(), u64::MAX);
        assert_eq!(r.read_bool().unwrap(), true);
        assert_eq!(r.read_bool().unwrap(), false);
        assert!(r.is_nil().unwrap());
        r.read_nil().unwrap();
        assert_eq!(r.read_raw().unwrap(), b"hello");
    }
}
