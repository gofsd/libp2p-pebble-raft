//! Rust-native port of `pkg/ipc/ipc_android.go`'s Call/Serve pattern to the
//! main-thread/Worker split, using `shmring` 0.3.0's Rust API directly --
//! both sides of this channel are this same crate compiled to wasm (see
//! `lib.rs`'s module doc), so there is no need to go through shmring's
//! JS-facing `wasm_api` bindings (published separately as `@gofsd/shmring`
//! for pure-JS apps); per shmring's own README, `backend::SharedArrayBufferStorage`
//! plus `Writer::new`/`Reader::new` work directly from Rust "exactly like
//! the native backend". Each round trip gets a fresh, single-use pair of
//! rings (request, then response), exactly like Android's transport, so
//! there's no equivalent of the desktop transport's stale-segment race to
//! guard against here either -- and, like that transport, this only
//! supports one in-flight call at a time (one operator driving this tab's
//! UI sequentially), which is what lets `MainChannel` track "the current
//! pending call" without a request-id map.
//!
//! Carries [`crate::shmevent::Msg`], the same capnp-encoded struct every
//! other hop in this project speaks (see that module's doc comment) --
//! unlike the fixed-size `ipcproto::Request`/`Response` this replaced, a
//! capnp message has no fixed length, so both directions read until the
//! writer closes rather than a known byte count (see `poll_read_to_end`).
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use futures::channel::oneshot;
use js_sys::SharedArrayBuffer;
use shmring::backend::SharedArrayBufferStorage;
use shmring::{Options, Reader, Writer};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{MessageEvent, Worker};

use crate::shmevent::{self, Msg};

/// Ring buffer payload size -- matches `pkg/ipc`'s `capacity` constant
/// (comfortably fits an encoded request/response; see
/// `shmevent::VALUE_SIZE`).
const CAPACITY: u64 = 4096;

/// shmring's own header is `pub(crate)` (currently 64 bytes as of 0.3.0,
/// not part of its public API), so this over-allocates rather than
/// depending on that exact constant -- `Writer::new`/`Reader::new` only
/// require `storage.size() >= HEADER_SIZE + capacity`, an inequality, so
/// any comfortably-larger overhead is safe.
const STORAGE_OVERHEAD: u64 = 128;

const MIN_POLL_MS: u32 = 1;
const MAX_POLL_MS: u32 = 4;

#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shmring_ipc: {}", self.0)
    }
}

impl From<shmring::Error> for Error {
    fn from(e: shmring::Error) -> Self {
        Error(e.to_string())
    }
}
impl From<shmevent::Error> for Error {
    fn from(e: shmevent::Error) -> Self {
        Error(e.to_string())
    }
}

fn new_writer() -> Result<(Writer<SharedArrayBufferStorage>, SharedArrayBuffer), Error> {
    let storage = SharedArrayBufferStorage::new(CAPACITY + STORAGE_OVERHEAD)?;
    let sab = storage.buffer();
    let writer = Writer::new(storage, CAPACITY, Options::default())?;
    Ok((writer, sab))
}

fn open_reader(sab: SharedArrayBuffer) -> Result<Reader<SharedArrayBufferStorage>, Error> {
    let storage = SharedArrayBufferStorage::wrap(sab)?;
    Ok(Reader::new(storage, CAPACITY, Options::default())?)
}

async fn poll_write_all(
    w: &mut Writer<SharedArrayBufferStorage>,
    data: &[u8],
) -> Result<(), Error> {
    let mut written = 0usize;
    let mut wait_ms = MIN_POLL_MS;
    while written < data.len() {
        let n = w.try_write(&data[written..])?;
        if n > 0 {
            written += n;
            wait_ms = MIN_POLL_MS;
            continue;
        }
        gloo_timers::future::TimeoutFuture::new(wait_ms).await;
        wait_ms = (wait_ms * 2).min(MAX_POLL_MS);
    }
    Ok(())
}

/// Reads `r` until the writer closes and the buffer drains -- a capnp
/// message has no fixed size, so (unlike the fixed-size `ipcproto` reads
/// this replaced) there is no byte count to read up front.
async fn poll_read_to_end(r: &mut Reader<SharedArrayBufferStorage>) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    let mut chunk = [0u8; 512];
    let mut wait_ms = MIN_POLL_MS;
    loop {
        match r.try_read(&mut chunk) {
            Ok(n) if n > 0 => {
                out.extend_from_slice(&chunk[..n]);
                wait_ms = MIN_POLL_MS;
                continue;
            }
            Ok(_) => {}
            Err(shmring::Error::Eof) => return Ok(out),
            Err(e) => return Err(e.into()),
        }
        gloo_timers::future::TimeoutFuture::new(wait_ms).await;
        wait_ms = (wait_ms * 2).min(MAX_POLL_MS);
    }
}

/// Main-thread side: holds the `Worker` handle and drives `call`,
/// mirroring how `MainActivity` drives Android's in-process daemon through
/// `pkg/ipc.Call` -- just with `Worker.postMessage` standing in for
/// Android's fd handoff, exactly like `webipc.ts` before it.
pub struct MainChannel {
    worker: Worker,
    pending: Rc<RefCell<Option<oneshot::Sender<SharedArrayBuffer>>>>,
    // Kept alive for as long as MainChannel is -- dropping it would
    // unregister the listener wasm-bindgen installed on the Worker.
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
}

impl MainChannel {
    pub fn new(worker: Worker) -> Self {
        let pending: Rc<RefCell<Option<oneshot::Sender<SharedArrayBuffer>>>> =
            Rc::new(RefCell::new(None));
        let pending_cb = pending.clone();
        let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
            if let Ok(sab) = e.data().dyn_into::<SharedArrayBuffer>() {
                if let Some(sender) = pending_cb.borrow_mut().take() {
                    let _ = sender.send(sab);
                }
            }
        });
        worker.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        MainChannel {
            worker,
            pending,
            _onmessage: onmessage,
        }
    }

    /// Sends `req` (signed with `priv_key`, `None` only for
    /// `EVENT_GET_PUBLIC_KEY`/`EVENT_GET_PRIVATE_KEY` -- see
    /// `shmevent::sign`) to the Worker's [`serve`] loop and returns its
    /// response. Must not be called again before the previous call
    /// returns (see this module's doc comment).
    pub async fn call(
        &self,
        req: &Msg,
        priv_key: Option<&ed25519_dalek::SigningKey>,
    ) -> Result<Msg, Error> {
        let (mut writer, req_sab) = new_writer()?;
        let buf = shmevent::encode(req, priv_key)?;
        poll_write_all(&mut writer, &buf).await?;
        writer.close()?;

        let (sender, receiver) = oneshot::channel();
        *self.pending.borrow_mut() = Some(sender);
        self.worker
            .post_message(&req_sab)
            .map_err(|e| Error(format!("postMessage: {e:?}")))?;

        let resp_sab = receiver
            .await
            .map_err(|_| Error("worker channel closed".into()))?;
        let mut reader = open_reader(resp_sab)?;
        let resp_buf = poll_read_to_end(&mut reader).await?;
        reader.close()?;
        writer.close_storage()?;

        let (resp, _, _) = shmevent::decode(&resp_buf)?;
        Ok(resp)
    }
}

/// Worker-side: installs `self.onmessage` and answers every request handoff
/// from the main thread by decoding it, calling `handle`, and posting back
/// a fresh response ring -- the Worker-side mirror of [`MainChannel::call`].
/// Must be called from within the Worker script itself. `handle` runs
/// against `self` (a `DedicatedWorkerGlobalScope`), same as
/// `kvmobile.Start/Submit/Get` answer `pkg/ipc` requests from Android's
/// `MainActivity`. `handle` receives the decoded crc/signature alongside
/// the message so it can verify authenticity itself (see
/// `shmevent::verify`) -- the same responsibility `pkg/daemon.handleShmEvent`
/// has on the Go side -- and returns the already-`shmevent::encode`d (and
/// so already-signed, with whatever key the Worker's own dispatch logic
/// decides is appropriate) response bytes directly, so this generic
/// transport layer never needs its own signing key.
pub fn serve<F, Fut>(global: web_sys::DedicatedWorkerGlobalScope, handle: F)
where
    F: Fn(Msg, u32, Vec<u8>) -> Fut + 'static,
    Fut: std::future::Future<Output = Vec<u8>> + 'static,
{
    let handle = Rc::new(handle);
    let global_for_post = global.clone();
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
        let handle = handle.clone();
        let global = global_for_post.clone();
        let Ok(req_sab) = e.data().dyn_into::<SharedArrayBuffer>() else {
            // Not this transport's message -- e.g. shmevent's own
            // {__debug}/{__ready} relay messages (see worker.js's doc
            // comment) also arrive on this same Worker, deliberately
            // ignored here since they're not a SharedArrayBuffer handoff.
            return;
        };
        wasm_bindgen_futures::spawn_local(async move {
            let Ok(mut reader) = open_reader(req_sab) else {
                crate::p2p::debug_log("kv-raft-web: shmring_ipc::serve: open_reader failed");
                return;
            };
            let Ok(req_buf) = poll_read_to_end(&mut reader).await else {
                crate::p2p::debug_log("kv-raft-web: shmring_ipc::serve: poll_read_to_end failed");
                return;
            };
            let _ = reader.close();

            let Ok((req, crc, sig)) = shmevent::decode(&req_buf) else {
                crate::p2p::debug_log("kv-raft-web: shmring_ipc::serve: decode failed");
                return;
            };
            let resp_buf = handle(req, crc, sig).await;

            let Ok((mut writer, resp_sab)) = new_writer() else {
                crate::p2p::debug_log("kv-raft-web: shmring_ipc::serve: new_writer failed");
                return;
            };
            if poll_write_all(&mut writer, &resp_buf).await.is_err() {
                crate::p2p::debug_log("kv-raft-web: shmring_ipc::serve: poll_write_all failed");
                return;
            }
            let _ = writer.close();
            let _ = global.post_message(&resp_sab);
            // Not close_storage()'d here -- the main thread's `call` reads
            // to completion and drops its own Reader, which is enough for
            // an in-process (same wasm heap isn't shared, but the
            // SharedArrayBuffer itself is GC'd by the JS engine once both
            // sides drop their reference) segment; unlike the desktop
            // shm_open transport, there is no OS resource to unlink here.
        });
    });
    global.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();
}
