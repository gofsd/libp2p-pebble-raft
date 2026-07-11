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

use crate::ipcproto::{Request, Response, REQUEST_SIZE, RESPONSE_SIZE};

/// Ring buffer payload size -- matches `pkg/ipc`'s `capacity` constant
/// (comfortably fits the larger of Request/Response).
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

async fn poll_read_exact(
    r: &mut Reader<SharedArrayBufferStorage>,
    buf: &mut [u8],
) -> Result<(), Error> {
    let mut total = 0usize;
    let mut wait_ms = MIN_POLL_MS;
    while total < buf.len() {
        match r.try_read(&mut buf[total..]) {
            Ok(n) if n > 0 => {
                total += n;
                wait_ms = MIN_POLL_MS;
                continue;
            }
            Ok(_) => {}
            Err(shmring::Error::Eof) => return Err(Error("unexpected EOF mid-message".into())),
            Err(e) => return Err(e.into()),
        }
        gloo_timers::future::TimeoutFuture::new(wait_ms).await;
        wait_ms = (wait_ms * 2).min(MAX_POLL_MS);
    }
    Ok(())
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

    /// Sends `req` to the Worker's [`serve`] loop and returns its
    /// response. Must not be called again before the previous call
    /// returns (see this module's doc comment).
    pub async fn call(&self, req: Request) -> Result<Response, Error> {
        let (mut writer, req_sab) = new_writer()?;
        let buf = req.encode();
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
        let mut resp_buf = [0u8; RESPONSE_SIZE];
        poll_read_exact(&mut reader, &mut resp_buf).await?;
        reader.close()?;
        writer.close_storage()?;

        Response::decode(&resp_buf).map_err(|e| Error(e.to_string()))
    }
}

/// Worker-side: installs `self.onmessage` and answers every request handoff
/// from the main thread by decoding it, calling `handle`, and posting back
/// a fresh response ring -- the Worker-side mirror of [`MainChannel::call`].
/// Must be called from within the Worker script itself. `handle` runs
/// against `self` (a `DedicatedWorkerGlobalScope`), same as
/// `kvmobile.Start/Submit/Get` answer `pkg/ipc` requests from Android's
/// `MainActivity`.
pub fn serve<F, Fut>(global: web_sys::DedicatedWorkerGlobalScope, handle: F)
where
    F: Fn(Request) -> Fut + 'static,
    Fut: std::future::Future<Output = Response> + 'static,
{
    let handle = Rc::new(handle);
    let global_for_post = global.clone();
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
        let handle = handle.clone();
        let global = global_for_post.clone();
        let Ok(req_sab) = e.data().dyn_into::<SharedArrayBuffer>() else {
            return;
        };
        wasm_bindgen_futures::spawn_local(async move {
            let Ok(mut reader) = open_reader(req_sab) else {
                return;
            };
            let mut req_buf = [0u8; REQUEST_SIZE];
            if poll_read_exact(&mut reader, &mut req_buf).await.is_err() {
                return;
            }
            let _ = reader.close();

            let Ok(req) = Request::decode(&req_buf) else {
                return;
            };
            let req_id = req.id;
            let mut resp = handle(req).await;
            resp.id = req_id;

            let Ok((mut writer, resp_sab)) = new_writer() else {
                return;
            };
            let buf = resp.encode();
            if poll_write_all(&mut writer, &buf).await.is_err() {
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
