//! C-ABI shim over restate-sdk for the restate.mojo Mojo binding.
//!
//! Architecture: Rust owns the Restate endpoint, event loop, and journal.
//! Mojo business logic runs a synchronous driver loop:
//!
//!   rst_serve(...)                      start the endpoint (background thread)
//!   loop:
//!     h = rst_next()                    block until an invocation arrives
//!     ... rst_sleep / rst_call / rst_get_state / rst_set_state / rst_run_* ...
//!     rst_complete(h, out) | rst_fail(h, msg)
//!
//! Each SDK handler future forwards the invocation to the Mojo loop over
//! channels and then executes the ctx operations Mojo requests. When Restate
//! suspends an invocation (e.g. a long sleep), the handler future is dropped,
//! the channels close, and the corresponding Mojo call returns STATUS_GONE —
//! the Mojo driver then abandons that invocation; Restate will re-invoke it
//! later with journal replay.
//!
//! Payloads are raw bytes end to end (Vec<u8> implements the SDK serde).

use futures::future::BoxFuture;
use restate_sdk::context::RequestTarget;
use restate_sdk::discovery;
use restate_sdk::endpoint::{ContextInternal, Endpoint};
use restate_sdk::errors::{HandlerError, TerminalError};
use restate_sdk::http_server::HttpServer;
use restate_sdk::service::{macro_support, Service};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc as smpsc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc as tmpsc;

// ── status codes shared with the Mojo side ─────────────────────────────────
const STATUS_OK: i32 = 0;
/// The invocation is gone (suspended/cancelled by Restate) — abandon it.
const STATUS_GONE: i32 = 1;
/// The operation failed with a Restate terminal error (message in rst_buf).
const STATUS_TERMINAL: i32 = 2;
/// Misuse / internal error (message via rst_last_error).
const STATUS_ERROR: i32 = 3;
/// get_state: no value for this key.
const STATUS_EMPTY: i32 = 4;
/// run_enter: not in the journal — execute the closure, then rst_run_exit.
const STATUS_EXECUTE: i32 = 5;

// ── bridge plumbing ────────────────────────────────────────────────────────

enum Cmd {
    Sleep(u64),
    GetState(String),
    SetState(String, Vec<u8>),
    ClearState(String),
    Call { target: RequestTarget, payload: Vec<u8> },
    Send { target: RequestTarget, payload: Vec<u8>, delay_ms: u64 },
    RunEnter,
    RunExit(Vec<u8>),
    Complete(Vec<u8>),
    Fail(String),
}

enum Reply {
    Ok(Vec<u8>),
    Empty,
    Terminal(String),
    RunExecute,
}

struct Job {
    handler: String,
    key: String,
    input: Vec<u8>,
    cmd_tx: tmpsc::UnboundedSender<Cmd>,
    reply_rx: smpsc::Receiver<Reply>,
}

struct Bridge {
    job_tx: smpsc::Sender<Job>,
    job_rx: Mutex<smpsc::Receiver<Job>>,
}

static BRIDGE: OnceLock<Bridge> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Invocation {
    job: Job,
}

fn invocations() -> &'static Mutex<HashMap<u64, Invocation>> {
    static INVS: OnceLock<Mutex<HashMap<u64, Invocation>>> = OnceLock::new();
    INVS.get_or_init(|| Mutex::new(HashMap::new()))
}

// The Mojo driver is single-threaded: a thread-local scratch holds the last
// result buffer and error text, read back via rst_buf_ptr/rst_buf_len and
// rst_last_error.
thread_local! {
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").unwrap());
}

fn set_scratch(bytes: Vec<u8>) {
    SCRATCH.with(|s| *s.borrow_mut() = bytes);
}

fn set_error(msg: impl Into<String>) {
    let msg = msg.into();
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg.replace('\0', "?")).unwrap();
    });
}

unsafe fn cstr(p: *const c_char) -> Result<String, ()> {
    if p.is_null() {
        return Err(());
    }
    CStr::from_ptr(p).to_str().map(str::to_string).map_err(|_| ())
}

unsafe fn bytes(p: *const u8, len: usize) -> Vec<u8> {
    if p.is_null() || len == 0 {
        Vec::new()
    } else {
        std::slice::from_raw_parts(p, len).to_vec()
    }
}

// ── the dynamic Restate service ────────────────────────────────────────────

struct DynamicService {
    job_tx: smpsc::Sender<Job>,
}

impl Service for DynamicService {
    type Future = BoxFuture<'static, Result<(), restate_sdk::endpoint::Error>>;

    fn handle(&self, ctx: ContextInternal) -> Self::Future {
        let job_tx = self.job_tx.clone();
        Box::pin(async move {
            let (input, metadata) = ctx.input::<Vec<u8>>().await;
            let (cmd_tx, mut cmd_rx) = tmpsc::unbounded_channel::<Cmd>();
            let (reply_tx, reply_rx) = smpsc::channel::<Reply>();
            if job_tx
                .send(Job {
                    handler: ctx.handler_name().to_string(),
                    key: metadata.key.clone(),
                    input,
                    cmd_tx,
                    reply_rx,
                })
                .is_err()
            {
                // No Mojo driver: nothing we can do.
                return Ok(());
            }

            loop {
                let Some(cmd) = cmd_rx.recv().await else {
                    // Mojo dropped the invocation (or exited).
                    return Ok(());
                };
                match cmd {
                    Cmd::Sleep(ms) => {
                        let r = ctx.sleep(Duration::from_millis(ms)).await;
                        let _ = reply_tx.send(match r {
                            Ok(()) => Reply::Ok(Vec::new()),
                            Err(te) => Reply::Terminal(te.to_string()),
                        });
                    }
                    Cmd::GetState(key) => {
                        let r = ctx.get::<Vec<u8>>(&key).await;
                        let _ = reply_tx.send(match r {
                            Ok(Some(v)) => Reply::Ok(v),
                            Ok(None) => Reply::Empty,
                            Err(te) => Reply::Terminal(te.to_string()),
                        });
                    }
                    Cmd::SetState(key, value) => {
                        ctx.set(&key, value);
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::ClearState(key) => {
                        ctx.clear(&key);
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::Call { target, payload } => {
                        let r = ctx
                            .call::<Vec<u8>, Vec<u8>>(target, None, None, None, vec![], payload)
                            .await;
                        let _ = reply_tx.send(match r {
                            Ok(v) => Reply::Ok(v),
                            Err(te) => Reply::Terminal(te.to_string()),
                        });
                    }
                    Cmd::Send { target, payload, delay_ms } => {
                        let delay = if delay_ms == 0 {
                            None
                        } else {
                            Some(Duration::from_millis(delay_ms))
                        };
                        let _ = ctx.send(target, None, None, None, vec![], payload, delay);
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::RunEnter => {
                        let rtx = reply_tx.clone();
                        let crx = &mut cmd_rx;
                        let r = ctx
                            .run(move || async move {
                                let _ = rtx.send(Reply::RunExecute);
                                match crx.recv().await {
                                    Some(Cmd::RunExit(bytes)) => Ok(bytes),
                                    _ => Err(HandlerError::from(TerminalError::new(
                                        "restate.mojo protocol error: expected rst_run_exit",
                                    ))),
                                }
                            })
                            .await;
                        let _ = reply_tx.send(match r {
                            Ok(v) => Reply::Ok(v),
                            Err(te) => Reply::Terminal(te.to_string()),
                        });
                    }
                    Cmd::RunExit(_) => {
                        // rst_run_exit without rst_run_enter — protocol error.
                        let _ = reply_tx.send(Reply::Terminal(
                            "restate.mojo protocol error: rst_run_exit without rst_run_enter"
                                .into(),
                        ));
                    }
                    Cmd::Complete(bytes) => {
                        ctx.handle_handler_result::<Vec<u8>>(Ok(bytes));
                        ctx.end();
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                        return Ok(());
                    }
                    Cmd::Fail(msg) => {
                        ctx.handle_handler_result::<Vec<u8>>(Err(HandlerError::from(
                            TerminalError::new(msg),
                        )));
                        ctx.end();
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                        return Ok(());
                    }
                }
            }
        })
    }
}

fn make_discovery(service_name: &str, object: bool, handlers: &[String]) -> discovery::Service {
    discovery::Service {
        ty: if object {
            discovery::ServiceType::VirtualObject
        } else {
            discovery::ServiceType::Service
        },
        name: discovery::ServiceName::try_from(service_name.to_string())
            .expect("service name valid"),
        handlers: handlers
            .iter()
            .map(|h| discovery::Handler {
                name: discovery::HandlerName::try_from(h.as_str()).expect("handler name valid"),
                input: Some(discovery::InputPayload::from_metadata::<Vec<u8>>()),
                output: Some(discovery::OutputPayload::from_metadata::<Vec<u8>>()),
                ty: None,
                documentation: None,
                metadata: Default::default(),
                abort_timeout: None,
                enable_lazy_state: None,
                idempotency_retention: None,
                inactivity_timeout: None,
                ingress_private: None,
                journal_retention: None,
                workflow_completion_retention: None,
                retry_policy_exponentiation_factor: None,
                retry_policy_initial_interval: None,
                retry_policy_max_attempts: None,
                retry_policy_max_interval: None,
                retry_policy_on_max_attempts: None,
            })
            .collect(),
        documentation: None,
        metadata: Default::default(),
        abort_timeout: None,
        inactivity_timeout: None,
        journal_retention: None,
        idempotency_retention: None,
        enable_lazy_state: None,
        ingress_private: None,
        retry_policy_initial_interval: None,
        retry_policy_max_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_exponentiation_factor: None,
        retry_policy_on_max_attempts: None,
    }
}

// ── C ABI ──────────────────────────────────────────────────────────────────

/// Start the Restate endpoint on `host_port` (e.g. "0.0.0.0:9080"), serving
/// one service with the given comma-separated handler names.
/// `object` != 0 registers a Virtual Object (keyed, with state); 0 a Service.
/// Non-blocking: the server runs on a background thread.
#[no_mangle]
pub extern "C" fn rst_serve(
    host_port: *const c_char,
    service_name: *const c_char,
    object: i32,
    handlers_csv: *const c_char,
) -> i32 {
    let (Ok(addr), Ok(name), Ok(handlers)) = (
        unsafe { cstr(host_port) },
        unsafe { cstr(service_name) },
        unsafe { cstr(handlers_csv) },
    ) else {
        set_error("rst_serve: null/invalid argument");
        return STATUS_ERROR;
    };
    let handlers: Vec<String> = handlers
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if handlers.is_empty() {
        set_error("rst_serve: no handlers given");
        return STATUS_ERROR;
    }
    let Ok(sock_addr) = addr.parse::<std::net::SocketAddr>() else {
        set_error(format!("rst_serve: invalid address '{addr}'"));
        return STATUS_ERROR;
    };

    let (job_tx, job_rx) = smpsc::channel::<Job>();
    if BRIDGE
        .set(Bridge {
            job_tx: job_tx.clone(),
            job_rx: Mutex::new(job_rx),
        })
        .is_err()
    {
        set_error("rst_serve: already serving");
        return STATUS_ERROR;
    }

    let discovery = make_discovery(&name, object != 0, &handlers);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async move {
            let endpoint = Endpoint::builder()
                .bind(macro_support::service_definition(
                    DynamicService { job_tx },
                    discovery,
                ))
                .build();
            HttpServer::new(endpoint).listen_and_serve(sock_addr).await;
        });
    });
    STATUS_OK
}

/// Block until the next invocation arrives; returns its handle (> 0), or 0
/// on error (bridge not started / channel closed).
#[no_mangle]
pub extern "C" fn rst_next() -> u64 {
    let Some(bridge) = BRIDGE.get() else {
        set_error("rst_next: rst_serve was not called");
        return 0;
    };
    let job = {
        let rx = bridge.job_rx.lock().expect("job_rx lock");
        match rx.recv() {
            Ok(j) => j,
            Err(_) => {
                set_error("rst_next: endpoint stopped");
                return 0;
            }
        }
    };
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    invocations()
        .lock()
        .expect("invocations lock")
        .insert(id, Invocation { job });
    id
}

fn with_invocation<F: FnOnce(&Invocation) -> i32>(handle: u64, f: F) -> i32 {
    // The map lock is held for the duration of the op. The Mojo driver is
    // single-threaded, so there is no contention in practice.
    let invs = invocations().lock().expect("invocations lock");
    match invs.get(&handle) {
        Some(inv) => f(inv),
        None => {
            set_error("unknown invocation handle");
            STATUS_ERROR
        }
    }
}

fn drop_invocation(handle: u64) {
    invocations()
        .lock()
        .expect("invocations lock")
        .remove(&handle);
}

/// Send one command and block for one reply, translating channel breakage
/// into STATUS_GONE (the invocation was suspended or cancelled by Restate).
fn roundtrip(inv: &Invocation, cmd: Cmd) -> Result<Reply, i32> {
    if inv.job.cmd_tx.send(cmd).is_err() {
        return Err(STATUS_GONE);
    }
    inv.job.reply_rx.recv().map_err(|_| STATUS_GONE)
}

fn reply_to_status(reply: Reply) -> i32 {
    match reply {
        Reply::Ok(bytes) => {
            set_scratch(bytes);
            STATUS_OK
        }
        Reply::Empty => {
            set_scratch(Vec::new());
            STATUS_EMPTY
        }
        Reply::Terminal(msg) => {
            set_scratch(msg.into_bytes());
            STATUS_TERMINAL
        }
        Reply::RunExecute => STATUS_EXECUTE,
    }
}

fn simple_op(handle: u64, cmd: Cmd) -> i32 {
    with_invocation(handle, |inv| match roundtrip(inv, cmd) {
        Ok(reply) => reply_to_status(reply),
        Err(status) => status,
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_handler(handle: u64) -> i32 {
    with_invocation(handle, |inv| {
        set_scratch(inv.job.handler.clone().into_bytes());
        STATUS_OK
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_key(handle: u64) -> i32 {
    with_invocation(handle, |inv| {
        set_scratch(inv.job.key.clone().into_bytes());
        STATUS_OK
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_input(handle: u64) -> i32 {
    with_invocation(handle, |inv| {
        set_scratch(inv.job.input.clone());
        STATUS_OK
    })
}

#[no_mangle]
pub extern "C" fn rst_sleep(handle: u64, millis: u64) -> i32 {
    simple_op(handle, Cmd::Sleep(millis))
}

#[no_mangle]
pub extern "C" fn rst_get_state(handle: u64, key: *const c_char) -> i32 {
    let Ok(key) = (unsafe { cstr(key) }) else {
        set_error("rst_get_state: null key");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::GetState(key))
}

#[no_mangle]
pub extern "C" fn rst_set_state(
    handle: u64,
    key: *const c_char,
    value: *const u8,
    value_len: usize,
) -> i32 {
    let Ok(key) = (unsafe { cstr(key) }) else {
        set_error("rst_set_state: null key");
        return STATUS_ERROR;
    };
    let value = unsafe { bytes(value, value_len) };
    simple_op(handle, Cmd::SetState(key, value))
}

#[no_mangle]
pub extern "C" fn rst_clear_state(handle: u64, key: *const c_char) -> i32 {
    let Ok(key) = (unsafe { cstr(key) }) else {
        set_error("rst_clear_state: null key");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::ClearState(key))
}

fn parse_target(
    service: *const c_char,
    handler: *const c_char,
    key: *const c_char,
) -> Result<RequestTarget, i32> {
    let (Ok(service), Ok(handler)) = (unsafe { cstr(service) }, unsafe { cstr(handler) }) else {
        set_error("call/send: null service/handler");
        return Err(STATUS_ERROR);
    };
    if key.is_null() {
        Ok(RequestTarget::Service { name: service, handler })
    } else {
        let Ok(key) = (unsafe { cstr(key) }) else {
            set_error("call/send: invalid key");
            return Err(STATUS_ERROR);
        };
        Ok(RequestTarget::Object { name: service, key, handler })
    }
}

/// Durable request/response call to another service/handler. Pass a NULL
/// `key` to target a plain service, non-NULL for a virtual object.
#[no_mangle]
pub extern "C" fn rst_call(
    handle: u64,
    service: *const c_char,
    handler: *const c_char,
    key: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let target = match parse_target(service, handler, key) {
        Ok(t) => t,
        Err(status) => return status,
    };
    let payload = unsafe { bytes(payload, payload_len) };
    simple_op(handle, Cmd::Call { target, payload })
}

/// Durable one-way message (optionally delayed).
#[no_mangle]
pub extern "C" fn rst_send(
    handle: u64,
    service: *const c_char,
    handler: *const c_char,
    key: *const c_char,
    payload: *const u8,
    payload_len: usize,
    delay_millis: u64,
) -> i32 {
    let target = match parse_target(service, handler, key) {
        Ok(t) => t,
        Err(status) => return status,
    };
    let payload = unsafe { bytes(payload, payload_len) };
    simple_op(handle, Cmd::Send { target, payload, delay_ms: delay_millis })
}

/// Enter a journaled side-effect block. STATUS_OK: the journal already had
/// the result (in rst_buf) — skip the computation. STATUS_EXECUTE: run the
/// side effect, then call rst_run_exit with its result.
#[no_mangle]
pub extern "C" fn rst_run_enter(handle: u64) -> i32 {
    simple_op(handle, Cmd::RunEnter)
}

/// Provide the side-effect result; it is journaled and echoed back in
/// rst_buf on STATUS_OK.
#[no_mangle]
pub extern "C" fn rst_run_exit(handle: u64, value: *const u8, value_len: usize) -> i32 {
    let value = unsafe { bytes(value, value_len) };
    simple_op(handle, Cmd::RunExit(value))
}

/// Complete the invocation successfully with the given output bytes.
#[no_mangle]
pub extern "C" fn rst_complete(handle: u64, output: *const u8, output_len: usize) -> i32 {
    let output = unsafe { bytes(output, output_len) };
    let status = simple_op(handle, Cmd::Complete(output));
    drop_invocation(handle);
    if status == STATUS_GONE {
        STATUS_GONE
    } else {
        STATUS_OK
    }
}

/// Fail the invocation with a terminal error (not retried).
#[no_mangle]
pub extern "C" fn rst_fail(handle: u64, message: *const c_char) -> i32 {
    let msg = unsafe { cstr(message) }.unwrap_or_else(|_| "handler failed".into());
    let status = simple_op(handle, Cmd::Fail(msg));
    drop_invocation(handle);
    if status == STATUS_GONE {
        STATUS_GONE
    } else {
        STATUS_OK
    }
}

/// Abandon an invocation without completing it (e.g. after STATUS_GONE).
/// Restate will retry / resume it with journal replay.
#[no_mangle]
pub extern "C" fn rst_abandon(handle: u64) -> i32 {
    drop_invocation(handle);
    STATUS_OK
}

/// Pointer/length of the last operation's result buffer (valid until the
/// next operation on this thread).
#[no_mangle]
pub extern "C" fn rst_buf_ptr() -> *const u8 {
    SCRATCH.with(|s| s.borrow().as_ptr())
}

#[no_mangle]
pub extern "C" fn rst_buf_len() -> usize {
    SCRATCH.with(|s| s.borrow().len())
}

/// Last error message (for STATUS_ERROR), as a NUL-terminated string.
#[no_mangle]
pub extern "C" fn rst_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}
