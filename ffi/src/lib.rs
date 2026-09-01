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
//! ## N Mojo driver threads
//!
//! That loop may be run by several Mojo threads at once, which is what makes a
//! handler able to `rst_call` another handler served by this same process. The
//! pieces that makes possible, all of them load bearing:
//!
//! - `job_rx` is a `Mutex<Receiver<Job>>` — the canonical N-consumer pattern
//!   over an mpsc receiver. One caller at a time waits in the receiver; the
//!   rest queue on the mutex. Nothing is lost and nothing is duplicated.
//! - the invocation map hands out `Arc<Mutex<Job>>`, and callers **clone the
//!   Arc under the map lock and then release it** before doing anything that
//!   blocks. Holding the map lock across a `rst_call` would serialise every
//!   driver thread behind the caller and reintroduce the very deadlock this
//!   exists to remove. One invocation is owned by one driver thread at a time,
//!   so the per-invocation mutex is uncontended; it is there because
//!   `mpsc::Receiver` is `Send` but not `Sync`.
//! - `SCRATCH` and `LAST_ERROR` are thread-locals, so the result buffer one
//!   driver thread reads back through `rst_buf_ptr` cannot be clobbered by
//!   another thread's operation.
//!
//! Shutdown: a thread parked in `rst_next` cannot see a flag in Mojo, so
//! `rst_stop` sets a process-wide flag here and `rst_next` waits on a short
//! timeout rather than indefinitely — see `rst_stop`.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc as smpsc, Arc, Mutex, OnceLock};
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
    AwakeableCreate,
    AwakeableAwait(String),
    AwakeableResolve(String, Vec<u8>),
    AwakeableReject(String, String),
    PromiseAwait(String),
    PromisePeek(String),
    PromiseResolve(String, Vec<u8>),
    PromiseReject(String, String),
    CancelInvocation(String),
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
    invocation_id: String,
    input: Vec<u8>,
    cmd_tx: tmpsc::UnboundedSender<Cmd>,
    reply_rx: smpsc::Receiver<Reply>,
}

struct Bridge {
    /// Held, never read: keeping a sender alive here is what stops the job
    /// channel from ever reporting `Disconnected`, so `rst_next` blocks rather
    /// than spinning when no invocation is in flight. It is also why dropping
    /// the sender is not available as a shutdown mechanism — hence `rst_stop`.
    #[allow(dead_code)]
    job_tx: smpsc::Sender<Job>,
    job_rx: Mutex<smpsc::Receiver<Job>>,
}

static BRIDGE: OnceLock<Bridge> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
/// Set by `rst_stop`; makes every `rst_next` caller return 0 so the Mojo
/// worker threads can leave their loops.
static STOPPING: AtomicBool = AtomicBool::new(false);

/// How long `rst_next` parks in the receiver before rechecking `STOPPING`.
/// A job still wakes it immediately — this only bounds shutdown latency.
const NEXT_POLL: Duration = Duration::from_millis(25);

/// One in-flight invocation, shared by handle.
///
/// `Arc` so a driver thread can keep working on it after the map lock is
/// released; `Mutex` because `mpsc::Receiver` is `Send` but not `Sync`, and
/// because one invocation must be driven by one thread at a time anyway.
type InvocationRef = Arc<Mutex<Job>>;

fn invocations() -> &'static Mutex<HashMap<u64, InvocationRef>> {
    static INVS: OnceLock<Mutex<HashMap<u64, InvocationRef>>> = OnceLock::new();
    INVS.get_or_init(|| Mutex::new(HashMap::new()))
}

// Per-thread scratch: the last result buffer and error text, read back via
// rst_buf_ptr/rst_buf_len and rst_last_error. Thread-local precisely so that N
// Mojo driver threads cannot clobber each other's results.
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
                    invocation_id: metadata.invocation_id.clone(),
                    input,
                    cmd_tx,
                    reply_rx,
                })
                .is_err()
            {
                // No Mojo driver: nothing we can do.
                return Ok(());
            }

            // Awakeables created but not yet awaited by the Mojo driver.
            // The futures borrow `ctx`, which outlives them in this block.
            let mut awakeables: HashMap<
                String,
                std::pin::Pin<Box<dyn futures::Future<Output = Result<Vec<u8>, TerminalError>> + Send + '_>>,
            > = HashMap::new();

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
                    Cmd::AwakeableCreate => {
                        let (id, fut) = ctx.awakeable::<Vec<u8>>();
                        awakeables.insert(id.clone(), Box::pin(fut));
                        let _ = reply_tx.send(Reply::Ok(id.into_bytes()));
                    }
                    Cmd::AwakeableAwait(id) => {
                        let reply = match awakeables.remove(&id) {
                            Some(fut) => match fut.await {
                                Ok(v) => Reply::Ok(v),
                                Err(te) => Reply::Terminal(te.to_string()),
                            },
                            None => Reply::Terminal(format!(
                                "restate.mojo protocol error: unknown awakeable '{id}' \
                                 (create it in this invocation first)"
                            )),
                        };
                        let _ = reply_tx.send(reply);
                    }
                    Cmd::AwakeableResolve(id, value) => {
                        ctx.resolve_awakeable::<Vec<u8>>(&id, value);
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::AwakeableReject(id, msg) => {
                        ctx.reject_awakeable(&id, TerminalError::new(msg));
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::PromiseAwait(name) => {
                        let r = ctx.promise::<Vec<u8>>(&name).await;
                        let _ = reply_tx.send(match r {
                            Ok(v) => Reply::Ok(v),
                            Err(te) => Reply::Terminal(te.to_string()),
                        });
                    }
                    Cmd::PromisePeek(name) => {
                        let r = ctx.peek_promise::<Vec<u8>>(&name).await;
                        let _ = reply_tx.send(match r {
                            Ok(Some(v)) => Reply::Ok(v),
                            Ok(None) => Reply::Empty,
                            Err(te) => Reply::Terminal(te.to_string()),
                        });
                    }
                    Cmd::PromiseResolve(name, value) => {
                        ctx.resolve_promise::<Vec<u8>>(&name, value);
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::PromiseReject(name, msg) => {
                        ctx.reject_promise(&name, TerminalError::new(msg));
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
                    }
                    Cmd::CancelInvocation(id) => {
                        ctx.cancel_invocation(&id);
                        let _ = reply_tx.send(Reply::Ok(Vec::new()));
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

/// service_ty: 0 = Service, 1 = Virtual Object, 2 = Workflow (the handler
/// named "run" is the workflow handler; all others are shared).
fn make_discovery(service_name: &str, service_ty: i32, handlers: &[String]) -> discovery::Service {
    discovery::Service {
        ty: match service_ty {
            1 => discovery::ServiceType::VirtualObject,
            2 => discovery::ServiceType::Workflow,
            _ => discovery::ServiceType::Service,
        },
        name: discovery::ServiceName::try_from(service_name.to_string())
            .expect("service name valid"),
        handlers: handlers
            .iter()
            .map(|h| discovery::Handler {
                name: discovery::HandlerName::try_from(h.as_str()).expect("handler name valid"),
                input: Some(discovery::InputPayload::from_metadata::<Vec<u8>>()),
                output: Some(discovery::OutputPayload::from_metadata::<Vec<u8>>()),
                ty: if service_ty == 2 {
                    if h == "run" {
                        Some(discovery::HandlerType::Workflow)
                    } else {
                        Some(discovery::HandlerType::Shared)
                    }
                } else {
                    None
                },
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
/// `service_ty`: 0 = Service, 1 = Virtual Object, 2 = Workflow (whose "run"
/// handler is the workflow handler; the rest are shared).
/// Non-blocking: the server runs on a background thread.
#[no_mangle]
pub extern "C" fn rst_serve(
    host_port: *const c_char,
    service_name: *const c_char,
    service_ty: i32,
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

    let discovery = make_discovery(&name, service_ty, &handlers);
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

/// Ask every `rst_next` caller to return 0, so the Mojo driver threads can
/// leave their serve loops. Idempotent, and safe to call from a handler.
///
/// This exists because a cooperative stop flag on the Mojo side cannot reach a
/// thread that is parked inside this shim's receiver: setting a flag does not
/// interrupt a blocking `recv`. The alternatives were dropping the sender
/// (impossible — `BRIDGE` is a `OnceLock` static, and the SDK service holds a
/// clone for the life of the endpoint) or pushing N sentinel jobs (requires
/// knowing N, and races a worker that is mid-invocation). A flag plus a short
/// receive timeout needs neither, and costs one wakeup every 25 ms per idle
/// worker. A job still arrives with no added latency.
#[no_mangle]
pub extern "C" fn rst_stop() -> i32 {
    STOPPING.store(true, Ordering::Release);
    STATUS_OK
}

/// Whether `rst_stop` has been called — lets the Mojo side tell an orderly
/// shutdown apart from a genuine `rst_next` failure.
#[no_mangle]
pub extern "C" fn rst_stopping() -> i32 {
    i32::from(STOPPING.load(Ordering::Acquire))
}

/// Block until the next invocation arrives; returns its handle (> 0), or 0 on
/// error (bridge not started / channel closed) or after `rst_stop`.
///
/// Safe to call from several threads at once: the receiver is behind a mutex,
/// so exactly one caller waits in it and the others queue on the lock. Each
/// invocation is handed to exactly one caller.
#[no_mangle]
pub extern "C" fn rst_next() -> u64 {
    let Some(bridge) = BRIDGE.get() else {
        set_error("rst_next: rst_serve was not called");
        return 0;
    };
    let job = loop {
        if STOPPING.load(Ordering::Acquire) {
            set_error("rst_next: stopping");
            return 0;
        }
        let rx = bridge.job_rx.lock().expect("job_rx lock");
        // Recheck after acquiring: a stop may have landed while we queued
        // behind another worker, and this keeps shutdown from costing one
        // NEXT_POLL per waiting thread.
        if STOPPING.load(Ordering::Acquire) {
            set_error("rst_next: stopping");
            return 0;
        }
        match rx.recv_timeout(NEXT_POLL) {
            Ok(j) => break j,
            Err(smpsc::RecvTimeoutError::Timeout) => continue,
            Err(smpsc::RecvTimeoutError::Disconnected) => {
                set_error("rst_next: endpoint stopped");
                return 0;
            }
        }
    };
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    invocations()
        .lock()
        .expect("invocations lock")
        .insert(id, Arc::new(Mutex::new(job)));
    id
}

fn with_invocation<F: FnOnce(&Job) -> i32>(handle: u64, f: F) -> i32 {
    // Clone the Arc under the map lock, then *release it* before running `f`.
    // `f` blocks — a `rst_call` waits for the callee to finish — so holding the
    // map lock here would serialise every driver thread behind this one and
    // put the deadlock straight back.
    let entry = {
        let invs = invocations().lock().expect("invocations lock");
        invs.get(&handle).cloned()
    };
    match entry {
        Some(inv) => {
            let job = inv.lock().expect("invocation lock");
            f(&job)
        }
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
fn roundtrip(job: &Job, cmd: Cmd) -> Result<Reply, i32> {
    if job.cmd_tx.send(cmd).is_err() {
        return Err(STATUS_GONE);
    }
    job.reply_rx.recv().map_err(|_| STATUS_GONE)
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
    with_invocation(handle, |job| match roundtrip(job, cmd) {
        Ok(reply) => reply_to_status(reply),
        Err(status) => status,
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_handler(handle: u64) -> i32 {
    with_invocation(handle, |job| {
        set_scratch(job.handler.clone().into_bytes());
        STATUS_OK
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_key(handle: u64) -> i32 {
    with_invocation(handle, |job| {
        set_scratch(job.key.clone().into_bytes());
        STATUS_OK
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_input(handle: u64) -> i32 {
    with_invocation(handle, |job| {
        set_scratch(job.input.clone());
        STATUS_OK
    })
}

#[no_mangle]
pub extern "C" fn rst_inv_id(handle: u64) -> i32 {
    with_invocation(handle, |job| {
        set_scratch(job.invocation_id.clone().into_bytes());
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

/// Create an awakeable: a durable, externally-resolvable promise. Returns
/// its id in rst_buf; hand the id to the outside world (via rst_run/rst_call),
/// then rst_awakeable_await it.
#[no_mangle]
pub extern "C" fn rst_awakeable_create(handle: u64) -> i32 {
    simple_op(handle, Cmd::AwakeableCreate)
}

/// Await an awakeable created in this invocation; its resolved payload lands
/// in rst_buf.
#[no_mangle]
pub extern "C" fn rst_awakeable_await(handle: u64, id: *const c_char) -> i32 {
    let Ok(id) = (unsafe { cstr(id) }) else {
        set_error("rst_awakeable_await: null id");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::AwakeableAwait(id))
}

/// Resolve someone else's awakeable with a payload (journaled).
#[no_mangle]
pub extern "C" fn rst_awakeable_resolve(
    handle: u64,
    id: *const c_char,
    value: *const u8,
    value_len: usize,
) -> i32 {
    let Ok(id) = (unsafe { cstr(id) }) else {
        set_error("rst_awakeable_resolve: null id");
        return STATUS_ERROR;
    };
    let value = unsafe { bytes(value, value_len) };
    simple_op(handle, Cmd::AwakeableResolve(id, value))
}

/// Reject someone else's awakeable with a terminal error (journaled).
#[no_mangle]
pub extern "C" fn rst_awakeable_reject(handle: u64, id: *const c_char, message: *const c_char) -> i32 {
    let (Ok(id), Ok(msg)) = (unsafe { cstr(id) }, unsafe { cstr(message) }) else {
        set_error("rst_awakeable_reject: null argument");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::AwakeableReject(id, msg))
}

/// Await a named workflow promise (workflow services only).
#[no_mangle]
pub extern "C" fn rst_promise_await(handle: u64, name: *const c_char) -> i32 {
    let Ok(name) = (unsafe { cstr(name) }) else {
        set_error("rst_promise_await: null name");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::PromiseAwait(name))
}

/// Peek a named workflow promise: STATUS_OK with the value, or STATUS_EMPTY.
#[no_mangle]
pub extern "C" fn rst_promise_peek(handle: u64, name: *const c_char) -> i32 {
    let Ok(name) = (unsafe { cstr(name) }) else {
        set_error("rst_promise_peek: null name");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::PromisePeek(name))
}

/// Resolve a named workflow promise (from a shared handler).
#[no_mangle]
pub extern "C" fn rst_promise_resolve(
    handle: u64,
    name: *const c_char,
    value: *const u8,
    value_len: usize,
) -> i32 {
    let Ok(name) = (unsafe { cstr(name) }) else {
        set_error("rst_promise_resolve: null name");
        return STATUS_ERROR;
    };
    let value = unsafe { bytes(value, value_len) };
    simple_op(handle, Cmd::PromiseResolve(name, value))
}

/// Reject a named workflow promise with a terminal error.
#[no_mangle]
pub extern "C" fn rst_promise_reject(
    handle: u64,
    name: *const c_char,
    message: *const c_char,
) -> i32 {
    let (Ok(name), Ok(msg)) = (unsafe { cstr(name) }, unsafe { cstr(message) }) else {
        set_error("rst_promise_reject: null argument");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::PromiseReject(name, msg))
}

/// Cancel another invocation by id (journaled).
#[no_mangle]
pub extern "C" fn rst_cancel_invocation(handle: u64, invocation_id: *const c_char) -> i32 {
    let Ok(id) = (unsafe { cstr(invocation_id) }) else {
        set_error("rst_cancel_invocation: null id");
        return STATUS_ERROR;
    };
    simple_op(handle, Cmd::CancelInvocation(id))
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
