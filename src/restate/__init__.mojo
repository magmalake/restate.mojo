"""`restate` — durable execution flows in Mojo, via Restate (restate.dev).

The Rust shim (ffi/ -> librestatemojo.dylib) embeds the Restate Rust SDK: it
owns the HTTP/2 endpoint, the event loop, and the journal. Mojo owns the
business logic, driven by a synchronous loop:

    from restate import App

    def main() raises:
        var app = App("Counter", ["add", "get"], object=True)
        while True:
            var inv = app.next()
            try:
                if inv.handler == "add":
                    var n = app.get_state_int(inv, "count", 0) + 1
                    app.set_state_int(inv, "count", n)
                    app.complete(inv, String(n))
                elif inv.handler == "get":
                    app.complete(inv, String(app.get_state_int(inv, "count", 0)))
            except e:
                app.abandon(inv)
                if not is_suspended(e):
                    raise e

That loop is single-threaded, and a handler that `call`s another handler served
by the same process therefore deadlocks. `App.serve` is the concurrent form —
the same loop on N threads, via threads-mojo's `WorkerPool`:

    from restate import App, Invocation, OpaquePtr

    def handle(app: App, inv: Invocation, worker: Int, ctx: OpaquePtr) raises:
        app.complete(inv, "ok")

    def main() raises:
        var app = App("Counter", ["add", "get"], object=True)
        _ = app.serve[handle](num_workers=4)   # until app.stop()

With two or more workers one thread can wait for a call while another runs the
callee, so self-calls complete. See `HandlerFn` for the handler's obligations
and `App.stop` for how a worker parked in `next()` is released.

Semantics: on retry or resume, Restate re-invokes the handler and replays the
journal — ctx operations (`sleep`, `call`, `get_state`, `run_*`, ...) return
their journaled results instantly, so handler logic must be deterministic
between them; wrap non-deterministic work in `run_enter`/`run_exit`. When
Restate suspends an invocation (e.g. a long `sleep`), the pending operation
raises a suspension error: abandon the invocation and keep serving — Restate
re-delivers it later.

Payloads are raw bytes on the wire; the String helpers below treat them as
UTF-8 (use the `_bytes` variants for binary data).
"""

from std.memory import alloc
from std.sys.info import CompilationTarget
from std.ffi import OwnedDLHandle, c_char
from std.os import getenv

from threads import (
    AtomicCounter,
    AtomicFlag,
    OpaquePtr,
    WorkerPool,
    i64_ptr,
    num_cpus,
    opaque_ptr,
)

comptime STATUS_OK = 0
comptime STATUS_GONE = 1
comptime STATUS_TERMINAL = 2
comptime STATUS_ERROR = 3
comptime STATUS_EMPTY = 4
comptime STATUS_EXECUTE = 5

comptime SUSPENDED_MSG = "restate: invocation suspended"
comptime STOPPED_MSG = "restate: driver stopped"


def is_suspended(e: Error) -> Bool:
    """True if `e` is the suspension signal (abandon the invocation and keep
    serving)."""
    return String(e) == SUSPENDED_MSG


def is_stopped(e: Error) -> Bool:
    """True if `e` means `stop()` was called: `next()` will not deliver
    anything more, so leave the loop."""
    return String(e) == STOPPED_MSG


def _find_lib() -> String:
    """Path to librestatemojo: `$CONDA_PREFIX/lib` (installed by the shim
    package), else the local cargo build for a bare checkout."""
    var ext = String("dylib") if CompilationTarget.is_macos() else String("so")
    var prefix = getenv("CONDA_PREFIX", "")
    if prefix == "":
        return String("ffi/target/release/librestatemojo.") + ext
    return prefix + "/lib/librestatemojo." + ext


def _cstr(s: String) -> List[UInt8]:
    """A NUL-terminated byte buffer for `s`, to pass as a C `const char*`."""
    var b = List[UInt8]()
    var src = s.as_bytes()
    for i in range(len(src)):
        b.append(src[i])
    b.append(0)
    return b^


def _bytes_to_string(b: List[UInt8]) -> String:
    var out = String("")
    for i in range(len(b)):
        out += chr(Int(b[i]))
    return out^


def _string_to_bytes(s: String) -> List[UInt8]:
    var b = List[UInt8]()
    var src = s.as_bytes()
    for i in range(len(src)):
        b.append(src[i])
    return b^


@fieldwise_init
struct Invocation(Copyable, Movable):
    """One in-flight invocation: which handler was called, the virtual-object
    key (empty for plain services), and the request payload."""

    var handle: Int
    var handler: String
    var key: String
    var id: String
    var input: List[UInt8]

    def input_string(self) -> String:
        return _bytes_to_string(self.input)


struct App(Movable):
    """A Restate endpoint serving one service with byte/string handlers,
    driven by `next()`."""

    var lib: OwnedDLHandle

    def __init__(
        out self,
        service: String,
        handlers: List[String],
        object: Bool = True,
        workflow: Bool = False,
        host: String = "0.0.0.0",
        port: Int = 9080,
    ) raises:
        """Start the endpoint (background thread in the shim). `object=True`
        registers a Virtual Object — keyed, with state and single-writer
        concurrency per key; `False` a plain (stateless) Service.
        `workflow=True` registers a Workflow: the handler named "run" is the
        workflow handler (runs once per key), the rest are shared and may use
        the promise operations to signal it."""
        self.lib = OwnedDLHandle(_find_lib())
        var addr = host + ":" + String(port)
        var names = String("")
        for i in range(len(handlers)):
            if i > 0:
                names += ","
            names += handlers[i]
        var addr_c = _cstr(addr)
        var service_c = _cstr(service)
        var names_c = _cstr(names)
        var ty = 0
        if workflow:
            ty = 2
        elif object:
            ty = 1
        var serve_fn = self.lib.get_function[Int]("rst_serve")
        var status = serve_fn(
            Int(addr_c.unsafe_ptr()),
            Int(service_c.unsafe_ptr()),
            ty,
            Int(names_c.unsafe_ptr()),
        )
        _ = addr_c^
        _ = service_c^
        _ = names_c^
        if status != STATUS_OK:
            raise Error("rst_serve failed: " + self._last_error())

    def _last_error(self) raises -> String:
        var func = self.lib.get_function[Pointer[UInt8, MutAnyOrigin]](
            "rst_last_error"
        )
        var p = func()
        var out = String("")
        var i = 0
        while p[unsafe_offset=i] != 0 and i < 4096:
            out += chr(Int(p[unsafe_offset=i]))
            i += 1
        return out^

    def _buf(self) raises -> List[UInt8]:
        """Copy the shim's result buffer for the last operation."""
        var ptr_fn = self.lib.get_function[Pointer[UInt8, MutAnyOrigin]](
            "rst_buf_ptr"
        )
        var len_fn = self.lib.get_function[Int]("rst_buf_len")
        var n = len_fn()
        var p = ptr_fn()
        var out = List[UInt8]()
        for i in range(n):
            out.append(p[unsafe_offset=i])
        return out^

    def _check(self, status: Int, what: String) raises:
        """Raise on non-OK statuses shared by most operations."""
        if status == STATUS_OK:
            return
        if status == STATUS_GONE:
            raise Error(SUSPENDED_MSG)
        if status == STATUS_TERMINAL:
            raise Error(
                "restate terminal error in "
                + what
                + ": "
                + _bytes_to_string(self._buf())
            )
        raise Error("restate " + what + " failed: " + self._last_error())

    def next(self) raises -> Invocation:
        """Block until Restate delivers the next invocation.

        Safe to call from several threads at once — the shim's job receiver is
        behind a mutex and hands each invocation to exactly one caller. That is
        what `serve` is built on.

        Raises the `is_stopped` sentinel once `stop()` has been called, so a
        driver loop can tell an orderly shutdown from a real failure.
        """
        var next_fn = self.lib.get_function[Int]("rst_next")
        var handle = next_fn()
        if handle == 0:
            if self.is_stopping():
                raise Error(STOPPED_MSG)
            raise Error("rst_next failed: " + self._last_error())
        var handler_fn = self.lib.get_function[Int]("rst_inv_handler")
        self._check(handler_fn(handle), "inv_handler")
        var handler = _bytes_to_string(self._buf())
        var key_fn = self.lib.get_function[Int]("rst_inv_key")
        self._check(key_fn(handle), "inv_key")
        var key = _bytes_to_string(self._buf())
        var id_fn = self.lib.get_function[Int]("rst_inv_id")
        self._check(id_fn(handle), "inv_id")
        var inv_id = _bytes_to_string(self._buf())
        var input_fn = self.lib.get_function[Int]("rst_inv_input")
        self._check(input_fn(handle), "inv_input")
        return Invocation(handle, handler^, key^, inv_id^, self._buf())

    # ── durable context operations ─────────────────────────────────────────

    def sleep_ms(self, inv: Invocation, millis: Int) raises:
        """Durable sleep. Journaled; long sleeps suspend the invocation."""
        var func = self.lib.get_function[Int]("rst_sleep")
        self._check(func(inv.handle, millis), "sleep")

    def get_state_bytes(
        self, inv: Invocation, key: String
    ) raises -> Optional[List[UInt8]]:
        var key_c = _cstr(key)
        var func = self.lib.get_function[Int]("rst_get_state")
        var status = func(inv.handle, Int(key_c.unsafe_ptr()))
        _ = key_c^
        if status == STATUS_EMPTY:
            return None
        self._check(status, "get_state")
        return self._buf()

    def get_state(
        self, inv: Invocation, key: String
    ) raises -> Optional[String]:
        var b = self.get_state_bytes(inv, key)
        if b:
            return _bytes_to_string(b.value())
        return None

    def get_state_int(
        self, inv: Invocation, key: String, default: Int
    ) raises -> Int:
        var s = self.get_state(inv, key)
        if s:
            return Int(s.value())
        return default

    def set_state_bytes(
        self, inv: Invocation, key: String, value: List[UInt8]
    ) raises:
        var key_c = _cstr(key)
        var func = self.lib.get_function[Int]("rst_set_state")
        var status = func(
            inv.handle,
            Int(key_c.unsafe_ptr()),
            Int(value.unsafe_ptr()),
            len(value),
        )
        _ = key_c^
        self._check(status, "set_state")

    def set_state(self, inv: Invocation, key: String, value: String) raises:
        self.set_state_bytes(inv, key, _string_to_bytes(value))

    def set_state_int(self, inv: Invocation, key: String, value: Int) raises:
        self.set_state(inv, key, String(value))

    def clear_state(self, inv: Invocation, key: String) raises:
        var key_c = _cstr(key)
        var func = self.lib.get_function[Int]("rst_clear_state")
        var status = func(inv.handle, Int(key_c.unsafe_ptr()))
        _ = key_c^
        self._check(status, "clear_state")

    def call(
        self,
        inv: Invocation,
        service: String,
        handler: String,
        payload: String,
        key: String = "",
    ) raises -> String:
        """Durable request/response call to another handler. A non-empty
        `key` targets a virtual object; empty targets a plain service."""
        var service_c = _cstr(service)
        var handler_c = _cstr(handler)
        var payload_b = _string_to_bytes(payload)
        var func = self.lib.get_function[Int]("rst_call")
        var status: Int
        if key == "":
            status = func(
                inv.handle,
                Int(service_c.unsafe_ptr()),
                Int(handler_c.unsafe_ptr()),
                0,
                Int(payload_b.unsafe_ptr()),
                len(payload_b),
            )
        else:
            var key_c = _cstr(key)
            status = func(
                inv.handle,
                Int(service_c.unsafe_ptr()),
                Int(handler_c.unsafe_ptr()),
                Int(key_c.unsafe_ptr()),
                Int(payload_b.unsafe_ptr()),
                len(payload_b),
            )
            _ = key_c^
        _ = service_c^
        _ = handler_c^
        _ = payload_b^
        self._check(status, "call")
        return _bytes_to_string(self._buf())

    def send(
        self,
        inv: Invocation,
        service: String,
        handler: String,
        payload: String,
        key: String = "",
        delay_ms: Int = 0,
    ) raises:
        """Durable one-way message (optionally delayed)."""
        var service_c = _cstr(service)
        var handler_c = _cstr(handler)
        var payload_b = _string_to_bytes(payload)
        var func = self.lib.get_function[Int]("rst_send")
        var status: Int
        if key == "":
            status = func(
                inv.handle,
                Int(service_c.unsafe_ptr()),
                Int(handler_c.unsafe_ptr()),
                0,
                Int(payload_b.unsafe_ptr()),
                len(payload_b),
                delay_ms,
            )
        else:
            var key_c = _cstr(key)
            status = func(
                inv.handle,
                Int(service_c.unsafe_ptr()),
                Int(handler_c.unsafe_ptr()),
                Int(key_c.unsafe_ptr()),
                Int(payload_b.unsafe_ptr()),
                len(payload_b),
                delay_ms,
            )
            _ = key_c^
        _ = service_c^
        _ = handler_c^
        _ = payload_b^
        self._check(status, "send")

    def awakeable_create(self, inv: Invocation) raises -> String:
        """Create an awakeable — a durable promise resolvable from outside
        (another service, or the ingress HTTP API). Returns its id: hand it
        to the outside world (inside `run_enter`/`run_exit` or via `call`),
        then `awakeable_await` it."""
        var func = self.lib.get_function[Int]("rst_awakeable_create")
        self._check(func(inv.handle), "awakeable_create")
        return _bytes_to_string(self._buf())

    def awakeable_await(self, inv: Invocation, id: String) raises -> String:
        """Await an awakeable created in this invocation; returns its
        resolved payload. Suspends if unresolved."""
        var id_c = _cstr(id)
        var func = self.lib.get_function[Int]("rst_awakeable_await")
        var status = func(inv.handle, Int(id_c.unsafe_ptr()))
        _ = id_c^
        self._check(status, "awakeable_await")
        return _bytes_to_string(self._buf())

    def awakeable_resolve(
        self, inv: Invocation, id: String, value: String
    ) raises:
        """Resolve another invocation's awakeable with a payload."""
        var id_c = _cstr(id)
        var value_b = _string_to_bytes(value)
        var func = self.lib.get_function[Int]("rst_awakeable_resolve")
        var status = func(
            inv.handle,
            Int(id_c.unsafe_ptr()),
            Int(value_b.unsafe_ptr()),
            len(value_b),
        )
        _ = id_c^
        _ = value_b^
        self._check(status, "awakeable_resolve")

    def awakeable_reject(
        self, inv: Invocation, id: String, message: String
    ) raises:
        """Reject another invocation's awakeable with a terminal error."""
        var id_c = _cstr(id)
        var message_c = _cstr(message)
        var func = self.lib.get_function[Int]("rst_awakeable_reject")
        var status = func(
            inv.handle, Int(id_c.unsafe_ptr()), Int(message_c.unsafe_ptr())
        )
        _ = id_c^
        _ = message_c^
        self._check(status, "awakeable_reject")

    def promise_await(self, inv: Invocation, name: String) raises -> String:
        """Await a named workflow promise (workflow services only)."""
        var name_c = _cstr(name)
        var func = self.lib.get_function[Int]("rst_promise_await")
        var status = func(inv.handle, Int(name_c.unsafe_ptr()))
        _ = name_c^
        self._check(status, "promise_await")
        return _bytes_to_string(self._buf())

    def promise_peek(
        self, inv: Invocation, name: String
    ) raises -> Optional[String]:
        """Peek a named workflow promise: its value, or None if unresolved."""
        var name_c = _cstr(name)
        var func = self.lib.get_function[Int]("rst_promise_peek")
        var status = func(inv.handle, Int(name_c.unsafe_ptr()))
        _ = name_c^
        if status == STATUS_EMPTY:
            return None
        self._check(status, "promise_peek")
        return _bytes_to_string(self._buf())

    def promise_resolve(
        self, inv: Invocation, name: String, value: String
    ) raises:
        """Resolve a named workflow promise (from a shared handler)."""
        var name_c = _cstr(name)
        var value_b = _string_to_bytes(value)
        var func = self.lib.get_function[Int]("rst_promise_resolve")
        var status = func(
            inv.handle,
            Int(name_c.unsafe_ptr()),
            Int(value_b.unsafe_ptr()),
            len(value_b),
        )
        _ = name_c^
        _ = value_b^
        self._check(status, "promise_resolve")

    def promise_reject(
        self, inv: Invocation, name: String, message: String
    ) raises:
        """Reject a named workflow promise with a terminal error."""
        var name_c = _cstr(name)
        var message_c = _cstr(message)
        var func = self.lib.get_function[Int]("rst_promise_reject")
        var status = func(
            inv.handle, Int(name_c.unsafe_ptr()), Int(message_c.unsafe_ptr())
        )
        _ = name_c^
        _ = message_c^
        self._check(status, "promise_reject")

    def cancel_invocation(self, inv: Invocation, invocation_id: String) raises:
        """Cancel another invocation by id."""
        var id_c = _cstr(invocation_id)
        var func = self.lib.get_function[Int]("rst_cancel_invocation")
        var status = func(inv.handle, Int(id_c.unsafe_ptr()))
        _ = id_c^
        self._check(status, "cancel_invocation")

    def run_enter(self, inv: Invocation) raises -> Optional[String]:
        """Enter a journaled side-effect block. Returns the journaled value
        on replay; None means: execute the side effect now, then close the
        block with `run_exit` (it succeeded) or `run_fail` (it did not).

        Exactly one of those must follow. Leaving the block open makes the
        invocation unreplayable: every later attempt raises "protocol error:
        expected rst_run_exit"."""
        var func = self.lib.get_function[Int]("rst_run_enter")
        var status = func(inv.handle)
        if status == STATUS_EXECUTE:
            return None
        self._check(status, "run_enter")
        return _bytes_to_string(self._buf())

    def run_exit(self, inv: Invocation, value: String) raises -> String:
        """Journal the side-effect result produced after `run_enter`
        returned None. Returns the journaled value."""
        var value_b = _string_to_bytes(value)
        var func = self.lib.get_function[Int]("rst_run_exit")
        var status = func(inv.handle, Int(value_b.unsafe_ptr()), len(value_b))
        _ = value_b^
        self._check(status, "run_exit")
        return _bytes_to_string(self._buf())

    def run_enter_policy(
        self,
        inv: Invocation,
        initial_delay_ms: Int = 0,
        factor: Float64 = 0.0,
        max_delay_ms: Int = 0,
        max_attempts: Int = 0,
        max_duration_ms: Int = 0,
    ) raises -> Optional[String]:
        """`run_enter`, with a retry policy for this block's non-terminal
        failures.

        Plain `run_enter` leaves retries to Restate's server-side invoker
        policy, which retries indefinitely. Set any of these to bound it; 0
        means "leave that knob alone", and `max_attempts` or `max_duration_ms`
        of 0 mean unbounded.

        When attempts or duration run out the SDK fails the block terminally,
        which arrives here the same way `run_fail(terminal=True)` does — as a
        raised error the handler can compensate for.
        """
        var func = self.lib.get_function[Int]("rst_run_enter_policy")
        var status = func(
            inv.handle,
            initial_delay_ms,
            Float32(factor),
            max_delay_ms,
            max_attempts,
            max_duration_ms,
        )
        if status == STATUS_EXECUTE:
            return None
        self._check(status, "run_enter_policy")
        return _bytes_to_string(self._buf())

    def run_fail(
        self, inv: Invocation, message: String, terminal: Bool = True
    ) raises -> Optional[String]:
        """Abort the run block opened by `run_enter`, rather than closing it
        with `run_exit`.

        Every `run_enter` that returns None must be closed by exactly one of
        `run_exit` or `run_fail`. Leaving one open makes the invocation
        unreplayable: the next attempt raises "protocol error: expected
        rst_run_exit" and keeps doing so forever.

        `terminal=True` journals the failure. The step is recorded as failed,
        replay reproduces the failure instead of executing again, and this
        raises so the handler can compensate and finish.

        `terminal=False` journals nothing and lets Restate re-run the block
        under its own retry policy. The return value is then the next
        execute slot: `None` means "run the side effect again", and a value
        means the journal answered in the meantime.
        """
        var message_c = _cstr(message)
        var func = self.lib.get_function[Int]("rst_run_fail")
        var status = func(inv.handle, Int(message_c.unsafe_ptr()), 1 if terminal else 0)
        _ = message_c^
        if status == STATUS_EXECUTE:
            return None
        self._check(status, "run_fail")
        return _bytes_to_string(self._buf())

    # ── finishing an invocation ────────────────────────────────────────────

    def complete(self, inv: Invocation, output: String) raises:
        """Finish the invocation successfully with `output`."""
        var output_b = _string_to_bytes(output)
        var func = self.lib.get_function[Int]("rst_complete")
        var status = func(inv.handle, Int(output_b.unsafe_ptr()), len(output_b))
        _ = output_b^
        self._check(status, "complete")

    def fail(self, inv: Invocation, message: String) raises:
        """Finish the invocation with a terminal error (not retried)."""
        var message_c = _cstr(message)
        var func = self.lib.get_function[Int]("rst_fail")
        var status = func(inv.handle, Int(message_c.unsafe_ptr()))
        _ = message_c^
        self._check(status, "fail")

    def abandon(self, inv: Invocation):
        """Drop an invocation without completing it (after suspension or an
        internal error). Restate retries/resumes it with journal replay."""
        try:
            var func = self.lib.get_function[Int]("rst_abandon")
            _ = func(inv.handle)
        except:
            pass

    # ── shutdown ───────────────────────────────────────────────────────────

    def stop(self) raises:
        """Unblock every `next()` — in this thread or any other — so driver
        loops can end.

        Setting a flag in Mojo cannot reach a thread parked inside the shim's
        blocking receiver, so this goes through the shim (`rst_stop`), which
        sets a process-wide flag its receive loop rechecks. Idempotent, and
        safe to call from inside a handler: that is how a service gives itself
        a `shutdown` endpoint.

        The endpoint itself keeps listening — this ends the Mojo driver, not
        the HTTP server. There is no un-stop.
        """
        var func = self.lib.get_function[Int]("rst_stop")
        _ = func()

    def is_stopping(self) raises -> Bool:
        """Whether `stop()` has been called.

        Returns:
            True once a stop has been requested.
        """
        var func = self.lib.get_function[Int]("rst_stopping")
        return func() != 0

    # ── served mode ────────────────────────────────────────────────────────

    def serve[
        handler: HandlerFn
    ](
        self,
        num_workers: Int = 0,
        ctx: OpaquePtr = opaque_ptr(0),
    ) raises -> Int:
        """Run `handler` on `num_workers` threads until `stop()` is called.

        The concurrent counterpart to the `while True: app.next()` loop, and
        the reason a handler may now `call` another handler served by this same
        process: with two or more workers, one thread waits for the callee
        while another picks it up. With one worker that is still a deadlock,
        exactly as before.

        Blocks until every worker has left its loop, then returns.

        Parameters:
            handler: The handler body — `def(App, Invocation, Int, OpaquePtr)
                thin raises -> None`, receiving the app, the invocation, its
                worker index, and `ctx`. Thin (non-capturing), because it is
                reached through a thread start routine; see the module
                docstring for the shape and the obligations.

        Args:
            num_workers: How many driver threads. `0` (the default) means
                `num_cpus()`. **Use at least 2** if any handler calls back into
                this process.
            ctx: Process-local state shared by every handler invocation,
                passed straight through. Must outlive the `serve` call — which
                it does automatically, since `serve` joins before returning.
                Defaults to a null pointer for handlers that need none.

        Returns:
            How many invocations completed without raising.

        Raises:
            Error: If the workers cannot be started or joined.
        """
        var n = num_workers if num_workers > 0 else num_cpus()
        if n < 1:
            n = 1

        # Heap allocated, and freed only after the join: the workers read it
        # for as long as they run, and Mojo destroys a local at its last *use*.
        var block = alloc[Int64](_SERVE_CELLS)
        var cells = i64_ptr(Int(block))
        # The App's own address. Every worker borrows it back through a
        # pointer rather than receiving a copy — `App` owns an `OwnedDLHandle`,
        # and a copy (or a move) would let one worker `dlclose` the library
        # while another is inside a call through it.
        cells[_SERVE_APP] = Int64(Int(UnsafePointer(to=self)))
        cells[_SERVE_USER] = Int64(Int(ctx))
        cells[_SERVE_SERVED] = 0
        cells[_SERVE_ERRORS] = 0
        var shared = opaque_ptr(Int(block))

        var pool: WorkerPool
        try:
            pool = WorkerPool.start[_serve_worker[handler]](n, shared)
        except e:
            block.unsafe_free()
            raise Error("restate serve could not start workers: ", String(e))

        # No `request_stop()` here: the workers leave when `next()` reports the
        # stop, which is the only signal that can reach a thread parked in the
        # shim's receiver. The pool's flag would never be observed by a worker
        # blocked in that call.
        try:
            pool.join()
        except e:
            block.unsafe_free()
            raise Error("restate serve could not join workers: ", String(e))

        var served = Int(cells[_SERVE_SERVED])
        block.unsafe_free()
        return served


# ── served mode: the handler contract and its plumbing ──────────────────────


comptime HandlerFn = def(App, Invocation, Int, OpaquePtr) thin raises -> None
"""What `App.serve` runs for each invocation: `(app, inv, worker, ctx)`.

**Thin**, i.e. non-capturing — it is reached through a pthread start routine,
which takes a C function pointer and one `void *`. Anything the handler needs
beyond the app and the invocation travels through `ctx`, exactly as in
`threads.parallel_for`.

It *may* raise, unlike a raw thread body: `serve` wraps each call, so a raise
is caught, the invocation is `abandon`ed (Restate re-delivers it), and the
worker carries on. Suspension arrives this way too — `is_suspended(e)` — and is
not logged, because it is normal.

A handler must finish its invocation: `complete`, `fail`, or raise. Returning
without doing any of those leaks the invocation handle in the shim and leaves
Restate waiting.
"""


# Worker context layout for `serve`, in 64-bit cells.
comptime _SERVE_APP: Int = 0
"""Address of the caller's `App` — borrowed, never copied."""
comptime _SERVE_USER: Int = 1
"""Address of the caller's own context, passed through to the handler."""
comptime _SERVE_SERVED: Int = 2
"""Atomic count of invocations that completed without raising."""
comptime _SERVE_ERRORS: Int = 3
"""Atomic count of invocations whose handler raised."""
comptime _SERVE_CELLS: Int = 4


def _serve_one[
    handler: HandlerFn
](
    app: App,
    var inv: Invocation,
    worker: Int,
    user_ctx: OpaquePtr,
    served: AtomicCounter,
    errors: AtomicCounter,
) -> None:
    """Run one invocation and swallow whatever it throws.

    `app` is a plain borrow. That is the whole point of this function existing
    separately: the worker holds only a raw address for the `App`, and passing
    `app_ptr[]` into a borrowing parameter is what keeps the `OwnedDLHandle`
    from being moved out from under the other workers.
    """
    try:
        handler(app, inv, worker, user_ctx)
        _ = served.fetch_add(1)
    except e:
        # Suspension is routine: Restate parked the invocation and will
        # re-deliver it with journal replay. Anything else is worth seeing.
        app.abandon(inv)
        _ = errors.fetch_add(1)
        if not is_suspended(e):
            print("restate: handler error in", inv.handler, "-", e)


def _serve_worker[
    handler: HandlerFn
](worker: Int, ctx: OpaquePtr, stop: AtomicFlag) -> None:
    """One driver thread: pull invocations and run them until stopped."""
    var cells = i64_ptr(Int(ctx))
    var app = UnsafePointer[App, MutUntrackedOrigin](
        unsafe_from_address=Int(cells[_SERVE_APP])
    )
    var user_ctx = opaque_ptr(Int(cells[_SERVE_USER]))
    var served = AtomicCounter.at(Int(ctx) + _SERVE_SERVED * 8)
    var errors = AtomicCounter.at(Int(ctx) + _SERVE_ERRORS * 8)
    while not stop.is_set():
        try:
            var inv = app[].next()
            _serve_one[handler](app[], inv^, worker, user_ctx, served, errors)
        except:
            # `next()` raised: either `stop()` was called, or the endpoint
            # died. Both mean this worker is finished. The pool's own stop flag
            # is checked above but can never be what releases a worker parked
            # in `next()` — see `App.stop`.
            break
