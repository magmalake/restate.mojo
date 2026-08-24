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

from std.ffi import OwnedDLHandle, c_char
from std.os import getenv

comptime STATUS_OK = 0
comptime STATUS_GONE = 1
comptime STATUS_TERMINAL = 2
comptime STATUS_ERROR = 3
comptime STATUS_EMPTY = 4
comptime STATUS_EXECUTE = 5

comptime SUSPENDED_MSG = "restate: invocation suspended"


def is_suspended(e: Error) -> Bool:
    """True if `e` is the suspension signal (abandon the invocation and keep
    serving)."""
    return String(e) == SUSPENDED_MSG


def _find_lib() -> String:
    """Path to librestatemojo: `$CONDA_PREFIX/lib` (installed by the shim
    package), else the local cargo build for a bare checkout."""
    var ext = String("dylib")  # ffi/ emits .so on Linux
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
        host: String = "0.0.0.0",
        port: Int = 9080,
    ) raises:
        """Start the endpoint (background thread in the shim). `object=True`
        registers a Virtual Object — keyed, with state and single-writer
        concurrency per key; `False` a plain (stateless) Service."""
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
        var serve_fn = self.lib.get_function[Int]("rst_serve")
        var status = serve_fn(
            Int(addr_c.unsafe_ptr()),
            Int(service_c.unsafe_ptr()),
            Int(1) if object else Int(0),
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
        """Block until Restate delivers the next invocation."""
        var next_fn = self.lib.get_function[Int]("rst_next")
        var handle = next_fn()
        if handle == 0:
            raise Error("rst_next failed: " + self._last_error())
        var handler_fn = self.lib.get_function[Int]("rst_inv_handler")
        self._check(handler_fn(handle), "inv_handler")
        var handler = _bytes_to_string(self._buf())
        var key_fn = self.lib.get_function[Int]("rst_inv_key")
        self._check(key_fn(handle), "inv_key")
        var key = _bytes_to_string(self._buf())
        var input_fn = self.lib.get_function[Int]("rst_inv_input")
        self._check(input_fn(handle), "inv_input")
        return Invocation(handle, handler^, key^, self._buf())

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

    def get_state(self, inv: Invocation, key: String) raises -> Optional[String]:
        var b = self.get_state_bytes(inv, key)
        if b:
            return _bytes_to_string(b.value())
        return None

    def get_state_int(self, inv: Invocation, key: String, default: Int) raises -> Int:
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
            inv.handle, Int(key_c.unsafe_ptr()), Int(value.unsafe_ptr()), len(value)
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

    def run_enter(self, inv: Invocation) raises -> Optional[String]:
        """Enter a journaled side-effect block. Returns the journaled value
        on replay; None means: execute the side effect now, then call
        `run_exit` with its result."""
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
