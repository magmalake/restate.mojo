"""Chain — the served-mode example, and the deadlock regression test.

Run it (`pixi run chain`, or `./build/chain --workers 1 --port 19080`),
register the endpoint with a running Restate server, and invoke it:

    restate deployments register http://localhost:9080
    curl localhost:8080/Chain/chain -d '""'      # -> "chain(leaf:"from-chain")"

The handler that matters is `chain`: it `call`s `leaf`, **a handler served by
this same process**. Under the v0 single-threaded driver that deadlocked — the
loop was blocked waiting for the call while the callee waited for the loop —
and it is the reason `App.serve` exists. With `--workers 1` it still deadlocks,
which is the honest way to show what the second worker buys.

`hold` is the concurrency evidence. It spins for 400 ms of real time and tracks
how many invocations are in flight at once in process-local state (the `ctx`
pointer `serve` passes through); `peak` reports the high-water mark. Fire
several `hold`s at once and `peak` comes back > 1 — the driver genuinely
overlaps invocations rather than interleaving them.

This is a plain Service, not a Virtual Object, on purpose: Restate serialises
invocations per object key, so a self-call between two keyed handlers would
stall for reasons that have nothing to do with the Mojo driver.
"""

from std.memory import alloc
from std.sys import argv
from std.time import perf_counter_ns

from restate import App, Invocation
from threads import AtomicCounter, OpaquePtr, i64_ptr, num_cpus, opaque_ptr


# Process-local state, in 64-bit cells: shared by every handler invocation on
# every worker, and reached through the `ctx` pointer `serve` hands along.
comptime C_LIVE: Int = 0
"""How many `hold` invocations are executing right now."""
comptime C_PEAK: Int = 1
"""The most that were ever executing at once."""
comptime C_CELLS: Int = 2

comptime HOLD_NS: Int = 400_000_000
"""How long `hold` occupies its worker. Real time, not a durable sleep: a
durable sleep would be journaled and may suspend, which would hand the worker
back and defeat the measurement."""


def _handle(
    app: App, inv: Invocation, worker: Int, ctx: OpaquePtr
) raises -> None:
    """One invocation, on one of `serve`'s worker threads."""
    if inv.handler == "leaf":
        app.complete(inv, "leaf:" + inv.input_string())

    elif inv.handler == "chain":
        # The v0 deadlock, exactly: a handler calling a handler served by this
        # same process. One worker blocks here; another must pick up `leaf`.
        var got = app.call(inv, "Chain", "leaf", '"from-chain"')
        app.complete(inv, "chain(" + got + ")")

    elif inv.handler == "hold":
        var live = AtomicCounter.at(Int(ctx) + C_LIVE * 8)
        var peak = AtomicCounter.at(Int(ctx) + C_PEAK * 8)
        var now = Int(live.fetch_add(1)) + 1
        # Racy max, deliberately: a lost update can only *under*-report the
        # peak, so the assertion it feeds can never pass spuriously.
        if Int64(now) > peak.load():
            peak.store(Int64(now))
        var t0 = perf_counter_ns()
        while perf_counter_ns() - t0 < HOLD_NS:
            pass
        _ = live.fetch_add(-1)
        app.complete(inv, String(now))

    elif inv.handler == "peak":
        var peak = AtomicCounter.at(Int(ctx) + C_PEAK * 8)
        app.complete(inv, String(Int(peak.load())))

    elif inv.handler == "worker":
        app.complete(inv, String(worker))

    elif inv.handler == "shutdown":
        # `stop()` reaches every worker, including the ones parked in `next()`,
        # so `serve` returns. Complete this one first — after the stop, but the
        # invocation we are already holding is still ours to finish.
        app.stop()
        app.complete(inv, "stopping")

    else:
        app.fail(inv, "unknown handler: " + inv.handler)


def main() raises:
    var workers = num_cpus()
    var port = 9080
    var args = argv()
    var i = 1
    while i < len(args):
        var flag = String(args[i])
        if flag == "--workers" and i + 1 < len(args):
            workers = Int(String(args[i + 1]))
            i += 2
        elif flag == "--port" and i + 1 < len(args):
            port = Int(String(args[i + 1]))
            i += 2
        else:
            raise Error("unknown option: ", flag)

    var app = App(
        "Chain",
        ["leaf", "chain", "hold", "peak", "worker", "shutdown"],
        object=False,
        port=port,
    )

    # Heap allocated and freed after `serve` returns — `serve` joins every
    # worker before returning, so nothing can still be reading it.
    var block = alloc[Int64](C_CELLS)
    block[C_LIVE] = 0
    block[C_PEAK] = 0

    print(
        "Chain listening on :",
        port,
        " with ",
        workers,
        " worker(s) — register with `restate deployments register`",
        sep="",
    )
    var served = app.serve[_handle](
        num_workers=workers, ctx=opaque_ptr(Int(block))
    )
    print("stopped after", served, "invocations")
    block.unsafe_free()
