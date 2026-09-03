"""Failures — what durable execution actually buys you.

A three-step order flow whose middle step fails, so you can watch the steps
that already succeeded *not* run again. That is the whole idea, and it is much
easier to believe from a terminal than from a paragraph.

    pixi run failures                                   # terminal 1
    restate deployments register http://localhost:9080  # terminal 2
    curl localhost:8080/Orders/order-1/process --json '{}'

The `--json` matters: a bare POST is rejected by the ingress with
"Empty content-type" before it ever reaches your handler.

Real output, with `--fail 2`:

    --- attempt 1 at t+0s
    [attempt 1] execute  reserve  -> res-order-1-c4ea8c
    [attempt 1] execute  charge   -> boom: card declined (retrying in-place)
    --- attempt 2 at t+0s
    [attempt 2] REPLAY   reserve  -> res-order-1-c4ea8c
    [attempt 2] execute  charge   -> boom: card declined (retrying in-place)
    --- attempt 3 at t+2s
    [attempt 3] REPLAY   reserve  -> res-order-1-c4ea8c
    [attempt 3] execute  charge   -> chg-order-1-b40aa
    [attempt 3] execute  ship     -> shipped
    [attempt 3] done

`reserve` ran once across three attempts. Its value ends in a nanosecond
timestamp, so a second execution could not have produced the same string —
that it is identical is the journal, not luck.

## Bounding the retries

Plain `run_enter` leaves retries to Restate's invoker policy, which keeps
going indefinitely. `run_enter_policy` bounds them — `notify` allows three
attempts a quarter-second apart, then the SDK fails the block terminally and
the handler carries on without the notification:

    --- notify attempt 5
    [attempt 5] execute  notify   -> smtp timeout
    --- notify attempt 6
    [attempt 6] execute  notify   -> smtp timeout
    --- notify attempt 7
    [attempt 7] execute  notify   -> smtp timeout
      gave up after 3 tries: Terminal error [500]: smtp timeout

Right for a courtesy email; wrong for a payment, which is why it is not the
default.

**A suspension is not a failure.** Between attempts the invocation can be
parked waiting on a durable timer, and that arrives as an exception too. Catch
it as if the step had failed and you will compensate for work that was still
in progress — `refund` and `notify` both re-raise it via `is_suspended` so the
outer handler abandons and Restate resumes.

## Three ways to fail, and they are not interchangeable

- **`run_fail(inv, msg, terminal=False)`** — the step failed, try it again.
  Nothing is journaled and Restate re-runs the block. `charge` uses this.
- **`run_fail(inv, msg, terminal=True)`** — the step failed for good. The
  failure is journaled and raised so the handler can compensate and finish.
  `refund` uses this.
- **`app.fail(inv, msg)`** — the whole invocation is a lost cause. The caller
  gets the error, nothing is retried. `validate` uses this on malformed input,
  which will not become well-formed by trying again.

Every `run_enter` that returns None must be closed by exactly one of
`run_exit` or `run_fail`. Leaving one open used to be the only thing you could
do when a side effect failed, and it made the invocation unreplayable —
every later attempt died with "protocol error: expected rst_run_exit".

    curl localhost:8080/Orders/order-3/refund --json '{}'
    refund failed, credit note issued

    curl localhost:8080/Orders/order-2/validate --json '""'
    {"code":500,"message":"order is empty","source":"invocation"}

## Handlers

    process    the three-step flow above
    refund     a step that fails terminally, and compensation
    notify     bounded retries: three attempts, then give up and carry on
    validate   terminal invocation failure on bad input, never retried
    status     what the object recorded, so you can watch state survive
    arm        re-arm the flaky charge and run the demo again

## Flags

    --port N   endpoint port (default 9080)
    --fail N   how many times charge fails first (default 1)
"""

from std.sys import argv
from std.time import perf_counter_ns

from restate import App, Ctx, Invocation, is_suspended
from threads import AtomicCounter


@fieldwise_init
struct Flow(Copyable, Movable):
    """Process-local state, shared by every worker.

    In the old single-threaded driver these were locals in `main`. They are
    atomics now because `serve` runs handlers on several threads at once and
    `Ctx` types that sharing without synchronising it.
    """

    var failures_left: Int64
    """How many more times `charge` should throw. Stands in for a flaky
    downstream, which is why it is not in Restate state: a real outage is not
    part of your journal."""
    var attempt: Int64
    """Attempts across all invocations, so the log reads as a story."""
    var fail_times: Int64
    """What `arm` resets `failures_left` to."""
    var t0: Int64
    """Process start, for the t+Ns column."""


def _cell(ref c: Int64) -> AtomicCounter:
    """An atomic view over one field, by name rather than by offset."""
    return AtomicCounter.at(Int(Pointer(to=c)))

comptime DEFAULT_FAILURES = 1
"""How many times `charge` throws before it succeeds, unless `--fail` says
otherwise. One by default because Restate re-delivers roughly once a minute:
two failures means a two-minute demo, and one is enough to show replay."""


def _short_id(prefix: String, key: String) -> String:
    """A value that cannot be reproduced by a second execution.

    That is the point: if the same string comes back on a retry, it came from
    the journal rather than from running this again.
    """
    var n = perf_counter_ns() % 0xFFFFFF
    return String(prefix, "-", key, "-", hex(n)[byte=2:])


def _step_reserve(app: App, inv: Invocation, attempt: Int) raises -> String:
    """Deliberately the long form.

    `step` handles the enter/exit protocol for you, and in exchange does not
    tell you which branch it took -- there is no way to print REPLAY from
    outside it. This is the step whose replay the demo exists to show, so it
    keeps the protocol. Every other step below uses `step`.
    """
    var replayed = app.run_enter(inv)
    if replayed:
        print("[attempt ", attempt, "] REPLAY   reserve  -> ", replayed.value(), sep="")
        return replayed.value()
    var value = _short_id("res", inv.key)
    print("[attempt ", attempt, "] execute  reserve  -> ", value, sep="")
    return app.run_exit(inv, value)


def _step_charge(
    app: App, inv: Invocation, attempt: Int, ctx: Ctx[Flow]
) raises -> String:
    """The flaky step. Raising inside the closure closes the block as a
    non-terminal failure, so Restate runs it again -- earlier steps untouched,
    because the handler never returned."""
    var key = inv.key
    var left = _cell(ctx[].failures_left)

    @parameter
    def compute() raises -> String:
        if left.fetch_add(-1) > 0:
            print(
                "[attempt ", attempt,
                "] execute  charge   -> boom: card declined (retrying in-place)",
                sep="",
            )
            raise Error("card declined")
        var value = _short_id("chg", key)
        print("[attempt ", attempt, "] execute  charge   -> ", value, sep="")
        return value

    return app.step[compute](inv)


def _step_notify(app: App, inv: Invocation, attempt: Int) raises -> String:
    """Bounded: three attempts a quarter-second apart, then the step fails
    terminally and the handler carries on without the notification."""

    @parameter
    def compute() raises -> String:
        print("[attempt ", attempt, "] execute  notify   -> smtp timeout", sep="")
        raise Error("smtp timeout")

    return app.step[compute](inv, initial_delay_ms=250, max_attempts=3)


def _step_ship(app: App, inv: Invocation, attempt: Int) raises -> String:
    @parameter
    def compute() raises -> String:
        print("[attempt ", attempt, "] execute  ship     -> shipped", sep="")
        return String("shipped")

    return app.step[compute](inv)


def _step_refund(app: App, inv: Invocation, attempt: Int) raises -> String:
    """A step that fails for good.

    `run_fail(terminal=True)` journals the failure and raises it, so the
    handler can compensate and finish. The contrast with `_step_charge` is the
    whole point: a non-terminal failure re-runs the block, a terminal one ends
    it, and choosing wrong means either retrying a dead endpoint forever or
    giving up on a blip.
    """
    var slot = app.run_enter(inv)
    if slot:
        print("[attempt ", attempt, "] REPLAY   refund   -> ", slot.value(), sep="")
        return slot.value()
    print("[attempt ", attempt, "] execute  refund   -> gateway says no, permanently", sep="")
    _ = app.run_fail(inv, String("refund rejected: account closed"), terminal=True)
    raise Error("unreachable: run_fail(terminal=True) raises")


def handle_process(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Flow]
) raises -> None:
    var attempt = Int(_cell(ctx[].attempt).fetch_add(1)) + 1
    var elapsed = (perf_counter_ns() - Int(ctx[].t0)) // 1_000_000_000
    print("--- attempt ", attempt, " at t+", elapsed, "s", sep="")

    var reserved = _step_reserve(app, inv, attempt)
    var charged = _step_charge(app, inv, attempt, ctx)
    var shipped = _step_ship(app, inv, attempt)

    app.set_state(inv, "reservation", reserved)
    app.set_state(inv, "charge", charged)
    app.set_state(inv, "shipment", shipped)
    app.set_state_int(inv, "attempts", attempt)

    print("[attempt ", attempt, "] done", sep="")
    print()
    app.complete(inv, String(reserved, " / ", charged, " / ", shipped))


def handle_refund(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Flow]
) raises -> None:
    """A step that fails for good, journaled as failed, then compensated."""
    var attempt = Int(_cell(ctx[].attempt).fetch_add(1)) + 1
    print("--- refund attempt ", attempt, sep="")
    try:
        app.complete(inv, _step_refund(app, inv, attempt))
    except e:
        # A suspension is not a failure: the invocation is parked waiting on
        # something durable and will be resumed. Catch it as if the step had
        # failed and you compensate for work that is merely in progress.
        if is_suspended(e):
            raise e
        print("  compensating: ", e, sep="")
        app.set_state(inv, "refund", String("failed: ", e))
        app.complete(inv, String("refund failed, credit note issued"))


def handle_notify(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Flow]
) raises -> None:
    """Bounded retries: three attempts, then give up and carry on."""
    var attempt = Int(_cell(ctx[].attempt).fetch_add(1)) + 1
    print("--- notify attempt ", attempt, sep="")
    try:
        app.complete(inv, _step_notify(app, inv, attempt))
    except e:
        if is_suspended(e):
            raise e
        print("  gave up after 3 tries: ", e, sep="")
        app.complete(inv, String("order placed, notification skipped"))


def handle_validate(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Flow]
) raises -> None:
    """Terminal: a malformed order will not become well-formed by being
    retried, so `fail` rather than a run-block failure."""
    var order = inv.input_string()
    if len(order.codepoints()) <= 2:
        print("terminal failure: empty order, not retrying")
        app.fail(inv, "order is empty")
    else:
        app.complete(inv, String("accepted: ", order))


def handle_status(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Flow]
) raises -> None:
    var res = app.get_state(inv, "reservation")
    var chg = app.get_state(inv, "charge")
    var shp = app.get_state(inv, "shipment")
    app.complete(
        inv,
        String(
            "reservation=", res.value() if res else String("-"),
            " charge=", chg.value() if chg else String("-"),
            " shipment=", shp.value() if shp else String("-"),
            " attempts=", app.get_state_int(inv, "attempts", 0),
        ),
    )


def handle_arm(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Flow]
) raises -> None:
    """Re-arm the flaky charge so the demo can be run again."""
    var n = ctx[].fail_times
    _cell(ctx[].failures_left).store(n)
    _cell(ctx[].attempt).store(0)
    print("re-armed: charge will fail ", n, " more times", sep="")
    print()
    app.complete(inv, String("armed"))


def main() raises:
    var port = 9080
    var fail_times = DEFAULT_FAILURES
    var workers = 0
    var args = argv()
    var i = 1
    while i < len(args):
        if String(args[i]) == "--port" and i + 1 < len(args):
            port = Int(String(args[i + 1]))
            i += 2
        elif String(args[i]) == "--fail" and i + 1 < len(args):
            fail_times = Int(String(args[i + 1]))
            i += 2
        elif String(args[i]) == "--workers" and i + 1 < len(args):
            workers = Int(String(args[i + 1]))
            i += 2
        else:
            raise Error("unknown option: ", String(args[i]))

    var flow = Flow(
        Int64(fail_times), 0, Int64(fail_times), Int64(perf_counter_ns())
    )

    print("Orders listening on :", port, sep="")
    print("  charge fails ", fail_times, "x before succeeding (--fail N)", sep="")
    print("  curl localhost:8080/Orders/order-1/process --json '{}'")
    print()

    var served = App.run[Flow, __functions_in_module()](
        "Orders", flow, object=True, port=port, num_workers=workers
    )
    print("stopped after", served, "invocations")
