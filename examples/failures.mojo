"""Failures — what durable execution actually buys you.

A three-step order flow where the middle step fails. Run it, invoke it, and
watch the log: the steps that already succeeded do **not** run again when
Restate retries. That is the whole idea, and it is much easier to believe from
a terminal than from a paragraph.

    pixi run failures                                   # terminal 1
    restate deployments register http://localhost:9080  # terminal 2
    curl localhost:8080/Orders/order-1/process --json '{}'

The `--json` matters: a bare POST is rejected by the ingress with
"Empty content-type" before it ever reaches your handler.

The flow reserves stock, charges a card, then ships. `charge` throws the first
time it is reached. Real output, `--fail 2` so replay happens twice:

    --- attempt 1 at t+0s
    [attempt 1] execute  reserve  -> res-order-1-108461
    [attempt 1] execute  charge   -> boom: card declined (transient)
      -> abandoned, Restate will retry: card declined
    --- attempt 2 at t+60s
    [attempt 2] REPLAY   reserve  -> res-order-1-108461
    [attempt 2] execute  charge   -> boom: card declined (transient)
      -> abandoned, Restate will retry: card declined
    --- attempt 3 at t+121s
    [attempt 3] REPLAY   reserve  -> res-order-1-108461
    [attempt 3] execute  charge   -> chg-order-1-8cca1c
    [attempt 3] execute  ship     -> shipped
    [attempt 3] done

`reserve` ran once across three attempts. Its value ends in a nanosecond
timestamp, so a second execution could not have produced the same string —
that it is identical is the journal, not luck.

**Retries are about a minute apart** (t+0, t+60, t+121 above). Nothing is
wrong if the terminal sits quiet; that is Restate's re-delivery interval, and
it is why `--fail` defaults to 1.

## Transient vs terminal

Two ways for a handler to end badly, and the difference is the thing people
get wrong:

- **`app.abandon(inv)`** — transient. Restate re-delivers the invocation and
  replays the journal. `charge` uses this.
- **`app.fail(inv, msg)`** — terminal. The caller gets the error, nothing is
  retried. `validate` uses this: a malformed order will not become
  well-formed by trying again.

Abandoning a permanently broken invocation retries it forever; failing a
transient one throws away work that would have succeeded on the next attempt.

    curl localhost:8080/Orders/order-2/validate --json '"12 widgets"'
    accepted: "12 widgets"

    curl localhost:8080/Orders/order-2/validate --json '""'
    {"code":500,"message":"order is empty","source":"invocation"}

## Where the journal boundary goes

`charge` makes its fallible call *before* `run_enter`, and that ordering is
forced rather than stylistic — see the comment on `_step_charge`.

## Handlers

    process    the three-step flow above
    validate   terminal failure on bad input, never retried
    status     what the object recorded, so you can watch state survive
    arm        re-arm the flaky charge and run the demo again

## Flags

    --port N   endpoint port (default 9080)
    --fail N   how many times charge fails first (default 1)
"""

from std.sys import argv
from std.time import perf_counter_ns

from restate import App, Invocation, is_suspended

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
    """A journaled side effect: `run_enter` returns the recorded value on a
    replay, and `None` the first time through."""
    var replayed = app.run_enter(inv)
    if replayed:
        print("[attempt ", attempt, "] REPLAY   reserve  -> ", replayed.value(), sep="")
        return replayed.value()
    var value = _short_id("res", inv.key)
    print("[attempt ", attempt, "] execute  reserve  -> ", value, sep="")
    return app.run_exit(inv, value)


def _step_charge(
    app: App, inv: Invocation, attempt: Int, mut failures_left: Int
) raises -> String:
    """The flaky step — and the one that shows where the journal boundary is.

    The call that can fail happens **before** `run_enter`. That ordering is
    forced: once `run_enter` hands you an execute slot, the journal expects a
    matching `run_exit`, and abandoning in between leaves the invocation
    unreplayable —

        restate.mojo protocol error: expected rst_run_exit

    So the rule with this API is: make the fallible call, and journal its
    result only once you have one. The cost is that a crash *between* a
    successful charge and `run_exit` would charge twice on retry; a
    `ctx.run` that could itself be failed would close that window, which this
    binding does not expose yet.
    """
    if failures_left > 0:
        failures_left -= 1
        print(
            "[attempt ", attempt,
            "] execute  charge   -> boom: card declined (transient)", sep="",
        )
        raise Error("card declined")

    var replayed = app.run_enter(inv)
    if replayed:
        print("[attempt ", attempt, "] REPLAY   charge   -> ", replayed.value(), sep="")
        return replayed.value()

    var value = _short_id("chg", inv.key)
    print("[attempt ", attempt, "] execute  charge   -> ", value, sep="")
    return app.run_exit(inv, value)


def _step_ship(app: App, inv: Invocation, attempt: Int) raises -> String:
    var replayed = app.run_enter(inv)
    if replayed:
        print("[attempt ", attempt, "] REPLAY   ship     -> ", replayed.value(), sep="")
        return replayed.value()
    print("[attempt ", attempt, "] execute  ship     -> shipped", sep="")
    return app.run_exit(inv, String("shipped"))


def main() raises:
    var port = 9080
    var fail_times = DEFAULT_FAILURES
    var args = argv()
    var i = 1
    while i < len(args):
        if String(args[i]) == "--port" and i + 1 < len(args):
            port = Int(String(args[i + 1]))
            i += 2
        elif String(args[i]) == "--fail" and i + 1 < len(args):
            fail_times = Int(String(args[i + 1]))
            i += 2
        else:
            raise Error("unknown option: ", String(args[i]))

    var app = App(
        "Orders",
        ["process", "validate", "status", "arm"],
        object=True,
        port=port,
    )

    # Deliberately *not* in Restate state: this stands in for a flaky
    # downstream service, and a real outage is not part of your journal. It
    # lives in the process, so it survives across invocations and retries.
    var failures_left = fail_times
    var attempt = 0
    var t0 = perf_counter_ns()

    print("Orders listening on :", port, sep="")
    print("  charge fails ", fail_times, "x before succeeding (--fail N)", sep="")
    print("  restate deployments register http://localhost:", port, sep="")
    print("  curl localhost:8080/Orders/order-1/process")
    print()

    while True:
        var inv = app.next()
        try:
            if inv.handler == "process":
                attempt += 1
                print("--- attempt ", attempt, " at t+", (perf_counter_ns() - t0) // 1_000_000_000, "s", sep="")
                var reserved = _step_reserve(app, inv, attempt)
                var charged = _step_charge(app, inv, attempt, failures_left)
                var shipped = _step_ship(app, inv, attempt)

                app.set_state(inv, "reservation", reserved)
                app.set_state(inv, "charge", charged)
                app.set_state(inv, "shipment", shipped)
                app.set_state_int(inv, "attempts", attempt)

                print("[attempt ", attempt, "] done", sep="")
                print()
                app.complete(inv, String(reserved, " / ", charged, " / ", shipped))

            elif inv.handler == "validate":
                # Terminal: a malformed order will not become well-formed by
                # being retried, so `fail` rather than `abandon`.
                var order = inv.input_string()
                if len(order.codepoints()) <= 2:
                    print("terminal failure: empty order, not retrying")
                    app.fail(inv, "order is empty")
                else:
                    app.complete(inv, String("accepted: ", order))

            elif inv.handler == "status":
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

            elif inv.handler == "arm":
                failures_left = fail_times
                attempt = 0
                print("re-armed: charge will fail ", fail_times, " more times", sep="")
                print()
                app.complete(inv, String("armed"))

            else:
                app.fail(inv, String("unknown handler: ", inv.handler))

        except e:
            # Transient: hand it back and let Restate re-deliver with the
            # journal intact. `is_suspended` is the ordinary "waiting on
            # something durable" path, not an error worth printing.
            app.abandon(inv)
            if not is_suspended(e):
                print("  -> abandoned, Restate will retry: ", e, sep="")
