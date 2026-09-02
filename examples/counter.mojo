"""Counter — a Restate virtual object implemented in Mojo.

Run it (`pixi run counter`), then register the endpoint with a running
Restate server and invoke it:

    restate deployments register http://localhost:9080
    curl localhost:8080/Counter/mykey/add --json '{}'
    curl localhost:8080/Counter/mykey/get --json '{}'

Each handler is a `handle_*` function; `App.run` finds them, registers them
under the name after the prefix, and dispatches by that name. There is no list
of handler names to keep in step, because there is no list.

`slowadd` demonstrates a durable sleep; `stamp` a journaled side effect
(`run_enter`/`run_exit`) — its value is computed once and replayed on retries.

This service keeps nothing in process memory: everything it remembers lives in
Restate state, keyed by the object key. `Ctx[Unit]` is the way to say so.
"""

from restate import App, Ctx, Invocation, Unit, is_suspended
from std.sys import argv


def handle_add(app: App, inv: Invocation, worker: Int, ctx: Ctx[Unit]) raises:
    var n = app.get_state_int(inv, "count", 0) + 1
    app.set_state_int(inv, "count", n)
    app.complete(inv, String(n))


def handle_get(app: App, inv: Invocation, worker: Int, ctx: Ctx[Unit]) raises:
    app.complete(inv, String(app.get_state_int(inv, "count", 0)))


def handle_slowadd(app: App, inv: Invocation, worker: Int, ctx: Ctx[Unit]) raises:
    app.sleep_ms(inv, 2000)
    var n = app.get_state_int(inv, "count", 0) + 1
    app.set_state_int(inv, "count", n)
    app.complete(inv, String(n))


def handle_stamp(app: App, inv: Invocation, worker: Int, ctx: Ctx[Unit]) raises:
    # Journaled side effect: executed once, replayed thereafter.
    var stamp: String
    var replayed = app.run_enter(inv)
    if replayed:
        stamp = replayed.value()
    else:
        stamp = String("stamp-for-") + inv.key
        stamp = app.run_exit(inv, stamp)
    app.set_state(inv, "stamp", stamp)
    app.complete(inv, stamp)


def handle_wait_signal(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Unit]
) raises:
    # Durable external signal: create an awakeable, publish its id (here:
    # stdout; real code hands it out via run/call), then await resolution.
    var aid = app.awakeable_create(inv)
    print("awakeable-id:", aid)
    var value = app.awakeable_await(inv, aid)
    app.set_state(inv, "signal", value)
    app.complete(inv, value)


def handle_get_signal(
    app: App, inv: Invocation, worker: Int, ctx: Ctx[Unit]
) raises:
    var sig = app.get_state(inv, "signal")
    app.complete(inv, sig.value() if sig else String("none"))


def main() raises:
    var port = 9080
    var workers = 0
    var args = argv()
    var i = 1
    while i < len(args):
        if String(args[i]) == "--port" and i + 1 < len(args):
            port = Int(String(args[i + 1]))
            i += 2
        elif String(args[i]) == "--workers" and i + 1 < len(args):
            workers = Int(String(args[i + 1]))
            i += 2
        else:
            raise Error("unknown option: ", String(args[i]))

    var nothing = Unit()
    print(
        "Counter listening on :",
        port,
        " — register with `restate deployments register`",
        sep="",
    )
    var served = App.run[Unit, __functions_in_module()](
        "Counter", nothing, object=True, port=port, num_workers=workers
    )
    print("stopped after", served, "invocations")
