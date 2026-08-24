"""Counter — a Restate virtual object implemented in Mojo.

Run it (`pixi run counter`), then register the endpoint with a running
Restate server and invoke it:

    restate deployments register http://localhost:9080
    curl localhost:8080/Counter/mykey/add
    curl localhost:8080/Counter/mykey/get

`slowadd` demonstrates a durable sleep; `stamp` a journaled side effect
(`run_enter`/`run_exit`) — its value is computed once and replayed on
retries.
"""

from restate import App, is_suspended


def main() raises:
    var app = App(
        "Counter",
        ["add", "get", "slowadd", "stamp", "wait_signal", "get_signal"],
        object=True,
    )
    print("Counter listening on :9080 — register with `restate deployments register`")
    while True:
        var inv = app.next()
        try:
            if inv.handler == "add":
                var n = app.get_state_int(inv, "count", 0) + 1
                app.set_state_int(inv, "count", n)
                app.complete(inv, String(n))
            elif inv.handler == "get":
                app.complete(inv, String(app.get_state_int(inv, "count", 0)))
            elif inv.handler == "slowadd":
                app.sleep_ms(inv, 2000)
                var n = app.get_state_int(inv, "count", 0) + 1
                app.set_state_int(inv, "count", n)
                app.complete(inv, String(n))
            elif inv.handler == "stamp":
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
            elif inv.handler == "wait_signal":
                # Durable external signal: create an awakeable, publish its id
                # (here: stdout; real code hands it out via run/call), then
                # await resolution from the outside world.
                var aid = app.awakeable_create(inv)
                print("awakeable-id:", aid)
                var value = app.awakeable_await(inv, aid)
                app.set_state(inv, "signal", value)
                app.complete(inv, value)
            elif inv.handler == "get_signal":
                var sig = app.get_state(inv, "signal")
                app.complete(inv, sig.value() if sig else String("none"))
            else:
                app.fail(inv, "unknown handler: " + inv.handler)
        except e:
            app.abandon(inv)
            if not is_suspended(e):
                print("handler error:", e)
