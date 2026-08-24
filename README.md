# restate.mojo

Durable execution flows in **Mojo**, via [Restate](https://restate.dev).

The Rust shim (`ffi/`) embeds the official Restate Rust SDK: Rust owns the
HTTP/2 endpoint, the event loop, and the journal. Your business logic is Mojo,
driven by a synchronous loop — every durable operation crosses one C-ABI call.

```mojo
from restate import App, is_suspended

def main() raises:
    var app = App("Counter", ["add", "get"], object=True)
    while True:
        var inv = app.next()          # block until Restate invokes a handler
        try:
            if inv.handler == "add":
                var n = app.get_state_int(inv, "count", 0) + 1
                app.set_state_int(inv, "count", n)
                app.complete(inv, String(n))
            elif inv.handler == "get":
                app.complete(inv, String(app.get_state_int(inv, "count", 0)))
        except e:
            app.abandon(inv)          # suspended/cancelled: Restate re-delivers
            if not is_suspended(e):
                raise e
```

## Quickstart

```sh
pixi install                  # builds the Rust shim into the env
pixi run counter              # serve examples/counter.mojo on :9080

# in another terminal, with a Restate server running (npx @restatedev/restate-server):
npx -y @restatedev/restate deployments register http://localhost:9080
curl -X POST localhost:8080/Counter/alice/add -d '""'    # -> 1
curl -X POST localhost:8080/Counter/alice/get -d '""'    # -> 1
```

## API

| operation | durable semantics |
|---|---|
| `app.next()` | block until the next invocation (handler, key, input bytes) |
| `app.sleep_ms(inv, ms)` | journaled timer; long sleeps suspend the invocation |
| `app.get_state / set_state / clear_state` | virtual-object K/V state (`_bytes`/`_int` variants) |
| `app.call(inv, service, handler, payload, key="")` | durable request/response to another service |
| `app.send(...)` | durable one-way message, optionally delayed |
| `app.run_enter / run_exit` | journaled side effect: executed once, replayed on retry |
| `app.complete / app.fail` | finish the invocation (success / terminal error) |
| `app.abandon` | drop after suspension; Restate resumes with journal replay |

## Semantics you must respect

- **Determinism between ctx ops.** On retries/resumes Restate re-runs your
  handler and replays the journal; code between durable operations must be
  deterministic. Wrap anything non-deterministic (time, random, HTTP) in
  `run_enter`/`run_exit`.
- **Suspension.** A pending operation on a suspended invocation raises the
  suspension error — `abandon` it and keep serving; Restate re-invokes with
  replay.

## v0 limitations

- The Mojo driver is single-threaded: one invocation executes at a time, and
  **`call`-ing a handler served by the same driver process deadlocks** (the
  loop is blocked waiting for the call while the callee waits for the loop).
  Call out to other endpoints/services only.
- Services and virtual objects; no workflows, awakeables, or promises yet
  (the shim's `ContextInternal` surface has them — contributions welcome).
- Built/tested on osx-arm64 with `mojo == 1.0.0`.

## Install as a mojoshelf book

```sh
pixi shelf add restate-mojo     # pixi mode (builds shim + .mojopkg)
shelf add restate-mojo          # or as a git submodule
```
