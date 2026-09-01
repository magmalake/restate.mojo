# restate.mojo

[![CI](https://github.com/millfolio/restate.mojo/actions/workflows/ci.yml/badge.svg)](https://github.com/millfolio/restate.mojo/actions/workflows/ci.yml) [![mojoshelf](https://mojoshelf.org/badge/restate-mojo.svg)](https://mojoshelf.org/tins/restate-mojo) [![mojo nightly](https://mojoshelf.org/badge/restate-mojo/nightly.svg)](https://mojoshelf.org/tins/restate-mojo)

Durable execution flows in **Mojo**, via [Restate](https://restate.dev).

The Rust shim (`ffi/`) embeds the official Restate Rust SDK: Rust owns the
HTTP/2 endpoint, the event loop, and the journal. Your business logic is Mojo,
driven by a synchronous loop — every durable operation crosses one C-ABI call.
Run that loop on one thread, or on N with `app.serve`.

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

Or hand the loop to `serve`, which runs it on N threads:

```mojo
from restate import App, Invocation, OpaquePtr

def handle(app: App, inv: Invocation, worker: Int, ctx: OpaquePtr) raises -> None:
    if inv.handler == "add":
        var n = app.get_state_int(inv, "count", 0) + 1
        app.set_state_int(inv, "count", n)
        app.complete(inv, String(n))
    elif inv.handler == "get":
        app.complete(inv, String(app.get_state_int(inv, "count", 0)))

def main() raises:
    var app = App("Counter", ["add", "get"], object=True)
    _ = app.serve[handle](num_workers=4)     # blocks until app.stop()
```

## Serving concurrently

`app.serve[handler](num_workers)` runs the same `next()` loop on `num_workers`
OS threads ([threads-mojo](https://github.com/magmalake/threads.mojo)'s
`WorkerPool`), and returns the number of invocations that completed once
`app.stop()` has been called. The single-threaded `while True: app.next()` loop
is untouched and still supported — `serve` is an addition, not a replacement.

| piece | shape |
|---|---|
| `HandlerFn` | `def(App, Invocation, Int, OpaquePtr) thin raises -> None` — app, invocation, worker index, your `ctx` |
| `app.serve[handler](num_workers=0, ctx=...)` | `0` means `num_cpus()`; returns the completed count |
| `app.stop()` / `app.is_stopping()` | unblocks every `next()`, in any thread — safe to call from inside a handler |
| `is_stopped(e)` | the sentinel `next()` raises after a stop, so a hand-rolled loop can tell shutdown from failure |

The handler is **thin** (non-capturing): it is reached through a thread start
routine, so anything it needs beyond the app and the invocation travels through
`ctx`, exactly as in `threads.parallel_for`. It *may* raise — `serve` catches,
`abandon`s the invocation so Restate re-delivers it, and carries on; suspension
arrives the same way and is not logged, because it is normal. A handler must
finish its invocation with `complete`, `fail`, or a raise.

**Workers and Restate's own concurrency.** For a **Virtual Object**, Restate
already serialises invocations per key — a second `add` on the same key waits
for the first regardless of how many workers you run. Workers buy you
concurrency *across* keys, and across services. For a plain **Service** there
is no such constraint and workers are the only limit. Either way, use **at
least 2** if any handler calls back into this process, and note that a worker
is occupied for the whole of an invocation, including a durable `sleep` short
enough not to suspend.

**Shutdown.** A stop flag in Mojo cannot reach a thread parked inside the
shim's blocking receiver, so `stop()` goes through the shim (`rst_stop`), which
sets a process-wide flag that its receive loop rechecks on a 25 ms timeout.
`serve` joins every worker before returning.

## Quickstart

```sh
pixi install                  # builds the Rust shim into the env
pixi run counter              # examples/counter.mojo — the single-threaded loop
pixi run chain                # examples/chain.mojo — served mode, self-calls

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
| `app.awakeable_create / awakeable_await` | durable promise resolvable from outside (services or the ingress `/restate/awakeables/<id>/resolve` API) |
| `app.awakeable_resolve / awakeable_reject` | resolve/reject another invocation's awakeable |
| `app.promise_await / promise_peek / promise_resolve / promise_reject` | named durable promises between a workflow's `run` handler and its shared handlers |
| `app.cancel_invocation` | cancel another invocation by id (`inv.id` carries this invocation's) |
| `app.complete / app.fail` | finish the invocation (success / terminal error) |
| `app.abandon` | drop after suspension; Restate resumes with journal replay |
| `app.serve[handler](num_workers)` | run the driver loop on N threads until `stop()` |
| `app.stop() / app.is_stopping()` | unblock every `next()`, from any thread |

Service flavors: `App(..., object=True)` (default) is a Virtual Object;
`object=False` a stateless Service; `workflow=True` a Workflow, whose
handler named `run` executes once per key while the other handlers are
shared (use promises to signal between them).

## Semantics you must respect

- **Determinism between ctx ops.** On retries/resumes Restate re-runs your
  handler and replays the journal; code between durable operations must be
  deterministic. Wrap anything non-deterministic (time, random, HTTP) in
  `run_enter`/`run_exit`.
- **Suspension.** A pending operation on a suspended invocation raises the
  suspension error — `abandon` it and keep serving; Restate re-invokes with
  replay.

## Limitations

- Payloads are raw bytes: what the outside world sends is what you get
  (JSON `"x"` arrives with its quotes) — parse/serialize in your handler.
- A **single-worker** driver — the `while True: app.next()` loop, or
  `serve(num_workers=1)` — still deadlocks if a handler `call`s a handler
  served by the same process. That is inherent: there is no second thread to
  run the callee. Use `serve` with two or more workers.
- CI runs both end-to-end gates on macOS and Linux with `mojo == 1.0.0`;
  linux-64 was previously untested and passes. Workflow mode is wired
  through discovery but has no end-to-end gate yet.

## Tests

```sh
pixi run it            # the single-threaded driver: state, sleep, run, awakeables
pixi run it-served     # served mode: self-calls, overlap, shutdown
```

Both boot a throwaway `restate-server` via `npx` and assert against it. Ports
are overridable (`RESTATE_IT_INGRESS`, `RESTATE_IT_ADMIN`, `RESTATE_IT_NODE`,
`RESTATE_IT_ENDPOINT`) because a developer machine often already has a Restate
stack up — `it-served` defaults to 18080/19070/15122/19080 for that reason.

`it-served` is the gate for the change that removed the old deadlock. It runs
`examples/chain.mojo` twice:

- **with 4 workers** — `chain`, whose handler `call`s `leaf` *in this same
  process*, returns `chain(leaf:"from-chain")`; four `hold` invocations that
  each occupy a worker for 400 ms finish in ~440 ms with a process-local
  high-water mark of 4 in flight; and `shutdown` calls `app.stop()` from
  inside a handler, after which the process must exit — the proof that
  `rst_stop` really does release workers parked in the blocking `rst_next`.
- **with 1 worker** — the ordinary handlers behave exactly as before, and the
  self-call still deadlocks. That last one is what makes the first leg
  evidence about the worker count rather than about anything else that
  changed.

## Install as a mojoshelf tin

```sh
pixi shelf add restate-mojo     # pixi mode (builds shim + .mojopkg)
shelf add restate-mojo          # or as a git submodule
```

Maintainers release new versions with `shelf publish` from the repo root
(see [getting started](https://mojoshelf.org/getting-started)).
