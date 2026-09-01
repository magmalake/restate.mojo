#!/bin/bash
#
# End-to-end gate for served mode (`pixi run it-served`).
#
# The one that matters: a handler that `call`s another handler *served by the
# same process* must complete. Under the v0 single-threaded driver it
# deadlocked — the loop was blocked waiting for the call while the callee
# waited for the loop — and that was restate.mojo's headline v0 limitation.
#
# Three things are asserted, against a real restate-server booted here:
#
#   1. with 4 workers, `chain` (which calls `leaf` in this same process)
#      returns the composed answer;
#   2. four concurrent `hold` invocations genuinely overlap — each occupies its
#      worker for 400 ms and the process-local high-water mark comes back > 1,
#      and the wall clock is far below the serialised 1.6 s;
#   3. with 1 worker the plain handlers still behave exactly as before, and
#      `chain` still deadlocks — which is what makes point 1 evidence about
#      worker count rather than about anything else that changed.
#
# Then `shutdown` calls App.stop() from inside a handler and the process must
# exit: that is the proof the shim's rst_stop actually releases workers parked
# in the blocking rst_next.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d /tmp/restate-mojo-served.XXXXXX)"

# Non-default ports throughout, and a base dir inside $WORK, so this gate can
# run on a machine that already has a Restate stack up — including the very
# common case of the developer's own service sitting on :9080. Override if
# these clash too.
INGRESS="${RESTATE_IT_INGRESS:-18080}"
ADMIN="${RESTATE_IT_ADMIN:-19070}"
NODE="${RESTATE_IT_NODE:-15122}"
ENDPOINT="${RESTATE_IT_ENDPOINT:-19080}"
CLEANUP() {
    kill "${CHAIN_PID:-0}" "${SERVER_PID:-0}" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$WORK" 2>/dev/null || true
}
trap CLEANUP EXIT

cd "$ROOT"
mkdir -p build
mojo build examples/chain.mojo -I src -o build/chain

(
    cd "$WORK" &&
    RESTATE_INGRESS__BIND_ADDRESS="0.0.0.0:$INGRESS" \
    RESTATE_ADMIN__BIND_ADDRESS="0.0.0.0:$ADMIN" \
    RESTATE_BIND_ADDRESS="0.0.0.0:$NODE" \
    RESTATE_ADVERTISED_ADDRESS="http://localhost:$NODE" \
    exec npx -y @restatedev/restate-server
) > "$WORK/server.log" 2>&1 & SERVER_PID=$!

echo "waiting for restate-server admin (:$ADMIN)..."
for _ in $(seq 1 60); do
    curl -s -o /dev/null "http://localhost:$ADMIN/health" && break
    sleep 2
done

expect() { # expect <label> <want> <got>
    if [ "$2" != "$3" ]; then
        echo "FAIL: $1 — expected '$2', got '$3'" >&2
        exit 1
    fi
    echo "  [OK] $1"
}

start_chain() { # start_chain <workers>
    ./build/chain --workers "$1" --port "$ENDPOINT" > "$WORK/chain-$1.log" 2>&1 & CHAIN_PID=$!
    for _ in $(seq 1 30); do
        curl -sf -X POST "http://localhost:$ADMIN/deployments" \
            -H "content-type: application/json" \
            -d "{\"uri\": \"http://localhost:$ENDPOINT\", \"force\": true}" > /dev/null && return 0
        sleep 1
    done
    echo "FAIL: could not register the $1-worker deployment" >&2
    exit 1
}

post() { # post <handler> [curl args...]
    local handler="$1"; shift
    curl -s -X POST "localhost:$INGRESS/Chain/$handler" -d '""' "$@"
}

# ── leg 1: several workers ──────────────────────────────────────────────────

echo "=== 4 workers ==="
start_chain 4

expect "leaf"   'leaf:""'                    "$(post leaf)"
expect "worker index is in range" "yes" \
    "$([ "$(post worker)" -ge 0 ] && echo yes || echo no)"

# THE acceptance test. Bounded, so a regression fails instead of hanging.
GOT="$(post chain --max-time 20 || echo TIMED-OUT)"
expect "self-call completes (was the v0 deadlock)" 'chain(leaf:"from-chain")' "$GOT"

# Overlap: four holds at once, each pinning its worker for 400 ms.
# `wait` with no arguments would also wait on the restate-server and the
# endpoint, neither of which ever exits — so collect and wait on these four.
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
T0=$(now_ms)
HOLD_PIDS=()
for _ in 1 2 3 4; do
    post hold --max-time 30 > /dev/null & HOLD_PIDS+=("$!")
done
for p in "${HOLD_PIDS[@]}"; do wait "$p" || true; done
ELAPSED_MS=$(( $(now_ms) - T0 ))
PEAK="$(post peak)"
echo "  four 400ms holds took ${ELAPSED_MS}ms; peak in-flight = ${PEAK}"
if [ "$PEAK" -lt 2 ]; then
    echo "FAIL: invocations did not overlap (peak in-flight = $PEAK)" >&2
    exit 1
fi
echo "  [OK] invocations overlap (peak in-flight $PEAK >= 2)"
if [ "$ELAPSED_MS" -ge 1200 ]; then
    echo "FAIL: 4x400ms holds took ${ELAPSED_MS}ms — that is serialised" >&2
    exit 1
fi
echo "  [OK] wall clock ${ELAPSED_MS}ms, well under the serialised 1600ms"

# stop() from inside a handler must release every worker parked in next().
post shutdown --max-time 5 > /dev/null 2>&1 || true
for _ in $(seq 1 20); do
    kill -0 "$CHAIN_PID" 2>/dev/null || break
    sleep 1
done
if kill -0 "$CHAIN_PID" 2>/dev/null; then
    echo "FAIL: the driver did not exit after stop() — rst_stop did not wake the workers" >&2
    exit 1
fi
echo "  [OK] stop() from a handler ended serve() and the process exited"
grep -q "^stopped after " "$WORK/chain-4.log" \
    || { echo "FAIL: serve() did not return cleanly" >&2; exit 1; }
echo "  [OK] serve() returned: $(grep '^stopped after ' "$WORK/chain-4.log")"

# ── leg 2: one worker — the old behaviour, unchanged ────────────────────────

echo "=== 1 worker ==="
start_chain 1

expect "leaf (1 worker)" 'leaf:""' "$(post leaf --max-time 20)"
expect "hold (1 worker)" "1"       "$(post hold --max-time 20)"

# And the deadlock is still there with one worker, which is what makes leg 1
# evidence about the worker count. 15s is far past the ~0ms a working call
# takes; the invocation stays wedged and the server keeps retrying, which is
# fine — this server is a throwaway.
if post chain --max-time 15 > /dev/null 2>&1; then
    echo "FAIL: a self-call returned with 1 worker — the test is not measuring what it claims" >&2
    exit 1
fi
echo "  [OK] a self-call still deadlocks with 1 worker (>=2 is what fixes it)"

echo "PASS — self-calls complete, invocations overlap, and stop() unwinds the driver"
