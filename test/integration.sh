#!/bin/bash
#
# End-to-end gate: build the counter example, boot a throwaway Restate server
# (npx @restatedev/restate-server), register the endpoint, invoke every
# handler, and assert the durable behavior. Run via `pixi run it`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d /tmp/restate-mojo-it.XXXXXX)"

# Ports, overridable: a developer machine often already has a Restate stack on
# 8080/9070 and a service on 9080, and registering against *that* server
# silently tests somebody else's deployment.
INGRESS="${RESTATE_IT_INGRESS:-8080}"
ADMIN="${RESTATE_IT_ADMIN:-9070}"
NODE="${RESTATE_IT_NODE:-5122}"
ENDPOINT="${RESTATE_IT_ENDPOINT:-9080}"
CLEANUP() {
    kill "${COUNTER_PID:-0}" "${SERVER_PID:-0}" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$WORK" 2>/dev/null || rm -rf "$WORK" 2>/dev/null || true
}
trap CLEANUP EXIT

cd "$ROOT"
mkdir -p build
mojo build examples/counter.mojo -I src -o build/counter

./build/counter --port "$ENDPOINT" > "$WORK/counter.log" 2>&1 & COUNTER_PID=$!
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

curl -sf -X POST "http://localhost:$ADMIN/deployments" \
    -H "content-type: application/json" \
    -d "{\"uri\": \"http://localhost:$ENDPOINT\"}" > /dev/null
echo "deployment registered"

expect() { # expect <label> <want> <got>
    if [ "$2" != "$3" ]; then
        echo "FAIL: $1 — expected '$2', got '$3'" >&2
        exit 1
    fi
    echo "  [OK] $1"
}

expect "add #1"        "1" "$(curl -s -X POST localhost:$INGRESS/Counter/alice/add -d '""')"
expect "add #2"        "2" "$(curl -s -X POST localhost:$INGRESS/Counter/alice/add -d '""')"
expect "get alice"     "2" "$(curl -s -X POST localhost:$INGRESS/Counter/alice/get -d '""')"
expect "get bob"       "0" "$(curl -s -X POST localhost:$INGRESS/Counter/bob/get -d '""')"
expect "slowadd"       "3" "$(curl -s -X POST localhost:$INGRESS/Counter/alice/slowadd -d '""')"
expect "stamp"         "stamp-for-alice" "$(curl -s -X POST localhost:$INGRESS/Counter/alice/stamp -d '""')"

# Awakeable: fire wait_signal asynchronously, resolve it from ingress.
curl -sf -X POST localhost:$INGRESS/Counter/dave/wait_signal/send -d '""' > /dev/null
for _ in $(seq 1 20); do
    AID="$(grep -m1 '^awakeable-id: ' "$WORK/counter.log" | awk '{print $2}' || true)"
    [ -n "${AID:-}" ] && break
    sleep 1
done
[ -n "${AID:-}" ] || { echo "FAIL: no awakeable id surfaced" >&2; exit 1; }
curl -sf -X POST "localhost:$INGRESS/restate/awakeables/$AID/resolve" \
    -H "content-type: application/json" -d '"pinged"' > /dev/null
for _ in $(seq 1 20); do
    GOT="$(curl -s -X POST localhost:$INGRESS/Counter/dave/get_signal -d '""')"
    [ "$GOT" = "\"pinged\"" ] && break
    sleep 1
done
expect "awakeable resolved" "\"pinged\"" "$GOT"

echo "PASS — durable state, sleep, run journaling, and awakeables through the Mojo bridge"
