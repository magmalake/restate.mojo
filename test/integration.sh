#!/bin/bash
#
# End-to-end gate: build the counter example, boot a throwaway Restate server
# (npx @restatedev/restate-server), register the endpoint, invoke every
# handler, and assert the durable behavior. Run via `pixi run it`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d /tmp/restate-mojo-it.XXXXXX)"
CLEANUP() {
    kill "${COUNTER_PID:-0}" "${SERVER_PID:-0}" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$WORK" 2>/dev/null || rm -rf "$WORK" 2>/dev/null || true
}
trap CLEANUP EXIT

cd "$ROOT"
mkdir -p build
mojo build examples/counter.mojo -I src -o build/counter

./build/counter & COUNTER_PID=$!
( cd "$WORK" && exec npx -y @restatedev/restate-server ) > "$WORK/server.log" 2>&1 & SERVER_PID=$!

echo "waiting for restate-server admin (:9070)..."
for _ in $(seq 1 60); do
    curl -s -o /dev/null http://localhost:9070/health && break
    sleep 2
done

curl -sf -X POST http://localhost:9070/deployments \
    -H "content-type: application/json" \
    -d '{"uri": "http://localhost:9080"}' > /dev/null
echo "deployment registered"

expect() { # expect <label> <want> <got>
    if [ "$2" != "$3" ]; then
        echo "FAIL: $1 — expected '$2', got '$3'" >&2
        exit 1
    fi
    echo "  [OK] $1"
}

expect "add #1"        "1" "$(curl -s -X POST localhost:8080/Counter/alice/add -d '""')"
expect "add #2"        "2" "$(curl -s -X POST localhost:8080/Counter/alice/add -d '""')"
expect "get alice"     "2" "$(curl -s -X POST localhost:8080/Counter/alice/get -d '""')"
expect "get bob"       "0" "$(curl -s -X POST localhost:8080/Counter/bob/get -d '""')"
expect "slowadd"       "3" "$(curl -s -X POST localhost:8080/Counter/alice/slowadd -d '""')"
expect "stamp"         "stamp-for-alice" "$(curl -s -X POST localhost:8080/Counter/alice/stamp -d '""')"

echo "PASS — durable state, sleep, and run journaling through the Mojo bridge"
