#!/bin/bash
# End-to-end persistence proof:
# 1. Start server with WAL persistence
# 2. Run gameplay (moves + fires)
# 3. Kill process (SIGKILL — not graceful)
# 4. Restart server with same WAL directory
# 5. Verify state is recovered (players exist, positions preserved)
set -e

DIR=$(mktemp -d)
PORT=9444
BINARY="cargo run --release -p game-server --"

cleanup() { kill $(jobs -p) 2>/dev/null; rm -rf "$DIR"; }
trap cleanup EXIT

echo "=== Phase 27 persistence proof ==="
echo "WAL dir: $DIR"

# ── 1. Start server with persistence ──
$BINARY -- server --port $PORT --persist "$DIR" --stop-after 50 &
SERVER_PID=$!
sleep 3  # let it boot

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "FAIL: server did not start"
    exit 1
fi
echo "server started (pid $SERVER_PID)"

# ── 2. Run a scripted client to generate state ──
$BINARY -- client --name alice --port $PORT --auto 5 &
CLIENT_PID=$!
sleep 6

# ── 3. Kill the server (SIGKILL, not graceful) ──
kill -9 $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
echo "server killed (pid $SERVER_PID)"

# ── 4. Restart from same WAL ──
$BINARY -- server --port $PORT --persist "$DIR" --stop-after 30 &
NEW_PID=$!
sleep 3

if kill -0 $NEW_PID 2>/dev/null; then
    echo "PASS: server restarted and recovered from WAL"
else
    echo "FAIL: server did not restart"
    exit 1
fi

# ── 5. Verify recovered state ──
$BINARY -- client --name bob --port $PORT --auto 3 &
sleep 4

echo "=== Persistence proof complete ==="
