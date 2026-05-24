#!/bin/bash
# Health check script for Kora nodes.
#
# Modes (set via HEALTHCHECK_MODE env var):
#   dkg   - DKG ceremony completed (share.key + output.json exist)
#   p2p   - P2P port is listening
#   ready - RPC responsive AND chain is making progress (stall detection)
#
# Stall detection (ready mode):
#   On each invocation, the script fetches eth_blockNumber and compares it
#   against the value from the previous check (cached in /tmp/healthcheck_*).
#   If the block number has not advanced for HEALTHCHECK_STALL_THRESHOLD
#   consecutive checks, the health check fails. This catches nodes whose
#   RPC is up but consensus has stalled.
#
#   The stall counter resets whenever the block number advances.
#   A grace period of HEALTHCHECK_GRACE_BLOCKS=0 means any single stalled
#   check increments the counter.  Default threshold is 6 consecutive stalls
#   (at 30s interval = 3 minutes of no progress before unhealthy).
set -e

MODE="${HEALTHCHECK_MODE:-p2p}"
STALL_THRESHOLD="${HEALTHCHECK_STALL_THRESHOLD:-6}"
RPC_TIMEOUT="${HEALTHCHECK_RPC_TIMEOUT:-8}"

# Persistent state files (on tmpfs, survives across checks but not restarts)
BLOCK_FILE="/tmp/healthcheck_block"
STALL_FILE="/tmp/healthcheck_stall_count"

case "$MODE" in
    dkg)
        [[ -f "/data/share.key" && -f "/data/output.json" ]]
        ;;
    p2p)
        nc -z localhost 30303
        ;;
    ready)
        # Step 1: Verify the RPC server responds to eth_blockNumber.
        # Use --max-time to enforce our own timeout rather than relying on
        # curl's default (which interacts poorly with Docker's health check
        # timeout under CPU contention).
        RESULT=$(curl -sf --max-time "$RPC_TIMEOUT" -X POST http://localhost:8545 \
            -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' 2>/dev/null) || exit 1

        # Extract the hex block number and convert to decimal
        BLOCK_HEX=$(echo "$RESULT" | jq -r '.result // empty' 2>/dev/null) || exit 1
        [[ -z "$BLOCK_HEX" ]] && exit 1

        # Strip 0x prefix and convert hex to decimal.
        # Use shell arithmetic to avoid dependency on bc.
        BLOCK_DEC=$((16#${BLOCK_HEX#0x}))

        # Step 2: Stall detection — compare against previous block number.
        PREV_BLOCK=0
        STALL_COUNT=0
        [[ -f "$BLOCK_FILE" ]] && PREV_BLOCK=$(cat "$BLOCK_FILE" 2>/dev/null) || true
        [[ -f "$STALL_FILE" ]] && STALL_COUNT=$(cat "$STALL_FILE" 2>/dev/null) || true

        # Ensure numeric values
        PREV_BLOCK=${PREV_BLOCK:-0}
        STALL_COUNT=${STALL_COUNT:-0}

        if [[ "$BLOCK_DEC" -gt "$PREV_BLOCK" ]]; then
            # Chain is progressing — reset stall counter
            STALL_COUNT=0
        else
            # Block number has not advanced since last check
            STALL_COUNT=$((STALL_COUNT + 1))
        fi

        # Persist state for next invocation
        echo "$BLOCK_DEC" > "$BLOCK_FILE"
        echo "$STALL_COUNT" > "$STALL_FILE"

        # Step 3: Fail if stalled for too long
        if [[ "$STALL_COUNT" -ge "$STALL_THRESHOLD" ]]; then
            echo "UNHEALTHY: chain stalled at block $BLOCK_DEC for $STALL_COUNT consecutive checks" >&2
            exit 1
        fi
        ;;
    *)
        exit 1
        ;;
esac
