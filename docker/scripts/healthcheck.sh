#!/bin/bash
# Health check script for Kora nodes.
#
# Modes (set via HEALTHCHECK_MODE env var):
#   dkg   - DKG ceremony completed (share.key + output.json exist)
#   p2p   - P2P port is listening
#   ready - RPC responsive AND chain is making progress AND consensus
#           participation is verified via kora_nodeStatus
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
#
# Consensus participation check (ready mode):
#   After the block-number stall check, queries kora_nodeStatus to verify:
#   1. The node has sufficient peers for BFT quorum (partitionStatus != "partitioned")
#   2. The node's finalized block count is advancing (not just serving stale RPC data)
#   These checks detect nodes that appear alive via RPC but are disconnected
#   from consensus — a blind spot in the original eth_blockNumber-only check.
#
#   The finalized-count stall check uses the same threshold as the block-number
#   check so that both signals trigger unhealthy at the same pace.
set -e

MODE="${HEALTHCHECK_MODE:-p2p}"
STALL_THRESHOLD="${HEALTHCHECK_STALL_THRESHOLD:-6}"
RPC_TIMEOUT="${HEALTHCHECK_RPC_TIMEOUT:-8}"
# Minimum peers required for health. Default 0 disables the absolute floor;
# quorum is still enforced via partitionStatus from kora_nodeStatus.
MIN_PEERS="${HEALTHCHECK_MIN_PEERS:-0}"

# Persistent state files (on tmpfs, survives across checks but not restarts)
BLOCK_FILE="/tmp/healthcheck_block"
STALL_FILE="/tmp/healthcheck_stall_count"
FINALIZED_FILE="/tmp/healthcheck_finalized"
FINALIZED_STALL_FILE="/tmp/healthcheck_finalized_stall"

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

        # Step 4: Consensus participation — query kora_nodeStatus.
        # This is a soft check: if the RPC method is unavailable (e.g. older
        # binary, secondary node), we skip gracefully and rely on the
        # eth_blockNumber stall check above.
        STATUS=$(curl -sf --max-time "$RPC_TIMEOUT" -X POST http://localhost:8545 \
            -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"kora_nodeStatus","params":[],"id":2}' 2>/dev/null) || true

        if [[ -n "$STATUS" ]]; then
            # Parse fields from the kora_nodeStatus response.
            # jq exits 0 even on null, so we check for empty strings.
            PARTITION=$(echo "$STATUS" | jq -r '.result.partitionStatus // empty' 2>/dev/null) || true
            PEER_COUNT=$(echo "$STATUS" | jq -r '.result.peerCount // empty' 2>/dev/null) || true
            FINALIZED_COUNT=$(echo "$STATUS" | jq -r '.result.finalizedCount // empty' 2>/dev/null) || true

            # 4a: Reject if the node is network-partitioned (below BFT quorum).
            # A partitioned node cannot participate in consensus and will
            # inevitably stall, but the block-number check takes 3 minutes
            # to detect this. The partition check catches it immediately.
            if [[ "$PARTITION" == "partitioned" ]]; then
                echo "UNHEALTHY: node is network-partitioned (insufficient peers for BFT quorum)" >&2
                exit 1
            fi

            # 4b: Optional absolute peer floor (disabled by default).
            if [[ -n "$PEER_COUNT" && "$MIN_PEERS" -gt 0 ]]; then
                if [[ "$PEER_COUNT" -lt "$MIN_PEERS" ]]; then
                    echo "UNHEALTHY: only $PEER_COUNT peers connected (minimum: $MIN_PEERS)" >&2
                    exit 1
                fi
            fi

            # 4c: Finalized-count stall detection.
            # Similar to the block-number stall check, but tracks the node's
            # own finalized_count from the consensus engine. A node that is
            # RPC-responsive but not finalizing blocks (e.g. disconnected from
            # consensus, serving stale data) will fail this check.
            if [[ -n "$FINALIZED_COUNT" ]]; then
                PREV_FINALIZED=0
                FIN_STALL=0
                [[ -f "$FINALIZED_FILE" ]] && PREV_FINALIZED=$(cat "$FINALIZED_FILE" 2>/dev/null) || true
                [[ -f "$FINALIZED_STALL_FILE" ]] && FIN_STALL=$(cat "$FINALIZED_STALL_FILE" 2>/dev/null) || true
                PREV_FINALIZED=${PREV_FINALIZED:-0}
                FIN_STALL=${FIN_STALL:-0}

                if [[ "$FINALIZED_COUNT" -gt "$PREV_FINALIZED" ]]; then
                    FIN_STALL=0
                else
                    FIN_STALL=$((FIN_STALL + 1))
                fi

                echo "$FINALIZED_COUNT" > "$FINALIZED_FILE"
                echo "$FIN_STALL" > "$FINALIZED_STALL_FILE"

                if [[ "$FIN_STALL" -ge "$STALL_THRESHOLD" ]]; then
                    echo "UNHEALTHY: consensus stalled — finalized count stuck at $FINALIZED_COUNT for $FIN_STALL consecutive checks" >&2
                    exit 1
                fi
            fi
        fi
        ;;
    *)
        exit 1
        ;;
esac
