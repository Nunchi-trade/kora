#!/bin/bash
set -e

MODE="${HEALTHCHECK_MODE:-p2p}"

case "$MODE" in
    dkg)
        [[ -f "/data/share.key" && -f "/data/output.json" ]]
        ;;
    p2p)
        nc -z localhost 30303
        ;;
    ready)
        # Verify the RPC server is responsive with a real method call
        RESULT=$(curl -sf -X POST http://localhost:8545 \
            -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' 2>/dev/null) || exit 1
        echo "$RESULT" | jq -e '.result' >/dev/null 2>&1
        ;;
    *)
        exit 1
        ;;
esac
