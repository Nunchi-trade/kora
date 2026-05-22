#!/bin/bash
set -euo pipefail

VALIDATOR_INDEX=${VALIDATOR_INDEX:-0}
VALIDATOR_COUNT=${VALIDATOR_COUNT:-0}
IS_BOOTSTRAP=${IS_BOOTSTRAP:-false}
BOOTSTRAP_PEERS=${BOOTSTRAP_PEERS:-""}
CHAIN_ID=${CHAIN_ID:-1337}
DATA_DIR=${DATA_DIR:-/data}
SHARED_DIR=${SHARED_DIR:-/shared}
BARRIER_DIR=${BARRIER_DIR:-/barrier}

MODE="${1:-validator}"
shift || true

log() { echo "[entrypoint] $*"; }
error() { echo "[entrypoint] ERROR: $*" >&2; exit 1; }

# Startup barrier: ensures all validators reach this point before any starts
# consensus. Each validator writes a marker file to a shared volume, then waits
# until the expected number of markers are present.
wait_for_barrier() {
    local count="$1"
    if [[ "$count" -le 0 || ! -d "$BARRIER_DIR" ]]; then
        return 0
    fi

    # Write our own marker
    touch "${BARRIER_DIR}/node${VALIDATOR_INDEX}.ready"
    log "Barrier: marked node${VALIDATOR_INDEX} ready (waiting for ${count} validators)"

    # Wait for all markers
    local timeout=120
    while true; do
        local ready
        ready=$(find "$BARRIER_DIR" -maxdepth 1 -name '*.ready' 2>/dev/null | wc -l | tr -d ' ')
        if [[ "$ready" -ge "$count" ]]; then
            log "Barrier: all ${count} validators ready, proceeding"
            return 0
        fi
        timeout=$((timeout - 1))
        if [[ $timeout -le 0 ]]; then
            log "Barrier: WARNING timeout after 120s (${ready}/${count} ready), proceeding anyway"
            return 0
        fi
        sleep 1
    done
}

case "$MODE" in
    setup)
        log "Running setup mode..."
        exec /usr/local/bin/keygen setup "$@"
        ;;
        
    dkg)
        log "Running DKG ceremony mode..."
        
        [[ -f "${SHARED_DIR}/peers.json" ]] || error "peers.json not found"
        [[ -f "${DATA_DIR}/validator.key" ]] || error "validator.key not found"
        
        if [[ -f "${DATA_DIR}/share.key" && -f "${DATA_DIR}/output.json" ]]; then
            log "DKG already completed (share.key exists)"
            exit 0
        fi
        
        if [[ "$IS_BOOTSTRAP" != "true" && -n "$BOOTSTRAP_PEERS" ]]; then
            BOOTSTRAP_HOST=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f1)
            BOOTSTRAP_PORT=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f2)
            
            log "Waiting for bootstrap peer ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
            timeout=120
            while ! nc -z "$BOOTSTRAP_HOST" "$BOOTSTRAP_PORT" 2>/dev/null; do
                timeout=$((timeout - 1))
                [[ $timeout -le 0 ]] && error "Timeout waiting for bootstrap peer"
                sleep 1
            done
            log "Bootstrap peer reachable"
        fi
        
        exec /usr/local/bin/kora dkg \
            --data-dir "$DATA_DIR" \
            --peers "${SHARED_DIR}/peers.json" \
            --chain-id "$CHAIN_ID" \
            "$@"
        ;;
        
    validator)
        log "Running validator mode..."

        [[ -f "${SHARED_DIR}/genesis.json" ]] || error "genesis.json not found"
        [[ -f "${DATA_DIR}/validator.key" ]] || error "validator.key not found"
        [[ -f "${DATA_DIR}/share.key" ]] || error "share.key not found (run DKG first)"
        [[ -f "${DATA_DIR}/output.json" ]] || error "output.json not found (run DKG first)"

        cp "${SHARED_DIR}/genesis.json" "${DATA_DIR}/" 2>/dev/null || true
        touch "${DATA_DIR}/.ready"

        # Wait for all validators to be ready before starting consensus.
        # This prevents height drift caused by staggered startup: if the
        # bootstrap node enters consensus minutes before the others, it
        # advances heights alone and later leaders return None from
        # propose() because they lack the parent snapshot.
        wait_for_barrier "$VALIDATOR_COUNT"

        if [[ "$IS_BOOTSTRAP" != "true" && -n "$BOOTSTRAP_PEERS" ]]; then
            BOOTSTRAP_HOST=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f1)
            BOOTSTRAP_PORT=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f2)

            log "Waiting for bootstrap peer ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
            timeout=120
            while ! nc -z "$BOOTSTRAP_HOST" "$BOOTSTRAP_PORT" 2>/dev/null; do
                timeout=$((timeout - 1))
                [[ $timeout -le 0 ]] && error "Timeout waiting for bootstrap peer"
                sleep 1
            done
        fi

        exec /usr/local/bin/kora validator \
            --data-dir "$DATA_DIR" \
            --peers "${SHARED_DIR}/peers.json" \
            --chain-id "$CHAIN_ID" \
            "$@"
        ;;

    secondary)
        log "Running secondary peer mode..."

        [[ -f "${SHARED_DIR}/peers.json" ]] || error "peers.json not found"
        [[ -f "${DATA_DIR}/validator.key" ]] || error "validator.key not found"

        touch "${DATA_DIR}/.ready"

        if [[ "$IS_BOOTSTRAP" != "true" && -n "$BOOTSTRAP_PEERS" ]]; then
            BOOTSTRAP_HOST=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f1)
            BOOTSTRAP_PORT=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f2)

            log "Waiting for bootstrap peer ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
            timeout=120
            while ! nc -z "$BOOTSTRAP_HOST" "$BOOTSTRAP_PORT" 2>/dev/null; do
                timeout=$((timeout - 1))
                [[ $timeout -le 0 ]] && error "Timeout waiting for bootstrap peer"
                sleep 1
            done
        fi

        exec /usr/local/bin/kora secondary \
            --data-dir "$DATA_DIR" \
            --peers "${SHARED_DIR}/peers.json" \
            --chain-id "$CHAIN_ID" \
            "$@"
        ;;
        
    *)
        exec "$MODE" "$@"
        ;;
esac
