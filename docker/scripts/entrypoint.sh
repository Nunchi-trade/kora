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

RUNTIME_DIR=${KORA_RUNTIME_DIR:-/runtime}

MODE="${1:-validator}"
shift || true

log() { echo "[entrypoint] $*"; }
error() { echo "[entrypoint] ERROR: $*" >&2; exit 1; }

# Ensure runtime directory exists and is writable by the kora user.
# Docker named volumes inherit ownership from the image on first mount,
# but we verify here in case an external volume with different ownership
# is attached.
if [[ -d "$RUNTIME_DIR" ]]; then
    if [[ ! -w "$RUNTIME_DIR" ]]; then
        log "WARNING: runtime dir ${RUNTIME_DIR} is not writable, attempting chown..."
        chown -R "$(id -u):$(id -g)" "$RUNTIME_DIR" 2>/dev/null || \
            error "Cannot write to runtime dir ${RUNTIME_DIR}. Fix volume permissions."
    fi
else
    mkdir -p "$RUNTIME_DIR" 2>/dev/null || error "Cannot create runtime dir ${RUNTIME_DIR}"
fi
log "Runtime dir: ${RUNTIME_DIR} (writable)"

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

        # Log key fingerprints so DKG key mismatches are immediately obvious
        SHARE_KEY_HASH=$(sha256sum "${DATA_DIR}/share.key" 2>/dev/null | cut -c1-16)
        OUTPUT_HASH=$(sha256sum "${DATA_DIR}/output.json" 2>/dev/null | cut -c1-16)
        log "DKG key fingerprints: share.key=${SHARE_KEY_HASH} output.json=${OUTPUT_HASH}"

        cp "${SHARED_DIR}/genesis.json" "${DATA_DIR}/" 2>/dev/null || true

        # Detect whether this is a first startup or a restart by checking
        # for the commit marker on the persistent /data volume. If it exists,
        # the node has finalized at least one block previously and does not
        # need the bootstrap peer or the startup barrier to proceed.
        # DO NOT use archive or QMDB paths -- those live on tmpfs (/runtime)
        # and are wiped on every container restart.
        if [[ -f "${DATA_DIR}/last_committed_digest" ]]; then
            log "Restart detected (last_committed_digest exists), skipping barrier and bootstrap wait"
        else
            # First startup -- wait for all validators to be ready before
            # starting consensus. This prevents height drift caused by
            # staggered startup: if the bootstrap node enters consensus
            # minutes before the others, it advances heights alone and
            # later leaders return None from propose() because they lack
            # the parent snapshot.
            wait_for_barrier "$VALIDATOR_COUNT"

            if [[ "$IS_BOOTSTRAP" != "true" && -n "$BOOTSTRAP_PEERS" ]]; then
                BOOTSTRAP_HOST=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f1)
                BOOTSTRAP_PORT=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f2)

                log "First startup: waiting for bootstrap peer ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
                timeout=120
                while ! nc -z "$BOOTSTRAP_HOST" "$BOOTSTRAP_PORT" 2>/dev/null; do
                    timeout=$((timeout - 1))
                    [[ $timeout -le 0 ]] && error "Timeout waiting for bootstrap peer"
                    sleep 1
                done
                log "Bootstrap peer reachable"
            fi
        fi

        touch "${DATA_DIR}/.ready"

        TX_GOSSIP=${TX_GOSSIP:-false}
        GOSSIP_FLAG=""
        if [[ "$TX_GOSSIP" == "true" ]]; then
            GOSSIP_FLAG="--tx-gossip"
            log "Transaction gossip enabled"
        fi

        TX_GOSSIP=${TX_GOSSIP:-false}
        GOSSIP_FLAG=""
        if [[ "$TX_GOSSIP" == "true" ]]; then
            GOSSIP_FLAG="--tx-gossip"
            log "Transaction gossip enabled"
        fi

        exec /usr/local/bin/kora validator \
            --data-dir "$DATA_DIR" \
            --peers "${SHARED_DIR}/peers.json" \
            --chain-id "$CHAIN_ID" \
            $GOSSIP_FLAG \
            "$@"
        ;;

    secondary)
        log "Running secondary peer mode..."

        [[ -f "${SHARED_DIR}/peers.json" ]] || error "peers.json not found"
        [[ -f "${DATA_DIR}/validator.key" ]] || error "validator.key not found"

        if [[ "$IS_BOOTSTRAP" != "true" && -n "$BOOTSTRAP_PEERS" ]]; then
            BOOTSTRAP_HOST=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f1)
            BOOTSTRAP_PORT=$(echo "$BOOTSTRAP_PEERS" | cut -d: -f2)

            # Only wait for bootstrap on first startup. On restarts, the
            # P2P layer handles reconnection internally.
            if [[ ! -f "${DATA_DIR}/.bootstrap_done" ]]; then
                log "First startup: waiting for bootstrap peer ${BOOTSTRAP_HOST}:${BOOTSTRAP_PORT}..."
                timeout=120
                while ! nc -z "$BOOTSTRAP_HOST" "$BOOTSTRAP_PORT" 2>/dev/null; do
                    timeout=$((timeout - 1))
                    [[ $timeout -le 0 ]] && error "Timeout waiting for bootstrap peer"
                    sleep 1
                done
                log "Bootstrap peer reachable"
                touch "${DATA_DIR}/.bootstrap_done"
            else
                log "Restart detected (.bootstrap_done exists), skipping bootstrap peer wait"
            fi
        fi

        touch "${DATA_DIR}/.ready"

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
