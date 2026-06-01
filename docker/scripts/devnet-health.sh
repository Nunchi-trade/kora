#!/usr/bin/env bash
# Devnet health diagnostic tool — queryable by humans and Claude.
# Queries Prometheus and prints a structured health report.
set -euo pipefail

PROM="${PROM_URL:-http://localhost:9090}"

query() { curl -sG --data-urlencode "query=$1" "${PROM}/api/v1/query" 2>/dev/null; }
val()   { echo "$1" | python3 -c "import json,sys; r=json.load(sys.stdin)['data']['result']; print(r[0]['value'][1] if r else 'N/A')" 2>/dev/null || echo "N/A"; }
vals()  { echo "$1" | python3 -c "
import json,sys
r=json.load(sys.stdin)['data']['result']
for m in r:
    lbl = m['metric'].get('validator_index', m['metric'].get('instance','?'))
    print(f'  node{lbl}: {m[\"value\"][1]}')
" 2>/dev/null || echo "  (no data)"; }

echo "============================================"
echo "  KORA DEVNET HEALTH REPORT"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "============================================"
echo ""

# --- Cluster Status ---
echo "## Cluster Status"
VALIDATOR_COUNT="${VALIDATOR_COUNT:-4}"
up=$(query 'count(up{job="kora-validators"}==1)')
echo "  Validators up: $(val "$up") / ${VALIDATOR_COUNT}"

height=$(query 'max(finalized_height)')
echo "  Finalized height: $(val "$height")"

view=$(query 'max(engine_voter_state_current_view)')
echo "  Current view: $(val "$view")"

drift=$(query 'max(finalized_height)-min(finalized_height)')
drift_val=$(val "$drift")
echo "  Height drift: ${drift_val}"
if [[ "$drift_val" != "N/A" ]] && python3 -c "exit(0 if float('${drift_val}') > 5 else 1)" 2>/dev/null; then
    echo "  ⚠ WARNING: nodes are diverging!"
fi
echo ""

# --- Per-node heights ---
echo "## Per-Node Finalized Height"
vals "$(query 'finalized_height')"
echo ""

# --- Throughput ---
echo "## Throughput"
bps=$(query 'avg(rate(finalized_height[1m]))')
echo "  Blocks/sec (1m avg): $(val "$bps")"
echo ""

# --- Latency ---
echo "## Latency (1m avg)"
nota=$(query 'avg(rate(engine_voter_notarization_latency_sum[1m])/rate(engine_voter_notarization_latency_count[1m]))')
echo "  Notarization: $(val "$nota")s"

fin=$(query 'avg(rate(engine_voter_finalization_latency_sum[1m])/rate(engine_voter_finalization_latency_count[1m]))')
echo "  Finalization: $(val "$fin")s"

build=$(query 'avg(rate(marshaled_build_duration_sum[1m])/rate(marshaled_build_duration_count[1m]))')
echo "  Block build: $(val "$build")s"

sig=$(query 'avg(rate(engine_batcher_verify_latency_sum[1m])/rate(engine_batcher_verify_latency_count[1m]))')
echo "  Sig verify: $(val "$sig")s"
echo ""

# --- Faults ---
echo "## Faults"
nulls=$(query 'sum(engine_voter_state_nullifications_total)')
echo "  Total nullifications: $(val "$nulls")"

timeouts=$(query 'sum(engine_voter_state_timeouts_total)')
echo "  Total timeouts: $(val "$timeouts")"

null_rate=$(query 'sum(rate(engine_voter_state_nullifications_total[5m]))')
echo "  Nullification rate (5m): $(val "$null_rate")/s"

skip=$(query 'avg(1-(rate(finalized_height[5m])/rate(engine_voter_state_current_view[5m])))')
echo "  Avg skip rate (wasted views): $(val "$skip")"

echo ""
echo "  Timeouts by reason:"
curl -sg "${PROM}/api/v1/query?query=sum%20by%20(reason)(engine_voter_state_timeouts_total)" 2>/dev/null | python3 -c "
import json,sys
r=json.load(sys.stdin)['data']['result']
for m in r:
    print(f\"    {m['metric']['reason']}: {m['value'][1]}\")
" 2>/dev/null || echo "    (no data)"
echo ""

# --- Resources ---
echo "## Resources"
echo "  Memory (RSS) per node:"
vals "$(query 'runtime_process_rss')"
echo ""

disk_w=$(query 'sum(runtime_storage_write_bytes_total)')
echo "  Total disk written: $(val "$disk_w") bytes"

disk_r=$(query 'sum(runtime_storage_read_bytes_total)')
echo "  Total disk read: $(val "$disk_r") bytes"
echo ""

# --- Network ---
echo "## Network"
in_bw=$(query 'sum(rate(runtime_inbound_bandwidth_total[1m]))')
echo "  Inbound bandwidth: $(val "$in_bw") B/s"

out_bw=$(query 'sum(rate(runtime_outbound_bandwidth_total[1m]))')
echo "  Outbound bandwidth: $(val "$out_bw") B/s"

in_conn=$(query 'sum(runtime_inbound_connections_total)')
echo "  Inbound connections: $(val "$in_conn")"

out_conn=$(query 'sum(runtime_outbound_connections_total)')
echo "  Outbound connections: $(val "$out_conn")"
echo ""

echo "============================================"
echo "  Dashboard: http://localhost:3000/d/kora-overview"
echo "  Prometheus: http://localhost:9090"
echo "============================================"
