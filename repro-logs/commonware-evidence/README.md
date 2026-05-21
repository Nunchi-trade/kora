# Kora Devnet Commonware Repro Evidence

Generated from live `just devnet` chaos tests on 2026-05-20.

## Test 1: One validator restart (`validator-node3`)
Directory: `test1-one-node-restart-20260520T173812Z`

### Observed behavior
- Healthy baseline: ~0.22-0.25 s/block on all 4 validators.
- After restarting `validator-node3`, the restarted node **failed to catch up for 120s**.
- Restarted node height stayed frozen at `25348` while healthy nodes advanced to `26029` (~681 blocks behind).
- Restarted node logs show repeated `commonware_resolver::p2p::engine: invalid data received`.

### Critical files
- `test1-one-node-restart-20260520T173812Z-monitor-post-restart.log`
- `test1-node3-invalid-window.log`
- `test1-one-node-restart-20260520T173812Z-critical-grep.txt`

## Test 2: Two validators down then restart (`validator-node2`, `validator-node3`)
Directory: `test2-two-node-restart-20260520T174131Z`

### Observed behavior
- With 2/4 validators down, chain height froze (expected no-quorum stall).
- After both validators restarted, quorum returned but network remained **severely degraded**:
  - Post-restart block rate ~0.5-1.0 blk/s (~1-2 s/block) vs baseline ~4.5 blk/s (~0.22 s/block).
  - `nullifiedCount` climbed rapidly on healthy nodes (4797 -> 5249 over 180s).
  - Restarted `validator-node3` again failed to catch up (stuck at height `25514`).

### Critical files
- `test2-two-node-restart-20260520T174131Z-monitor-while-down.log`
- `test2-two-node-restart-20260520T174131Z-monitor-post-restart.log`
- `test2-two-node-restart-20260520T174131Z-critical-grep.txt`

## Mapping to Commonware bugs
1. **Resolver peer blocking on application-level invalid data**
   - Evidence: repeated `invalid data received` on restarted validator, followed by catch-up failure.
2. **Simplex resolver rejecting lower-view certificates during catch-up**
   - Evidence: catch-up stalls with resolver failures after restart; complements unit-level repro in Commonware monorepo.
