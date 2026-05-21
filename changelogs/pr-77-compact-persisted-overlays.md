# PR #77: Compact Persisted Ledger Snapshot Overlays

## Problem

When the ledger persisted a chain of snapshots to QMDB, only the **tip** snapshot
(the most recently committed block) had its in-memory overlay state compacted.
All intermediate ancestor snapshots in the chain retained their full overlay
change sets even though those changes had already been flushed to disk.

Over time this caused **unbounded memory growth**: every persisted-but-not-compacted
snapshot kept a copy of its `OverlayState` changes and the corresponding
`ChangeSet` inside the `Snapshot` struct. On long-running nodes processing many
blocks, memory usage grew proportionally to the total number of persisted blocks
rather than only the number of *unpersisted* blocks.

Additionally, when a snapshot was missing during the compaction loop, the code
silently continued (`continue`), masking a bug that should never occur in normal
operation.

## Solution

### Compact all snapshots in the persisted chain (not just the tip)

The `persist_snapshot` method in `LedgerView` now iterates over **every** digest
in the persisted chain and replaces each snapshot with a compacted version. The
compacted snapshot:

- Repoints its `state` field to a fresh `OverlayState` backed by the current
  QMDB state with an empty change set.
- Clears its `changes` field to `QmdbChangeSet::default()`.
- Preserves `parent`, `state_root`, and `tx_ids` unchanged.

This ensures that once data is flushed to QMDB, no snapshot retains a redundant
in-memory copy of the same state changes.

### Return errors for missing snapshots instead of silently continuing

If a snapshot in the persisted chain cannot be found during compaction, the code
now returns `ConsensusError::SnapshotNotFound` instead of silently skipping it.
This makes the failure observable and debuggable rather than hiding a
potentially serious internal inconsistency.

### Add overlay state inspection helpers

Two new methods on `OverlayState<S>` allow callers (and tests) to inspect the
overlay change set without accessing private fields:

- `changes_is_empty()` -- returns `true` when the change set has no entries.
- `change_len()` -- returns the number of accounts in the change set.

Both methods are annotated with `#[must_use]`.

## Files Modified

### `crates/node/ledger/src/lib.rs`

- **`persist_snapshot`**: Changed from compacting only the chain tip to
  compacting every snapshot in the chain. Replaced the `if let` guard that
  operated on `chain.last()` with a `for digest in &chain` loop. Each iteration
  fetches the snapshot, builds a compact replacement, and reinserts it.
- **Error handling**: The `.get(digest)` call now uses
  `.ok_or(ConsensusError::SnapshotNotFound(*digest))?` instead of a silent
  `continue`, surfacing unexpected missing snapshots as errors.
- **New test `persist_snapshot_compacts_all_persisted_chain_snapshots`**: Builds
  a two-block chain, persists it, and asserts that *both* snapshots have empty
  `changes` and empty overlay change sets afterward, while preserving `parent`,
  `state_root`, and `tx_ids`.
- **Formatting**: Several `setup_ledger(...)` call sites were reformatted by
  `rustfmt` to use trailing-comma style (no semantic change).

### `crates/storage/overlay/src/overlay.rs`

- **New method `change_len(&self) -> usize`**: Returns the number of accounts
  in the overlay change set. Annotated `#[must_use]`.
- **New method `changes_is_empty(&self) -> bool`**: Returns whether the overlay
  change set is empty. Annotated `#[must_use]`.
- **New test `test_changes_is_empty_and_change_len`**: Exercises both helpers
  on empty and non-empty overlays.
- **Formatting**: `AccountUpdate` struct literals in tests reformatted by
  `rustfmt` to use trailing-comma style (no semantic change).

## Breaking Changes

None. The public API is only *expanded* (two new methods). The compaction
behavioral change is internal and does not alter any external-facing contract.

## Migration Considerations

No migration is needed. Existing persisted data is unaffected; the change only
alters how in-memory snapshots are handled after a successful QMDB commit.

## Testing

| Test | What it covers |
|------|----------------|
| `persist_snapshot_compacts_all_persisted_chain_snapshots` | Verifies that every snapshot in a two-block persisted chain has its overlay and change set emptied, while metadata (`parent`, `state_root`, `tx_ids`) is preserved. |
| `persist_snapshot_merges_unpersisted_ancestors` | Existing test ensuring multi-block changes merge correctly and the QMDB balance reflects the combined transfers. |
| `persist_snapshot_duplicate_is_noop` | Existing test confirming that persisting the same digest twice is idempotent (`Ok(false)`). |
| `persist_snapshot_merges_overlays` | Existing test with five independent senders verifying overlay merge correctness. |
| `persist_snapshot_unrelated_merges` | Existing test for two independent fork chains persisted sequentially. |
| `persist_snapshot_updates_snapshot_state` | Existing test confirming the state root is preserved after persistence. |
| `empty_child_inherits_parent_state_root_after_persist` | Existing test for empty-block root inheritance. |
| `test_changes_is_empty_and_change_len` | New unit test exercising the `changes_is_empty()` and `change_len()` accessors on empty and populated overlays. |
