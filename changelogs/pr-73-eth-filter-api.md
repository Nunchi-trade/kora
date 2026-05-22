# PR #73: Implement Ethereum HTTP Filter API

## Overview

This PR adds server-side support for the Ethereum JSON-RPC filter methods
used by HTTP clients. These methods allow callers to register interest in
new blocks, pending transactions, or log events and then poll for changes
rather than holding a persistent WebSocket connection.

Prior to this change, the node's RPC layer had no filter support. Clients
that relied on `eth_newFilter`, `eth_newBlockFilter`,
`eth_newPendingTransactionFilter`, `eth_getFilterChanges`,
`eth_getFilterLogs`, or `eth_uninstallFilter` would receive
"method not found" errors.

## New RPC Methods

| Method | Description |
|---|---|
| `eth_newFilter` | Create a log filter with address/topic criteria and an optional block range. Returns a filter ID. |
| `eth_newBlockFilter` | Create a filter that tracks new block hashes. Returns a filter ID. |
| `eth_newPendingTransactionFilter` | Create a filter that tracks new pending transaction hashes. Returns a filter ID. |
| `eth_getFilterChanges` | Poll a filter for changes since the last call. Returns logs (for log filters) or hashes (for block/pending-tx filters). |
| `eth_getFilterLogs` | Return all logs matching a log filter's original criteria (does not advance the cursor). Only valid for log filters. |
| `eth_uninstallFilter` | Remove a filter by ID. Returns `true` if the filter existed. |

## How It Works

### Filter Store

A bounded, in-memory `FilterStore` holds active filters keyed by
monotonically increasing `u64` IDs. Each filter entry tracks:

- The filter variant (log, block, or pending transaction) and its matching
  criteria.
- A cursor recording what has already been reported (last polled block
  number, or last seen index into the pending-tx insertion-order vector).
- A last-poll timestamp used for TTL-based expiry.

Filters expire after 5 minutes of inactivity (configurable). When the
store reaches its maximum capacity (default 1024), the oldest filter is
evicted to make room.

### Cursor Initialization

When a log filter is created:

- If `from_block` is an explicit block number, the cursor is set so the
  first poll starts at that block (inclusive).
- If `from_block` is "earliest", the cursor starts at genesis.
- If `from_block` is omitted, "latest", or another tag, the cursor starts
  at the current head so only future events are returned.
- If `block_hash` is provided, the filter is treated as a single-block
  query: the first poll returns matching logs and all subsequent polls
  return empty.

### Polling Semantics

- **Log filters**: Each poll queries logs from `last_poll_block + 1` to the
  current head (or the original `to_block` bound, whichever is lower),
  then advances the cursor to the head. The original address, topic, and
  `to_block`/`block_hash` criteria are preserved across polls.
- **Block filters**: Each poll iterates from `last_poll_block + 1` to the
  current head, collecting block hashes. The cursor advances only to the
  highest block actually observed (tolerating gaps).
- **Pending transaction filters**: Each poll returns new transaction hashes
  in insertion order by scanning the shared `pending_tx_order` vector from
  the last seen index. Already-known hashes are skipped.

### Concurrency

The filter's internal `Mutex` is held only to snapshot the cursor state,
then released before performing any async I/O (state provider queries).
After the query completes, the mutex is re-acquired to update the cursor.
This avoids holding a lock across `.await` points.

## Files Modified

### `crates/node/rpc/src/filters.rs` (new file, 232 lines)

Defines the filter data model and storage layer:

- `FilterChanges` -- response enum (`Logs` or `Hashes`) serialized with
  `#[serde(untagged)]`.
- `Filter` -- cursor enum with `Log`, `Block`, and `PendingTransaction`
  variants.
- `FilterEntry` -- wrapper pairing a `Mutex<Filter>` with a
  `RwLock<Instant>` for TTL tracking.
- `FilterStore` -- bounded `HashMap` with monotonic ID generation,
  TTL-based expiry, and oldest-entry eviction.
- Unit tests for create/get/remove, expiry cleanup, and bounded eviction.

### `crates/node/rpc/src/eth.rs` (modified, +200 lines in implementation, +190 lines in tests)

- Added six new trait methods to `EthApi` and their implementations on
  `EthApiImpl`.
- Added `pending_tx_order: Arc<RwLock<Vec<B256>>>` field to track pending
  transaction insertion order.
- Added `filter_store: Arc<FilterStore>` field.
- Extracted `current_block_number()` helper (also simplifies
  `block_number()` RPC method).
- Added `filter_id_to_u64()` as a `const fn` to safely convert `U256`
  filter IDs to `u64`.
- Added `TestStateProvider` for integration-style tests with controllable
  block/log state.
- Added comprehensive test suite: block filter lifecycle, log filter
  lifecycle, pending transaction filter lifecycle, block-hash log filter
  single-return semantics, `getFilterLogs` rejection for non-log filters,
  and `getFilterChanges` with invalid/overflow IDs.

### `crates/node/rpc/src/error.rs` (modified, +19 lines)

- Added `RpcError::FilterNotFound` variant mapped to `SERVER_ERROR`
  (-32000), matching Geth's behavior.
- Added display and error-object conversion tests.

### `crates/node/rpc/src/lib.rs` (modified, +3 lines)

- Added `mod filters` and re-exported `FilterChanges`.

### `crates/node/rpc/src/types.rs` (modified, +1 derive)

- Added `PartialEq` and `Eq` derives to `RpcLog` so `FilterChanges` can
  derive equality comparison (useful for tests and downstream consumers).

## Breaking Changes

- `RpcLog` now derives `PartialEq` and `Eq`. This is additive and should
  not break existing code, but downstream types that embed `RpcLog` in
  non-`PartialEq` contexts are unaffected since `PartialEq` is opt-in.
- The `EthApi` trait gains six new methods. Any custom implementations of
  `EthApiServer` (outside this crate) will need to implement them.

## Testing

The following test cases cover the new functionality (all in
`crates/node/rpc/src/eth.rs` and `crates/node/rpc/src/filters.rs`):

- **`filter_store_create_and_get`** -- Verifies filter creation returns
  valid IDs and lookup works.
- **`filter_store_remove`** -- Verifies removal returns true once and
  false on double-remove.
- **`filter_store_cleanup_expired`** -- Verifies TTL-based expiry removes
  stale entries while keeping fresh ones.
- **`filter_store_evicts_oldest_when_bounded`** -- Verifies oldest filter
  is evicted when the store is full.
- **`eth_block_filter_lifecycle`** -- Creates a block filter, inserts new
  blocks, polls for changes (expecting new hashes), polls again (expecting
  empty), then uninstalls.
- **`eth_log_filter_lifecycle`** -- Creates a log filter with address and
  topic criteria, inserts matching and non-matching logs, polls for
  changes (expecting only matching logs), polls again (expecting empty),
  then calls `getFilterLogs` for the full history.
- **`eth_pending_transaction_filter_lifecycle`** -- Submits a transaction
  before filter creation and one after, verifies only the post-creation
  transaction is returned by the filter.
- **`eth_log_filter_block_hash_returns_once`** -- Verifies that a log
  filter created with `block_hash` returns matching logs on the first poll
  and empty results on all subsequent polls, even as new blocks arrive.
- **`eth_get_filter_logs_rejects_non_log_filter`** -- Verifies that
  calling `getFilterLogs` on a block filter returns an error.
- **`eth_get_filter_changes_invalid_id`** -- Verifies that
  `getFilterChanges` returns an error for non-existent and overflowing
  filter IDs.
- **`filter_id_to_u64_edge_cases`** -- Verifies the `const fn` conversion
  handles zero, valid values, `u64::MAX`, overflow, and `U256::MAX`.
- **Error tests** in `error.rs` -- Display and error-object conversion
  for `FilterNotFound`.
