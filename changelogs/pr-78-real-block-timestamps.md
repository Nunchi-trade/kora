# PR #78: Real Block Timestamps

## Problem

Previously, blocks used their **height** (block number) as the EVM `timestamp` field.
This meant `block.timestamp` was `0`, `1`, `2`, ... instead of a real Unix epoch value.
Any Solidity contract that called `block.timestamp` received a meaningless
value that bore no relation to wall-clock time, breaking time-dependent
logic such as timelocks, vesting schedules, and oracle freshness checks.

## Solution

Blocks now carry an explicit `timestamp: u64` field that records a real Unix
timestamp (seconds since the epoch). The timestamp is chosen at proposal time
by reading the system clock and ensuring the value is **strictly greater**
than the parent block's timestamp, which is a standard monotonicity invariant.

### Timestamp selection logic (`Block::next_timestamp`)

```
let timestamp = max(now_secs, parent_timestamp + 1)
```

- If the wall clock is ahead of the parent, the current time is used.
- If the clock lags (e.g. fast block production or clock skew), the parent
  timestamp is incremented by one second to guarantee monotonicity.
- If `parent_timestamp` is `u64::MAX`, the function returns `None` to signal
  that no valid timestamp can be produced (overflow protection).

### Genesis timestamp

The genesis block timestamp is now configurable via
`BootstrapConfig::genesis_timestamp` and is read from the `"timestamp"` field
in the genesis JSON file. When constructing a `BootstrapConfig` programmatically,
the default genesis timestamp is `0`.

## Files Modified

### `crates/node/domain/src/block.rs`
- Added `timestamp: u64` field to `Block`.
- Added `Block::next_timestamp(now_secs, parent_timestamp) -> Option<u64>`.
- Updated codec `Write`/`Read`/`EncodeSize` implementations to include `timestamp`.
- Added tests for timestamp-dependent block ID uniqueness and `next_timestamp` edge cases.

### `crates/node/domain/src/bootstrap.rs`
- Added `genesis_timestamp: u64` to `BootstrapConfig`.
- Added builder method `with_genesis_timestamp`.
- `BootstrapConfig::load` now reads and preserves the `"timestamp"` field from the genesis JSON.
- Added unit tests for default and loaded genesis timestamps.

### `crates/node/domain/src/idents.rs`
- Updated test block construction to include `timestamp`.

### `crates/node/consensus/src/error.rs`
- Added `ConsensusError::TimestampOverflow` variant for when a valid next
  timestamp cannot be produced.
- Added test for the error's `Display` implementation.

### `crates/node/consensus/src/proposal.rs`
- `build_proposal` and `build_proposal_async` now accept a `now_secs: u64`
  parameter and use `Block::next_timestamp` to derive the block timestamp.
- `block_context` helper now takes `timestamp` instead of using `height`.
- All test call sites updated to pass `now_secs`.

### `crates/node/consensus/src/application.rs`
- Updated mock block construction in tests to include `timestamp: 0`.

### `crates/node/runner/src/app.rs`
- `RevmApplication::propose` now reads the system clock via `unix_timestamp_secs`
  and passes the timestamp to `Block::next_timestamp`.
- Timestamp overflow is logged at `error` level and the proposal is skipped.
- Block build and proposal logging now includes the `timestamp` field.

### `crates/node/runner/src/runner.rs`
- `RevmContextProvider::context` uses `block.timestamp` instead of `block.height`.
- `ProductionRunner::run` initialises the ledger with
  `LedgerView::init_with_genesis_timestamp`, passing through the bootstrap
  genesis timestamp.

### `crates/node/ledger/src/lib.rs`
- Added `LedgerView::init_with_genesis_timestamp` and
  `init_with_config_and_genesis_timestamp`.
- The genesis block is constructed with the configured `genesis_timestamp`.
- The original `init` and `init_with_config` methods delegate with
  `genesis_timestamp = 0` for backward compatibility.
- Added `init_uses_configured_genesis_timestamp` test.

### `crates/node/reporters/src/lib.rs`
- `index_finalized_block` now sets `IndexedBlock.timestamp` from
  `block.timestamp` rather than `block_context.header.timestamp`.

### `crates/e2e/src/harness.rs`
- All block construction and context methods updated to thread `timestamp`.
- `TestApplication::propose` reads the clock and derives the timestamp
  identically to production.
- `TestContextProvider::context` uses `block.timestamp`.
- Ledger initialization uses `init_with_genesis_timestamp`.

## Breaking Changes

- **Block codec**: The on-wire encoding of `Block` now includes the
  `timestamp` field between `height` and `prevrandao`. Nodes running the old
  codec will fail to decode blocks from nodes running the new codec and vice
  versa. All nodes must be upgraded simultaneously.
- **`ProposalBuilder::build_proposal` / `build_proposal_async`**: These
  methods now require an additional `now_secs: u64` parameter.
- **`LedgerView::init`**: Still works, but callers that need a non-zero
  genesis timestamp must switch to `init_with_genesis_timestamp`.

## Testing

- **`block.rs`**: `next_timestamp_uses_clock_when_ahead`,
  `next_timestamp_advances_parent_when_clock_lags`,
  `next_timestamp_returns_none_at_u64_max`, `block_id_differs_by_timestamp` --
  cover the core timestamp selection logic and block identity.
- **`bootstrap.rs`**: `new_defaults_genesis_timestamp_to_zero`,
  `load_preserves_genesis_timestamp` -- verify the genesis timestamp flows
  through configuration.
- **`error.rs`**: `test_timestamp_overflow_display` -- verifies the new error
  variant renders correctly.
- **`proposal.rs`**: All existing proposal tests updated to pass `now_secs`,
  ensuring backward compatibility of the proposal builder.
- **`ledger/src/lib.rs`**: `init_uses_configured_genesis_timestamp` --
  confirms the ledger honours the configured genesis timestamp.
- **E2E harness**: The full e2e test suite exercises real-timestamp proposals
  end-to-end across multiple simulated nodes.
- All proposal tests pass `now_secs` to validate the timestamp threading.
