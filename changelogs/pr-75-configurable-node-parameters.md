# PR #75: Make validator runtime parameters configurable

## Problem

Several critical node runtime parameters were hardcoded across the codebase,
making it impossible to tune them without recompiling:

- **RPC bind address**: Always bound to `0.0.0.0:8545`, with no way to change
  it from configuration.
- **Gas limit**: Passed as a constructor argument to `ProductionRunner` even
  though it already lived in `config.execution.gas_limit`, creating a redundant
  source of truth that could drift.
- **Consensus tuning (Simplex)**: Buffer sizes, timeouts, leader/certification
  deadlines, and fetch concurrency were all compile-time constants
  (`NZUsize!(16 * 1024 * 1024)`, `Duration::from_secs(5)`, etc.).
- **Block codec limits**: Maximum transactions per block and maximum bytes per
  transaction were module-level constants in the runner.
- **Leader election**: Hardcoded `view % 4` assumed exactly four validators,
  producing incorrect leader rotation for any other validator set size.
- **Validator indexing**: The DKG ceremony produces 1-indexed share indices,
  but the leader election expected 0-indexed values. There was no explicit
  conversion, which could cause off-by-one leadership mismatches.

## Solution

All previously-hardcoded parameters are now expressed as configuration fields
with sensible defaults, so existing config files continue to work unchanged.

### Configuration additions

Two new nested config sections live under `[consensus]`:

```toml
[consensus.block_codec]
max_txs = 10000              # maximum transactions decoded per block
max_tx_bytes = 8388608        # maximum bytes per transaction (8 MiB)

[consensus.simplex]
replay_buffer_bytes = 16777216
write_buffer_bytes = 16777216
leader_timeout_secs = 5
certification_timeout_secs = 10
timeout_retry_secs = 2
fetch_timeout_secs = 5
activity_timeout_views = 20
skip_timeout_views = 10
fetch_concurrent = 8
```

Every field uses `NonZeroUsize` or `NonZeroU64` so that zero values are
rejected at deserialization time rather than causing division-by-zero or
silent misconfiguration at runtime.

### RPC bind address

The runner now reads `config.rpc.http_addr` (which already defaulted to
`0.0.0.0:8545`) instead of hardcoding the address. Invalid addresses produce
a clear error message at startup.

### Gas limit deduplication

`ProductionRunner::new()` no longer accepts a `gas_limit` parameter. The gas
limit is read from `config.execution.gas_limit` at runtime, eliminating the
duplicate source of truth.

### Leader election fix

`NodeState::with_validator_count()` replaces the old `view % 4` leader
calculation with `view % validator_count`, and the constructor validates that
`validator_index < validator_count`. The CLI converts the 1-indexed DKG
`share_index` to 0-based via `checked_sub(1)`, with an error if the index is
unexpectedly zero.

## Files modified

### `bin/kora/src/cli.rs`

- Reads `config.rpc.http_addr` and parses it into a `SocketAddr` with a
  descriptive error on failure.
- Converts `dkg_output.participants` (a `usize`) to `u32` with overflow
  checking, and rejects zero.
- Converts `dkg_output.share_index` from 1-indexed to 0-indexed using
  `checked_sub(1)`.
- Calls `NodeState::with_validator_count()` instead of `NodeState::new()`.
- Removes the `gas_limit` argument from `ProductionRunner::new()`.

### `crates/node/config/src/consensus.rs`

- Adds `ConsensusBlockCodecConfig` struct with `max_txs` and `max_tx_bytes`
  (`NonZeroUsize` fields with serde defaults).
- Adds `ConsensusSimplexConfig` struct with nine tuning parameters (buffer
  sizes, timeouts, concurrency) using `NonZeroUsize` and `NonZeroU64`.
- Adds `block_codec` and `simplex` fields to `ConsensusConfig` (both
  `#[serde(default)]`).
- Adds 11 `const fn` default constructors (one per NonZero field).
- Adds 11 `pub const` default values for use in downstream assertions.
- Adds tests: default value coverage, partial deserialization of both
  sub-configs, zero-value rejection for `NonZero` fields.

### `crates/node/config/src/lib.rs`

- Re-exports the two new config structs and all 11 default constants.

### `crates/node/rpc/src/state.rs`

- Adds `with_validator_count(chain_id, validator_index, validator_count)`
  constructor that stores a `NonZeroU32` validator count.
- `set_view()` now uses `view % validator_count` instead of `view % 4`.
- `NodeState::new()` is preserved for backward compatibility, delegating to
  `with_validator_count` with `DEFAULT_VALIDATOR_COUNT = 4`.
- Adds panics with descriptive messages when `validator_count == 0` or
  `validator_index >= validator_count`.
- Adds tests for non-four-validator leadership, zero-count rejection, and
  out-of-range index rejection.

### `crates/node/runner/src/runner.rs`

- Removes module-level `BLOCK_CODEC_MAX_TXS` and `BLOCK_CODEC_MAX_TX_BYTES`
  constants (now sourced from config).
- Changes `block_codec_cfg()` from a no-arg `const fn` to one that accepts
  `&ConsensusBlockCodecConfig`.
- Removes `gas_limit` field from `ProductionRunner`; reads it from
  `config.execution.gas_limit` in `run()`.
- Reads `config.consensus.simplex` for all Simplex engine parameters.
- Removes the unused `NZUsize` import.
- Adds a unit test verifying `block_codec_cfg()` correctly maps config values.

### `crates/node/config/README.md`

- Adds the `[consensus.block_codec]` and `[consensus.simplex]` sections to
  the example configuration schema.

### `crates/node/runner/README.md`

- Updates code examples to remove the `gas_limit` argument from
  `ProductionRunner::new()`.
- Removes `gas_limit` from the configuration parameters table.
- Adds a note that gas limit comes from `config.execution.gas_limit` at
  runtime.

## Breaking changes

- `ProductionRunner::new()` no longer accepts a `gas_limit` parameter. Callers
  that previously passed `gas_limit` must remove that argument; the runner will
  read it from the supplied `NodeConfig` at runtime.
- `NodeState::with_validator_count()` panics if `validator_count` is zero or
  if `validator_index >= validator_count`. Code that previously constructed
  `NodeState` with out-of-range indices will now panic at construction instead
  of silently producing incorrect leader rotation.

## Migration considerations

- **Config files**: No changes required. All new fields have `#[serde(default)]`
  with the same values that were previously hardcoded, so existing TOML/JSON
  configs continue to work identically.
- **Downstream callers of `ProductionRunner::new()`**: Remove the third
  (`gas_limit`) argument. The gas limit is now exclusively sourced from
  `config.execution.gas_limit`.
- **Tests using `NodeState::new()`**: The legacy constructor still works with
  the default four-validator assumption. Tests that need a different validator
  count should use `NodeState::with_validator_count()`.

## Testing

The test suite covers:

- **Default config values**: All 11 new `NonZero` fields match their declared
  default constants.
- **Serde round-trip**: JSON and TOML serialization/deserialization preserves
  all consensus config fields.
- **Partial deserialization**: Omitted `block_codec` or `simplex` sub-objects
  fall back to defaults; specifying only some fields within a sub-object leaves
  the rest at defaults.
- **Zero rejection**: `NonZeroUsize` and `NonZeroU64` fields correctly reject
  zero values during deserialization.
- **Leader election with variable validator counts**: Verifies correct leader
  rotation for 3-validator and 5-validator sets.
- **Validator index boundary**: `with_validator_count` panics when
  `validator_index >= validator_count` (e.g., index 5 with count 4).
- **Zero validator count**: `with_validator_count` panics when
  `validator_count == 0`.
- **Block codec config mapping**: The `block_codec_cfg()` function in the runner
  correctly converts `ConsensusBlockCodecConfig` to the domain `BlockCfg`.

Run with: `cargo test -p kora-config -p kora-rpc -p kora-runner`
