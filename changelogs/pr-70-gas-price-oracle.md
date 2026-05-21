# PR #70: Estimate gas fees from recent blocks

## Problem

The Ethereum JSON-RPC fee endpoints (`eth_gasPrice`, `eth_maxPriorityFeePerGas`,
and `eth_feeHistory`) previously returned hardcoded values of 1 gwei regardless
of actual on-chain activity. Wallets and client libraries rely on these endpoints
to choose transaction fees, so static responses led to poor fee suggestions,
all-zero reward percentiles, and a misleading fee market signal.

## Solution

This PR introduces a recent-block fee oracle that samples indexed block and
transaction data to produce dynamic fee estimates. The oracle:

- Scans a configurable window of recent blocks (default: 20) to collect
  transaction gas prices and priority fees.
- Computes a percentile-based estimate (default: 60th percentile) from the
  sampled values.
- Enforces configurable minimum and maximum bounds to prevent extreme values.
- Caches results by head block number so repeated fee queries within the same
  block do not rescan history.
- Falls back to safe defaults (1 gwei base + 1 gwei tip) when there are no
  transactions or no blocks available.

For `eth_feeHistory`, the implementation now returns real per-block data:
- Actual base fees from indexed blocks (with carry-forward for missing blocks).
- Computed gas used ratios from each block's gas_used / gas_limit.
- EIP-1559 next-block base fee prediction using the standard elasticity formula.
- Gas-weighted reward percentiles derived from transaction priority fees.

### Key design decisions

- **EIP-1559 effective price**: For type-2 (and later) transactions, the oracle
  uses `min(max_fee, base_fee + tip)` instead of the raw `gas_price` field, which
  for EIP-1559 transactions represents `max_fee_per_gas` and would inflate
  estimates.
- **Indexed EIP-1559 transactions without fee fields**: When a type-2+ transaction
  is missing `max_fee_per_gas` / `max_priority_fee_per_gas` (possible depending on
  the indexer), the oracle returns zero priority fee rather than computing a
  misleading value from `gas_price - base_fee`.
- **Max price bypass**: When the chain's base fee alone exceeds the configured
  `max_price`, the oracle still returns a usable price (base_fee + tip) instead of
  clamping to a value that would make transactions un-submittable.

## Files modified

### `crates/node/rpc/src/eth.rs`

- Added `GasOracleConfig` (public, configurable), `GasOracleEstimate`, and
  `CachedGasOracleEstimate` types.
- Added constants: `DEFAULT_GAS_ORACLE_BLOCKS`, `DEFAULT_GAS_ORACLE_PERCENTILE`,
  `GWEI`, `DEFAULT_MAX_GAS_PRICE`.
- Added `gas_oracle_config` and `gas_oracle_cache` fields to `EthApiImpl`.
- Added builder method `with_gas_oracle_config()` and internal constructor
  `from_parts()` to support oracle configuration.
- Added `recent_fee_estimate()` method that drives `eth_gasPrice` and
  `eth_maxPriorityFeePerGas`.
- Replaced hardcoded `eth_gasPrice` and `eth_maxPriorityFeePerGas` with
  oracle-derived values.
- Rewrote `eth_feeHistory` to return real block data instead of static values.
- Added helper functions: `estimate_recent_fees`, `block_by_number_or_none`,
  `resolve_fee_history_newest`, `default_base_fee`, `percentile_value`,
  `block_gas_used_ratio`, `compute_reward_percentiles`,
  `weighted_percentile_reward`, `percentile_threshold`, `effective_priority_fee`,
  `is_dynamic_fee_type`, `effective_gas_price_for_sampling`,
  `calculate_next_base_fee`.
- Added `MockFeeStateProvider` and test helpers (`make_fee_block`,
  `make_eip1559_fee_block`, `Eip1559TxParams`, `gwei`) for fee oracle testing.
- Added tests covering: oracle with recent transactions, empty-chain fallback,
  fee history base fees and gas ratios, fee history rewards (non-empty and empty
  blocks), EIP-1559 effective priority fee (normal, capped-at-headroom, missing
  fields), EIP-1559 effective gas price sampling, max_price enforcement,
  base-fee-above-cap bypass, calculate_next_base_fee (at/above/below target, zero
  limit), percentile edge cases, resolve_fee_history_newest with Earliest tag,
  multi-block fee_history structure, legacy tx gas price sampling, and
  block_gas_used_ratio edge cases.

### `crates/node/rpc/src/lib.rs`

- Added `GasOracleConfig` to the public re-exports so downstream crates can
  configure the gas oracle.

## Breaking changes

None. The public API is additive:
- `GasOracleConfig` is a new public type.
- `EthApiImpl::with_gas_oracle_config()` is a new builder method.
- Existing constructors (`new`, `with_tx_submit`) continue to work with default
  oracle settings.

The only behavioral change is that `eth_gasPrice`, `eth_maxPriorityFeePerGas`,
and `eth_feeHistory` now return dynamic values instead of hardcoded 1 gwei. This
is the intended fix and should not break any correctly-written clients (clients
that hardcoded expectations around 1 gwei responses were already working around
a bug).

## Migration considerations

No code changes required for existing consumers. The default oracle configuration
(20-block window, 60th percentile, 1 gwei min, 500 gwei max) is suitable for
most deployments. Operators who need different bounds can use
`EthApiImpl::with_gas_oracle_config()`.

## Testing

The test suite covers:
- **Happy path**: Gas price and priority fee derived from recent transaction data
  across multiple blocks.
- **Empty chain**: Fallback to base_fee + min_priority_fee when no transactions
  exist.
- **Fee history structure**: Base fees, gas used ratios, and next-block prediction
  from indexed blocks.
- **Fee history rewards**: Non-zero reward percentiles for blocks with
  transactions, zero rewards for empty blocks.
- **EIP-1559 correctness**: Effective priority fee uses min(tip, headroom), caps
  at headroom when tip exceeds it, and returns zero for indexed transactions
  missing fee fields.
- **Gas price sampling**: EIP-1559 transactions use effective gas price, not
  max_fee_per_gas.
- **Max price enforcement**: Gas price is clamped to max_price when base fee is
  below the cap.
- **Base fee above cap**: Oracle returns a usable price when base fee alone
  exceeds max_price.
- **EIP-1559 base fee calculation**: Next-block base fee increases above target,
  decreases below target, stays flat at target, and handles zero gas limit.
- **Percentile edge cases**: 0th and 100th percentile return min/max values;
  empty input returns None.
- **Block tag resolution**: Earliest tag resolves to block 0.
- **Multi-block fee history**: Correct array lengths across a 3-block window.
- **Legacy transactions**: Gas price sampling uses raw gas_price field.
- **Gas used ratio**: Handles zero gas limit and full blocks correctly.

Run with: `cargo test -p kora-rpc`
