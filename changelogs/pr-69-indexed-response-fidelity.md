# PR #69: Preserve Indexed Transaction and Log Metadata for JSON-RPC Response Fidelity

## Problem

When the JSON-RPC server returned transactions, receipts, and logs fetched from the
block index, several fields were either zeroed out or hardcoded to placeholder
values. Specifically:

- **Transaction responses** were missing `tx_type`, `chain_id`, `max_fee_per_gas`,
  `max_priority_fee_per_gas`, and the signature components (`v`, `r`, `s`).
  All of these were returned as zero/null regardless of the actual transaction.

- **Receipt responses** were missing `logs_bloom`, `tx_type`, and
  `effective_gas_price`. The logs bloom was always empty bytes, the type was
  always `0x0`, and the effective gas price was always zero.

- **Log responses** (both within receipts and from `eth_getLogs`) were missing
  `block_number`, `block_hash`, `transaction_hash`, and `transaction_index`.
  These were returned as zero values, breaking any client that relies on log
  context to correlate events with their originating transactions and blocks.

- **Signature `v` field** was typed as `U64` in the RPC transaction struct,
  which cannot represent legacy EIP-155 `v` values (which can be `chain_id * 2 + 35`
  and exceed `u64::MAX` for very large chain IDs). The `v` value was also
  computed as raw y-parity (0 or 1) instead of the EIP-155 encoded value for
  legacy transactions.

These gaps caused Ethereum tooling (ethers.js, viem, Foundry cast, block
explorers) to reject or misinterpret responses, since the missing fields are
required by the Ethereum JSON-RPC specification.

## Solution

The fix enriches the indexer's data model and the RPC conversion layer so that
every field mandated by the Ethereum JSON-RPC specification is captured at
indexing time and faithfully reproduced in responses.

### How it works

1. **At indexing time** (when a finalized block is processed), the transaction
   envelope is decoded to extract the full set of metadata: transaction type,
   chain ID, EIP-1559 fee parameters, and the cryptographic signature (v, r, s).
   The `v` component is computed using `to_eip155_value` for legacy transactions
   to produce the correct EIP-155 encoded value.

2. **For receipts**, the logs bloom filter is computed from the receipt's logs
   using `alloy_primitives::logs_bloom`, the transaction type is carried through
   from the transaction metadata, and the effective gas price is calculated
   using the standard formula:
   `min(max_fee_per_gas, base_fee_per_gas + max_priority_fee_per_gas)`.

3. **For logs**, block-level and transaction-level context (block number, block
   hash, transaction hash, transaction index) is attached to each log entry at
   index time, so it is available when logs are returned individually via
   `eth_getLogs` or embedded in receipt responses.

4. **The `v` field type** in `RpcTransaction` was widened from `U64` to `U256`
   to accommodate the full EIP-155 value range.

5. **The pending transaction path** (`raw_tx_to_pending_rpc` in `eth.rs`) was
   updated to compute `v` using the same EIP-155 logic, ensuring consistency
   between pending and indexed transaction responses.

## Files Modified

- **`crates/storage/indexer/src/types.rs`** -- Added fields to `IndexedTransaction`
  (`tx_type`, `chain_id`, `max_fee_per_gas`, `max_priority_fee_per_gas`, `v`,
  `r`, `s`), `IndexedReceipt` (`logs_bloom`, `tx_type`, `effective_gas_price`),
  and `IndexedLog` (`block_number`, `block_hash`, `transaction_hash`,
  `transaction_index`).

- **`crates/storage/indexer/src/store.rs`** -- Updated all test helpers to
  populate the new fields.

- **`crates/node/reporters/src/lib.rs`** -- Extended `TxMetadata` and the
  `index_finalized_block` function to extract and propagate the new fields.
  Added helper functions: `signature_v`, `transaction_type`,
  `transaction_gas_price` (renamed from `effective_gas_price`),
  `max_fee_per_gas`, `max_priority_fee_per_gas`, and
  `receipt_effective_gas_price`. Added an integration test that constructs a
  real signed EIP-1559 transaction, indexes it, and verifies all fields.

- **`crates/node/reporters/Cargo.toml`** -- Added `k256` and `sha3` as
  dev-dependencies for the integration test's transaction signing.

- **`crates/node/rpc/src/types.rs`** -- Widened `RpcTransaction::v` from `U64`
  to `U256`.

- **`crates/node/rpc/src/eth.rs`** -- Updated `raw_tx_to_pending_rpc` to
  compute `v` using EIP-155 encoding for legacy transactions (matching the
  indexed path). Added a `signature_v` helper function.

- **`crates/node/rpc/src/indexed_provider.rs`** -- Updated `indexed_tx_to_rpc`
  and `indexed_receipt_to_rpc` to propagate the new fields instead of returning
  zeros. Updated `get_logs` to use per-log block/transaction metadata instead
  of block-level placeholders. Added tests for EIP-1559 field preservation,
  receipt metadata, and `get_logs` metadata.

- **`Cargo.lock`** -- Updated to reflect the new dev-dependencies.

## Breaking Changes

- **`IndexedTransaction`**, **`IndexedReceipt`**, and **`IndexedLog`** have new
  required fields. Any code that constructs these types directly (e.g., in tests
  or alternative indexer implementations) must be updated to provide the
  additional fields.

- **`RpcTransaction::v`** changed from `U64` to `U256`. Any code that reads
  this field and expects a `U64` type must be updated. The JSON serialization
  format is unchanged (both serialize as hex-encoded integers), so downstream
  JSON-RPC clients are not affected.

## Testing

The following tests cover these changes:

- **`kora-reporters::tests::finalized_index_preserves_transaction_receipt_and_log_metadata`** --
  End-to-end test that signs a real EIP-1559 transaction with k256, constructs
  a block and execution outcome, runs `index_finalized_block`, and verifies
  that the indexed transaction has correct `tx_type`, `chain_id`, gas fields,
  and non-zero signature components; that the receipt has the correct
  `effective_gas_price` (13 = min(20, 10+3)) and a non-zero logs bloom; and
  that logs carry the correct block and transaction metadata.

- **`kora-rpc::indexed_provider::tests::indexed_tx_preserves_eip1559_fields`** --
  Unit test verifying that `indexed_tx_to_rpc` correctly converts all new
  `IndexedTransaction` fields into the corresponding `RpcTransaction` fields.

- **`kora-rpc::indexed_provider::tests::indexed_receipt_preserves_fee_type_bloom_and_log_metadata`** --
  Unit test verifying that `indexed_receipt_to_rpc` correctly converts receipt
  type, effective gas price, logs bloom bytes, and per-log metadata.

- **`kora-rpc::indexed_provider::tests::get_logs_returns_indexed_block_and_transaction_metadata`** --
  Integration test that inserts a block with a receipt containing a log,
  queries via `get_logs`, and verifies that the returned RPC log carries the
  correct block number, block hash, transaction hash, transaction index, and
  log index.

- All pre-existing tests in `kora-indexer::store` and
  `kora-rpc::indexed_provider` have been updated to construct the enriched
  types and continue to pass.
