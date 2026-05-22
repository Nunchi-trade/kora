# PR #71: Pending Transaction & Mempool Subscription Support

## Problem

Before this change, Kora nodes had no way for external clients to receive
real-time notifications about transaction lifecycle events.  Wallets, block
explorers, and monitoring tools had to poll `eth_getTransactionByHash` or
`eth_getTransactionReceipt` in a loop to discover when a transaction was
accepted, included in a block, or evicted from the mempool.  This created
unnecessary RPC load and introduced latency between events and their
observation.

## Solution

This PR adds two WebSocket/SSE subscription endpoints that push transaction
lifecycle events to connected clients:

1. **`eth_subscribe("newPendingTransactions")`** -- standard Ethereum
   subscription that notifies clients whenever a new transaction enters the
   mempool.  Supports an optional `{ "fullTx": true }` parameter to receive
   the full `RpcTransaction` object instead of just the hash.

2. **`kora_subscribe("mempool")`** -- Kora-specific subscription that streams
   the full mempool lifecycle for every transaction: `TxAdded` (accepted into
   the pool), `TxIncluded` (finalized in a block), and `TxEvicted` (removed
   without inclusion, with a human-readable reason).

Both subscriptions use `tokio::sync::broadcast` channels so that multiple
WebSocket clients can subscribe independently without blocking the main
transaction processing pipeline.

## How It Works

### Event Flow

```
eth_sendRawTransaction
  --> EthApiImpl::broadcast_pending_tx()
      --> PendingTxEvent::Added          (eth_subscribe consumers)
      --> MempoolEvent::TxAdded          (kora_subscribe consumers)

Block finalized
  --> FinalizedReporter::report()
      --> publish_mempool_inclusions()
          --> MempoolEvent::TxIncluded   (kora_subscribe consumers)

Transaction replaced / removed from pool
  --> TransactionPool::remove_with_reason()
      --> MempoolEvent::TxEvicted        (kora_subscribe consumers)
```

### Channel Architecture

- `PendingTxEventSender` (`broadcast::Sender<PendingTxEvent>`) -- carries
  Ethereum-standard pending transaction notifications (hash or full tx).
- `MempoolEventSender` (`broadcast::Sender<MempoolEvent>`) -- carries
  Kora-specific mempool lifecycle events with richer metadata.
- Both channels are created in the runner, wired into the RPC server and the
  `FinalizedReporter`, and passed through to the subscription module.

## Breaking Changes

None.  All new types and endpoints are additive.  Existing RPC methods and
behavior are unchanged.  The `TransactionPool` API gains a new
`remove_with_reason()` method while the original `remove()` continues to work
unchanged (it delegates to `remove_with_reason` with reason `"removed"`).

## Migration Notes

- Node operators do not need to change configuration.  Subscriptions are
  available automatically when RPC is enabled.
- If the RPC is not configured, the broadcast channels are `None` and no events
  are emitted (zero overhead).

## Files Modified

### `crates/node/domain/Cargo.toml`
- Added `serde` feature to `alloy-primitives` for serializing `Address`, `B256`,
  and `U256` inside `MempoolEvent`.

### `crates/node/domain/src/events.rs`
- Added `MempoolEvent` enum with three variants: `TxAdded`, `TxIncluded`, and
  `TxEvicted`.
- `MempoolEvent` derives `Serialize`/`Deserialize` with `camelCase` serde
  renaming for JSON-RPC compatibility.
- Added `mempool_event_serde_roundtrip` unit test.

### `crates/node/domain/src/lib.rs`
- Re-exported `MempoolEvent` from the crate root.

### `crates/node/rpc/Cargo.toml`
- Added `kora-domain` dependency (needed for `MempoolEvent` type in
  subscriptions).

### `crates/node/rpc/src/subscription.rs` (new file)
- `PendingTxEvent` / `PendingTxInfo` -- types for Ethereum-standard pending
  transaction notifications.
- `subscription_module()` -- builds the `RpcModule` with `eth_subscribe` and
  `kora_subscribe` handlers.
- `pending_tx_channel()` / `mempool_event_channel()` -- factory functions for
  broadcast channels with default capacities (2048 / 4096).
- `recv_broadcast()` -- helper that handles `Lagged` errors by skipping missed
  events and logging a warning.
- Tests covering hash-only subscriptions, full-tx subscriptions, Kora mempool
  subscriptions, and lagged-receiver recovery.

### `crates/node/rpc/src/eth.rs`
- `EthApiImpl` gains optional `pending_tx_broadcast` and `mempool_broadcast`
  fields, set via builder methods `with_pending_tx_broadcast()` and
  `with_mempool_broadcast()`.
- `broadcast_pending_tx()` sends both `PendingTxEvent::Added` and
  `MempoolEvent::TxAdded` after a transaction is accepted via
  `eth_sendRawTransaction`.
- Tests verify that broadcasts fire on acceptance and do not fire when the raw
  transaction fails to decode.

### `crates/node/rpc/src/server.rs`
- `RpcServer` and `JsonRpcServer` gain `pending_tx_broadcast` and
  `mempool_broadcast` fields with corresponding builder methods.
- The subscription module is merged into the RPC module at startup.
- Debug impl updated to show broadcast channel presence.

### `crates/node/rpc/src/lib.rs`
- Re-exports the new subscription types and channel factory functions from the
  crate root.

### `crates/node/reporters/src/lib.rs`
- `FinalizedReporter` gains an optional `mempool_broadcast` field set via
  `with_mempool_broadcast()`.
- `publish_mempool_inclusions()` iterates finalized block transactions and sends
  `MempoolEvent::TxIncluded` for each one.
- Unit test verifies `TxIncluded` events are emitted with correct block
  number and hash.

### `crates/node/runner/src/runner.rs`
- Creates `pending_tx_broadcast` and `mempool_broadcast` channels when RPC is
  configured.
- Wires both channels into the `RpcServer` and the `FinalizedReporter`.

### `crates/node/txpool/Cargo.toml`
- Added `tokio` dependency with `sync` feature for `broadcast::Sender`.

### `crates/node/txpool/src/config.rs`
- No functional change; a blank line was added for consistency.

### `crates/node/txpool/src/pool.rs`
- `TransactionPool` gains an optional `events: Option<broadcast::Sender<MempoolEvent>>`
  field and a `new_with_events()` constructor.
- `add()` emits `MempoolEvent::TxEvicted` (reason: `"replaced"`) when a
  transaction at the same nonce is displaced, followed by `MempoolEvent::TxAdded`
  for the new transaction.
- `remove_with_reason()` emits `MempoolEvent::TxEvicted` with a caller-supplied
  reason string.
- `remove()` delegates to `remove_with_reason()` with reason `"removed"`.
- `tx_added_event()` helper constructs `MempoolEvent::TxAdded` from an
  `OrderedTransaction`.
- Tests cover: `TxAdded` on insert, `TxEvicted` on replacement, `TxEvicted` on
  remove, and custom eviction reasons.

### `Cargo.lock`
- Updated to reflect the new `kora-domain` dependency from `kora-rpc`.

## Testing

The following test cases cover the subscription functionality:

**Domain events (`kora-domain`)**
- `mempool_event_serde_roundtrip` -- verifies `MempoolEvent::TxAdded`
  serializes to JSON with camelCase field names and deserializes back
  identically.

**RPC subscriptions (`kora-rpc`)**
- `eth_pending_subscription_receives_hash` -- subscribes to
  `newPendingTransactions` and verifies the hash is received.
- `eth_pending_subscription_receives_full_tx` -- subscribes with `fullTx: true`
  and verifies the full `RpcTransaction` object is received.
- `kora_mempool_subscription_receives_event` -- subscribes to `kora_subscribe("mempool")`
  and verifies a `MempoolEvent::TxIncluded` event is received.
- `broadcast_receiver_skips_lagged_events` -- verifies the `recv_broadcast`
  helper correctly recovers from a lagged receiver by skipping to the latest
  available message.

**RPC broadcast integration (`kora-rpc`)**
- `eth_send_raw_transaction_broadcasts_after_acceptance` -- verifies that
  `eth_sendRawTransaction` emits both `PendingTxEvent` and `MempoolEvent`
  after successful validation.
- `invalid_raw_transaction_does_not_broadcast` -- verifies that a malformed
  transaction does not emit any broadcast events.

**Transaction pool events (`kora-txpool`)**
- `pool_broadcasts_tx_added_on_insert` -- verifies `MempoolEvent::TxAdded` is
  emitted when a transaction is added to the pool.
- `pool_broadcasts_replaced_transaction_as_evicted` -- verifies that replacing
  a transaction emits `TxEvicted` for the old transaction followed by `TxAdded`
  for the new one.
- `pool_remove_broadcasts_tx_evicted` -- verifies `remove()` emits `TxEvicted`
  with reason `"removed"`.
- `pool_remove_with_reason_broadcasts_custom_reason` -- verifies
  `remove_with_reason()` emits `TxEvicted` with the caller-supplied reason.

**Reporter integration (`kora-reporters`)**
- `publish_mempool_inclusions_broadcasts_tx_included` -- verifies that
  `publish_mempool_inclusions()` emits `TxIncluded` with the correct block
  number and block hash for each transaction in a finalized block.
