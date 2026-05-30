//! Transaction pool implementation.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::{Address, B256, Bytes, U256};
use kora_domain::{MempoolEvent, Tx, TxId};
use kora_metrics::{AppMetrics, ReasonLabel};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};

use crate::{
    config::PoolConfig,
    error::TxPoolError,
    ordering::{OrderedTransaction, SenderQueue},
    traits::Mempool,
    validator::recover_sender_from_envelope,
};

#[derive(Debug)]
struct BuildSenderState {
    txs: Vec<OrderedTransaction>,
    index: usize,
    expected_nonce: u64,
}

impl BuildSenderState {
    fn next_candidate(&mut self, excluded: &BTreeSet<TxId>) -> Option<OrderedTransaction> {
        while let Some(tx) = self.txs.get(self.index) {
            if tx.nonce < self.expected_nonce {
                self.index += 1;
                continue;
            }

            if tx.nonce > self.expected_nonce {
                return None;
            }

            if excluded.contains(&ordered_tx_id(tx)) {
                self.expected_nonce = tx.nonce.saturating_add(1);
                self.index += 1;
                continue;
            }

            return Some(tx.clone());
        }

        None
    }

    const fn consume(&mut self) {
        self.expected_nonce = self.expected_nonce.saturating_add(1);
        self.index += 1;
    }
}

#[derive(Debug)]
struct PoolInner {
    by_hash: HashMap<B256, OrderedTransaction>,
    by_id: HashMap<TxId, B256>,
    by_sender: HashMap<Address, SenderQueue>,
    pending_count: usize,
    queued_count: usize,
}

impl PoolInner {
    fn new() -> Self {
        Self {
            by_hash: HashMap::new(),
            by_id: HashMap::new(),
            by_sender: HashMap::new(),
            pending_count: 0,
            queued_count: 0,
        }
    }

    fn update_counts(&mut self) {
        self.pending_count = self.by_sender.values().map(|q| q.pending_count()).sum();
        self.queued_count = self.by_sender.values().map(|q| q.queued_count()).sum();
    }

    fn remove_by_hash(&mut self, hash: &B256) -> Option<OrderedTransaction> {
        let tx = self.by_hash.remove(hash)?;
        self.by_id.remove(&ordered_tx_id(&tx));

        if let Some(queue) = self.by_sender.get_mut(&tx.sender) {
            queue.remove_by_hash(hash);
            if queue.is_empty() {
                self.by_sender.remove(&tx.sender);
            }
        }

        Some(tx)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InsertionTarget {
    Pending,
    Queued,
    Replacement,
}

fn existing_nonce_tx(queue: &SenderQueue, nonce: u64) -> Option<&OrderedTransaction> {
    queue.pending.iter().chain(queue.queued.iter()).find(|tx| tx.nonce == nonce)
}

fn insertion_target(queue: Option<&SenderQueue>, tx: &OrderedTransaction) -> InsertionTarget {
    let Some(queue) = queue else {
        return InsertionTarget::Pending;
    };

    if existing_nonce_tx(queue, tx.nonce).is_some() {
        return InsertionTarget::Replacement;
    }

    if tx.nonce == queue.next_nonce + queue.pending.len() as u64 {
        InsertionTarget::Pending
    } else {
        InsertionTarget::Queued
    }
}

/// A thread-safe transaction pool with nonce ordering and fee prioritization.
#[derive(Debug)]
pub struct TransactionPool {
    inner: Arc<RwLock<PoolInner>>,
    config: PoolConfig,
    events: Option<broadcast::Sender<MempoolEvent>>,
    metrics: Arc<RwLock<Option<AppMetrics>>>,
}

impl TransactionPool {
    /// Creates a new transaction pool with the given configuration.
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PoolInner::new())),
            config,
            events: None,
            metrics: Arc::new(RwLock::new(None)),
        }
    }

    /// Creates a new transaction pool that broadcasts mempool lifecycle events.
    #[must_use]
    pub fn new_with_events(config: PoolConfig, events: broadcast::Sender<MempoolEvent>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PoolInner::new())),
            config,
            events: Some(events),
            metrics: Arc::new(RwLock::new(None)),
        }
    }

    /// Attach application-level metrics to this pool.
    ///
    /// Because the metrics handle is shared across all clones of this pool,
    /// this method affects every clone that shares the same backing store.
    pub fn set_metrics(&self, metrics: AppMetrics) {
        *self.metrics.write() = Some(metrics);
    }

    /// Update gauge metrics to reflect current pool state.
    ///
    /// Must be called while the caller does NOT hold the inner lock (it takes
    /// a read lock internally).
    fn sync_metrics(&self) {
        let metrics_guard = self.metrics.read();
        if let Some(ref m) = *metrics_guard {
            let inner = self.inner.read();
            m.txpool_size.set(inner.by_hash.len() as i64);
            m.txpool_pending.set(inner.pending_count as i64);
            m.txpool_queued.set(inner.queued_count as i64);
        }
    }

    /// Record a rejected transaction metric.
    fn record_rejection(&self, reason: &str) {
        let metrics_guard = self.metrics.read();
        if let Some(ref m) = *metrics_guard {
            m.txpool_rejected.get_or_create(&ReasonLabel { reason: reason.to_string() }).inc();
        }
    }

    /// Adds a validated transaction to the pool.
    pub fn add(&self, tx: OrderedTransaction) -> Result<(), TxPoolError> {
        let added_event = tx_added_event(&tx);
        let mut replaced_hash = None;
        let mut evicted_hashes = Vec::new();

        let mut inner = self.inner.write();
        let tx_id = ordered_tx_id(&tx);

        if inner.by_hash.contains_key(&tx.hash) || inner.by_id.contains_key(&tx_id) {
            return Err(TxPoolError::AlreadyExists);
        }

        let sender = tx.sender;
        let target = insertion_target(inner.by_sender.get(&sender), &tx);

        if let Some(queue) = inner.by_sender.get(&sender) {
            if tx.nonce < queue.next_nonce {
                return Err(TxPoolError::NonceTooLow { got: tx.nonce, expected: queue.next_nonce });
            }

            if target != InsertionTarget::Replacement
                && queue.total_count() >= self.config.max_txs_per_sender
            {
                return Err(TxPoolError::SenderFull(sender));
            }
        }

        self.reject_underpriced_when_full(&inner, &tx, target)?;

        let queue =
            inner.by_sender.entry(sender).or_insert_with(|| SenderQueue::new(sender, tx.nonce));

        if let Some(replaced) = queue.insert(tx.clone()) {
            if replaced.hash == tx.hash {
                return Err(TxPoolError::ReplacementUnderpriced);
            }
            replaced_hash = Some(replaced.hash);
            inner.remove_by_hash(&replaced.hash);
            debug!(hash = ?replaced.hash, "replaced transaction");
        }

        let inserted_hash = tx.hash;
        inner.by_hash.insert(tx.hash, tx);
        inner.by_id.insert(tx_id, inserted_hash);
        inner.update_counts();

        let mut inserted_evicted = false;
        while inner.pending_count > self.config.max_pending_txs {
            let Some(evicted) = Self::evict_lowest_pending(&mut inner) else {
                break;
            };
            inserted_evicted |= evicted.hash == inserted_hash;
            evicted_hashes.push(evicted.hash);
            debug!(
                hash = ?evicted.hash,
                sender = ?evicted.sender,
                nonce = evicted.nonce,
                gas_price = evicted.effective_gas_price,
                "evicted lowest-fee pending transaction"
            );
        }

        while inner.queued_count > self.config.max_queued_txs {
            let Some(evicted) = Self::evict_lowest_queued(&mut inner) else {
                break;
            };
            inserted_evicted |= evicted.hash == inserted_hash;
            evicted_hashes.push(evicted.hash);
            debug!(
                hash = ?evicted.hash,
                sender = ?evicted.sender,
                nonce = evicted.nonce,
                gas_price = evicted.effective_gas_price,
                "evicted lowest-fee queued transaction"
            );
        }

        if inner.pending_count > self.config.max_pending_txs {
            warn!(
                count = inner.pending_count,
                max = self.config.max_pending_txs,
                "pool still exceeds pending limit after eviction"
            );
        }

        if inner.queued_count > self.config.max_queued_txs {
            warn!(
                count = inner.queued_count,
                max = self.config.max_queued_txs,
                "pool still exceeds queued limit after eviction"
            );
        }

        // Drop the write lock before sending events
        drop(inner);

        if let Some(events) = &self.events {
            if let Some(hash) = replaced_hash {
                let _ =
                    events.send(MempoolEvent::TxEvicted { hash, reason: "replaced".to_string() });
            }
            if !inserted_evicted {
                let _ = events.send(added_event);
            }
            for hash in &evicted_hashes {
                let _ = events
                    .send(MempoolEvent::TxEvicted { hash: *hash, reason: "evicted".to_string() });
            }
        }

        self.sync_metrics();

        if inserted_evicted {
            return Err(TxPoolError::PoolFull);
        }

        Ok(())
    }

    fn reject_underpriced_when_full(
        &self,
        inner: &PoolInner,
        tx: &OrderedTransaction,
        target: InsertionTarget,
    ) -> Result<(), TxPoolError> {
        match target {
            InsertionTarget::Pending => {
                if self.config.max_pending_txs == 0 {
                    return Err(TxPoolError::PoolFull);
                }
                if inner.pending_count >= self.config.max_pending_txs
                    && let Some(min_price) = Self::min_pending_price(inner)
                    && tx.effective_gas_price <= min_price
                {
                    return Err(TxPoolError::PoolFull);
                }
            }
            InsertionTarget::Queued => {
                if self.config.max_queued_txs == 0 {
                    return Err(TxPoolError::PoolFull);
                }
                if inner.queued_count >= self.config.max_queued_txs
                    && let Some(min_price) = Self::min_queued_price(inner)
                    && tx.effective_gas_price <= min_price
                {
                    return Err(TxPoolError::PoolFull);
                }
            }
            InsertionTarget::Replacement => {}
        }

        Ok(())
    }

    fn min_pending_price(inner: &PoolInner) -> Option<u128> {
        inner
            .by_sender
            .values()
            .flat_map(|queue| queue.pending.iter().map(|tx| tx.effective_gas_price))
            .min()
    }

    fn min_queued_price(inner: &PoolInner) -> Option<u128> {
        inner
            .by_sender
            .values()
            .flat_map(|queue| queue.queued.iter().map(|tx| tx.effective_gas_price))
            .min()
    }

    fn evict_lowest_pending(inner: &mut PoolInner) -> Option<OrderedTransaction> {
        let hash = inner
            .by_sender
            .values()
            .flat_map(|queue| queue.pending.iter())
            .min_by_key(|tx| (tx.effective_gas_price, std::cmp::Reverse(tx.timestamp), tx.hash))
            .map(|tx| tx.hash)?;
        let removed = inner.remove_by_hash(&hash);
        inner.update_counts();
        removed
    }

    fn evict_lowest_queued(inner: &mut PoolInner) -> Option<OrderedTransaction> {
        let hash = inner
            .by_sender
            .values()
            .flat_map(|queue| queue.queued.iter())
            .min_by_key(|tx| (tx.effective_gas_price, std::cmp::Reverse(tx.timestamp), tx.hash))
            .map(|tx| tx.hash)?;
        let removed = inner.remove_by_hash(&hash);
        inner.update_counts();
        removed
    }

    /// Returns pending transactions sorted by effective gas price.
    pub fn pending(&self, max_txs: usize) -> Vec<OrderedTransaction> {
        let inner = self.inner.read();

        let mut all_pending: Vec<_> =
            inner.by_sender.values().flat_map(|q| q.pending.iter().cloned()).collect();

        all_pending.sort();
        all_pending.truncate(max_txs);
        all_pending
    }

    /// Returns pending transactions for a specific sender.
    pub fn pending_for_sender(&self, sender: &Address) -> Vec<OrderedTransaction> {
        let inner = self.inner.read();
        inner.by_sender.get(sender).map(|q| q.pending.clone()).unwrap_or_default()
    }

    /// Returns the next expected nonce for `sender` after all pending
    /// (executable) transactions, or `None` if the sender has no queue.
    pub fn next_nonce(&self, sender: &Address) -> Option<u64> {
        let inner = self.inner.read();
        inner.by_sender.get(sender).map(SenderQueue::next_pending_nonce)
    }

    /// Gets a transaction by its hash.
    pub fn get(&self, hash: &B256) -> Option<OrderedTransaction> {
        self.inner.read().by_hash.get(hash).cloned()
    }

    /// Removes a transaction by its hash, emitting a `TxEvicted` event with the
    /// provided `reason`.
    pub fn remove_with_reason(&self, hash: &B256, reason: &str) -> Option<OrderedTransaction> {
        let mut inner = self.inner.write();
        let tx = inner.remove_by_hash(hash)?;
        inner.update_counts();
        drop(inner);

        if let Some(events) = &self.events {
            let _ =
                events.send(MempoolEvent::TxEvicted { hash: *hash, reason: reason.to_string() });
        }

        self.sync_metrics();
        Some(tx)
    }

    /// Removes a transaction by its hash.
    pub fn remove(&self, hash: &B256) -> Option<OrderedTransaction> {
        self.remove_with_reason(hash, "removed")
    }

    /// Removes confirmed transactions for a sender up to the given nonce.
    pub fn remove_confirmed(&self, sender: &Address, confirmed_nonce: u64) {
        let mut inner = self.inner.write();

        let hashes_to_remove: Vec<B256> = inner
            .by_sender
            .get(sender)
            .map(|queue| {
                queue
                    .pending
                    .iter()
                    .chain(queue.queued.iter())
                    .filter(|tx| tx.nonce <= confirmed_nonce)
                    .map(|tx| tx.hash)
                    .collect()
            })
            .unwrap_or_default();

        for hash in hashes_to_remove {
            inner.remove_by_hash(&hash);
        }

        if let Some(queue) = inner.by_sender.get_mut(sender) {
            queue.remove_confirmed(confirmed_nonce);
            if queue.is_empty() {
                inner.by_sender.remove(sender);
            }
        }

        inner.update_counts();
        drop(inner);
        self.sync_metrics();
    }

    /// Returns the count of pending (executable) transactions.
    pub fn pending_count(&self) -> usize {
        self.inner.read().pending_count
    }

    /// Returns the count of queued (future nonce) transactions.
    pub fn queued_count(&self) -> usize {
        self.inner.read().queued_count
    }

    /// Returns the total number of transactions in the pool.
    pub fn len(&self) -> usize {
        self.inner.read().by_hash.len()
    }

    /// Returns true if the pool contains no transactions.
    pub fn is_empty(&self) -> bool {
        self.inner.read().by_hash.is_empty()
    }

    /// Returns all senders with transactions in the pool.
    pub fn senders(&self) -> Vec<Address> {
        self.inner.read().by_sender.keys().cloned().collect()
    }

    /// Checks if a transaction with the given hash exists in the pool.
    pub fn contains(&self, hash: &B256) -> bool {
        self.inner.read().by_hash.contains_key(hash)
    }

    /// Returns `true` if the pool already contains a transaction from `sender`
    /// with the given `nonce`.
    ///
    /// This is a cheap, synchronous check (read-lock only) intended for use by
    /// the transaction validator to reject same-nonce duplicates at ingress.
    pub fn has_nonce(&self, sender: &Address, nonce: u64) -> bool {
        let inner = self.inner.read();
        let Some(queue) = inner.by_sender.get(sender) else {
            return false;
        };
        queue.pending.iter().chain(queue.queued.iter()).any(|tx| tx.nonce == nonce)
    }

    /// Returns all sender queues for pool introspection.
    pub fn snapshot(&self) -> HashMap<Address, (Vec<OrderedTransaction>, Vec<OrderedTransaction>)> {
        self.inner
            .read()
            .by_sender
            .iter()
            .map(|(sender, queue)| (*sender, (queue.pending.clone(), queue.queued.clone())))
            .collect()
    }

    /// Removes expired transactions and returns the number removed.
    pub fn cleanup(&self) -> usize {
        let now = current_timestamp();
        let mut inner = self.inner.write();
        let expired: Vec<B256> = inner
            .by_sender
            .values()
            .flat_map(|queue| {
                let pending = queue.pending.iter().filter_map(|tx| {
                    (now.saturating_sub(tx.timestamp) > self.config.pending_ttl_secs)
                        .then_some(tx.hash)
                });
                let queued = queue.queued.iter().filter_map(|tx| {
                    (now.saturating_sub(tx.timestamp) > self.config.queued_ttl_secs)
                        .then_some(tx.hash)
                });
                pending.chain(queued)
            })
            .collect();

        let mut removed = 0;
        for hash in expired {
            if inner.remove_by_hash(&hash).is_some() {
                removed += 1;
            }
        }
        inner.update_counts();
        drop(inner);
        if removed > 0 {
            self.sync_metrics();
        }
        removed
    }

    /// Returns the pool configuration.
    pub const fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Removes all transactions from the pool.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.by_hash.clear();
        inner.by_id.clear();
        inner.by_sender.clear();
        inner.pending_count = 0;
        inner.queued_count = 0;
        drop(inner);
        self.sync_metrics();
    }
}

impl Clone for TransactionPool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            events: self.events.clone(),
            metrics: self.metrics.clone(), // Arc clone: all clones share the same metrics handle
        }
    }
}

fn tx_added_event(tx: &OrderedTransaction) -> MempoolEvent {
    MempoolEvent::TxAdded {
        hash: tx.hash,
        from: tx.sender,
        to: tx.envelope.to(),
        value: tx.envelope.value(),
        gas_price: U256::from(tx.effective_gas_price),
        nonce: tx.nonce,
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn ordered_to_tx(tx: &OrderedTransaction) -> Tx {
    let mut raw = Vec::new();
    tx.envelope.encode_2718(&mut raw);
    Tx::new(Bytes::from(raw))
}

fn ordered_tx_id(tx: &OrderedTransaction) -> TxId {
    ordered_to_tx(tx).id()
}

/// Map a [`TxPoolError`] to a short label suitable for the `reason`
/// dimension of the `kora_txpool_rejected_total` metric.
fn rejection_reason(err: &TxPoolError) -> String {
    match err {
        TxPoolError::PoolFull => "pool_full".to_string(),
        TxPoolError::SenderFull(_) => "sender_full".to_string(),
        TxPoolError::TxTooLarge { .. } => "tx_too_large".to_string(),
        TxPoolError::GasPriceTooLow { .. } => "gas_price_too_low".to_string(),
        TxPoolError::NonceTooLow { .. } => "nonce_too_low".to_string(),
        TxPoolError::NonceGap { .. } => "nonce_gap".to_string(),
        TxPoolError::InsufficientBalance { .. } => "insufficient_balance".to_string(),
        TxPoolError::InvalidChainId { .. } => "invalid_chain_id".to_string(),
        TxPoolError::InvalidSignature => "invalid_signature".to_string(),
        TxPoolError::DecodeError(_) => "decode_error".to_string(),
        TxPoolError::IntrinsicGasTooLow { .. } => "intrinsic_gas_too_low".to_string(),
        TxPoolError::AlreadyExists => "already_exists".to_string(),
        TxPoolError::NonceAlreadyInPool { .. } => "nonce_already_in_pool".to_string(),
        TxPoolError::StateError(_) => "state_error".to_string(),
        TxPoolError::ReplacementUnderpriced => "replacement_underpriced".to_string(),
        TxPoolError::BlobValidation(_) => "blob_validation".to_string(),
    }
}

fn tx_to_ordered(tx: &Tx) -> Option<OrderedTransaction> {
    let envelope = TxEnvelope::decode_2718(&mut tx.bytes.as_ref()).ok()?;
    let sender = recover_sender_from_envelope(&envelope).ok()?;
    let hash = alloy_primitives::keccak256(&tx.bytes);
    let nonce = envelope.nonce();
    let effective_gas_price = match &envelope {
        TxEnvelope::Legacy(tx) => tx.tx().gas_price,
        TxEnvelope::Eip2930(tx) => tx.tx().gas_price,
        TxEnvelope::Eip1559(tx) => tx.tx().max_fee_per_gas,
        TxEnvelope::Eip4844(tx) => tx.tx().tx().max_fee_per_gas,
        TxEnvelope::Eip7702(tx) => tx.tx().max_fee_per_gas,
    };

    Some(OrderedTransaction::new(
        hash,
        sender,
        nonce,
        effective_gas_price,
        current_timestamp(),
        envelope,
    ))
}

impl Mempool for TransactionPool {
    fn insert(&self, tx: Tx) -> bool {
        let Some(ordered) = tx_to_ordered(&tx) else {
            trace!("failed to decode transaction for mempool insert");
            self.record_rejection("decode_error");
            return false;
        };

        match self.add(ordered) {
            Ok(()) => true,
            Err(e) => {
                trace!(?e, "failed to insert transaction");
                self.record_rejection(&rejection_reason(&e));
                false
            }
        }
    }

    fn build(&self, max_txs: usize, excluded: &BTreeSet<TxId>) -> Vec<Tx> {
        let inner = self.inner.read();
        let mut senders: HashMap<Address, BuildSenderState> = inner
            .by_sender
            .iter()
            .filter(|(_, queue)| !queue.pending.is_empty())
            .map(|(sender, queue)| {
                (*sender, BuildSenderState {
                    txs: queue.pending.clone(),
                    index: 0,
                    expected_nonce: queue.next_nonce,
                })
            })
            .collect();
        let pending_count = senders.values().map(|state| state.txs.len()).sum();
        let mut result = Vec::with_capacity(max_txs.min(pending_count));

        while result.len() < max_txs {
            let Some((sender, tx)) = senders
                .iter_mut()
                .filter_map(|(sender, state)| {
                    state.next_candidate(excluded).map(|tx| (*sender, tx))
                })
                .min_by(|(_, left), (_, right)| left.cmp(right))
            else {
                break;
            };

            if let Some(state) = senders.get_mut(&sender) {
                state.consume();
                result.push(ordered_to_tx(&tx));
            }
        }

        result
    }

    fn prune(&self, tx_ids: &[TxId]) {
        let mut inner = self.inner.write();

        let mut confirmed_by_sender: HashMap<Address, u64> = HashMap::new();
        for id in tx_ids {
            let Some(hash) = inner.by_id.get(id) else {
                continue;
            };
            if let Some(tx) = inner.by_hash.get(hash) {
                confirmed_by_sender
                    .entry(tx.sender)
                    .and_modify(|nonce| *nonce = (*nonce).max(tx.nonce))
                    .or_insert(tx.nonce);
            }
        }

        let mut senders_to_check: Vec<Address> = Vec::with_capacity(confirmed_by_sender.len());
        let mut hashes_to_remove = Vec::new();
        for (sender, confirmed_nonce) in confirmed_by_sender {
            if let Some(queue) = inner.by_sender.get_mut(&sender) {
                hashes_to_remove.extend(
                    queue
                        .pending
                        .iter()
                        .chain(queue.queued.iter())
                        .filter(|tx| tx.nonce <= confirmed_nonce)
                        .map(|tx| tx.hash),
                );
                queue.remove_confirmed(confirmed_nonce);
                senders_to_check.push(sender);
            }
        }

        for hash in hashes_to_remove {
            inner.remove_by_hash(&hash);
        }

        for sender in senders_to_check {
            if let Some(queue) = inner.by_sender.get(&sender)
                && queue.is_empty()
            {
                inner.by_sender.remove(&sender);
            }
        }

        inner.update_counts();
        drop(inner);
        self.sync_metrics();
    }

    fn len(&self) -> usize {
        self.inner.read().by_hash.len()
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction as _, TxEip1559};
    use alloy_primitives::{Signature, TxKind, U256};
    use rand::Rng;

    use super::*;

    fn random_b256() -> B256 {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill(&mut bytes);
        B256::from(bytes)
    }

    fn random_address() -> Address {
        let mut bytes = [0u8; 20];
        rand::thread_rng().fill(&mut bytes);
        Address::from(bytes)
    }

    fn make_ordered_tx(sender: Address, nonce: u64, gas_price: u128) -> OrderedTransaction {
        let inner = TxEip1559 {
            chain_id: 1,
            nonce,
            gas_limit: 21000,
            max_fee_per_gas: gas_price,
            max_priority_fee_per_gas: gas_price,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let sig = Signature::from_scalars_and_parity(B256::ZERO, B256::ZERO, false);
        let signed = inner.into_signed(sig);
        let envelope = TxEnvelope::from(signed);
        OrderedTransaction::new(random_b256(), sender, nonce, gas_price, 0, envelope)
    }

    fn tx_nonce(tx: &Tx) -> u64 {
        let mut data = tx.bytes.as_ref();
        TxEnvelope::decode_2718(&mut data).unwrap().nonce()
    }

    fn tx_nonce_and_gas_price(tx: &Tx) -> (u64, u128) {
        let mut data = tx.bytes.as_ref();
        let envelope = TxEnvelope::decode_2718(&mut data).unwrap();
        let gas_price = match &envelope {
            TxEnvelope::Legacy(tx) => tx.tx().gas_price,
            TxEnvelope::Eip2930(tx) => tx.tx().gas_price,
            TxEnvelope::Eip1559(tx) => tx.tx().max_fee_per_gas,
            TxEnvelope::Eip4844(tx) => tx.tx().tx().max_fee_per_gas,
            TxEnvelope::Eip7702(tx) => tx.tx().max_fee_per_gas,
        };
        (envelope.nonce(), gas_price)
    }

    #[test]
    fn pool_add_and_pending() {
        let config = PoolConfig::default();
        let pool = TransactionPool::new(config);

        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx1 = make_ordered_tx(sender, 1, 100);

        pool.add(tx0).unwrap();
        pool.add(tx1).unwrap();

        assert_eq!(pool.pending_count(), 2);
        assert_eq!(pool.len(), 2);

        let pending = pool.pending(10);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn pool_broadcasts_tx_added_on_insert() {
        let (events, mut receiver) = broadcast::channel(16);
        let pool = TransactionPool::new_with_events(PoolConfig::default(), events);
        let sender = random_address();
        let tx = make_ordered_tx(sender, 0, 100);

        pool.add(tx.clone()).unwrap();

        let event = receiver.try_recv().unwrap();
        assert_eq!(event, MempoolEvent::TxAdded {
            hash: tx.hash,
            from: tx.sender,
            to: tx.envelope.to(),
            value: tx.envelope.value(),
            gas_price: U256::from(tx.effective_gas_price),
            nonce: tx.nonce,
        });
    }

    #[test]
    fn pool_broadcasts_replaced_transaction_as_evicted() {
        let (events, mut receiver) = broadcast::channel(16);
        let pool = TransactionPool::new_with_events(PoolConfig::default(), events);
        let sender = random_address();
        let low_fee = make_ordered_tx(sender, 0, 100);
        let high_fee = make_ordered_tx(sender, 0, 200);

        pool.add(low_fee.clone()).unwrap();
        pool.add(high_fee.clone()).unwrap();

        let _ = receiver.try_recv().unwrap();
        assert_eq!(receiver.try_recv().unwrap(), MempoolEvent::TxEvicted {
            hash: low_fee.hash,
            reason: "replaced".to_string()
        });
        assert!(matches!(
            receiver.try_recv().unwrap(),
            MempoolEvent::TxAdded { hash, .. } if hash == high_fee.hash
        ));
    }

    #[test]
    fn pool_duplicate_rejected() {
        let config = PoolConfig::default();
        let pool = TransactionPool::new(config);

        let sender = random_address();
        let tx = make_ordered_tx(sender, 0, 100);
        let tx_dup = tx.clone();

        pool.add(tx).unwrap();
        assert!(matches!(pool.add(tx_dup), Err(TxPoolError::AlreadyExists)));
    }

    #[test]
    fn pool_sender_limit() {
        let config = PoolConfig::default().with_max_txs_per_sender(2);
        let pool = TransactionPool::new(config);

        let sender = random_address();
        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();
        pool.add(make_ordered_tx(sender, 1, 100)).unwrap();

        assert!(matches!(
            pool.add(make_ordered_tx(sender, 2, 100)),
            Err(TxPoolError::SenderFull(_))
        ));
    }

    #[test]
    fn pool_evicts_lowest_fee_pending_on_overflow() {
        let config = PoolConfig::default().with_max_pending_txs(3);
        let pool = TransactionPool::new(config);

        let tx_low = make_ordered_tx(random_address(), 0, 10);
        let tx_med = make_ordered_tx(random_address(), 0, 20);
        let tx_high = make_ordered_tx(random_address(), 0, 30);
        let tx_new = make_ordered_tx(random_address(), 0, 15);

        pool.add(tx_low.clone()).unwrap();
        pool.add(tx_med.clone()).unwrap();
        pool.add(tx_high.clone()).unwrap();
        pool.add(tx_new.clone()).unwrap();

        assert_eq!(pool.pending_count(), 3);
        assert!(!pool.contains(&tx_low.hash));
        assert!(pool.contains(&tx_new.hash));
        assert!(pool.contains(&tx_med.hash));
        assert!(pool.contains(&tx_high.hash));
    }

    #[test]
    fn pool_rejects_low_fee_pending_when_full() {
        let config = PoolConfig::default().with_max_pending_txs(2);
        let pool = TransactionPool::new(config);

        pool.add(make_ordered_tx(random_address(), 0, 100)).unwrap();
        pool.add(make_ordered_tx(random_address(), 0, 200)).unwrap();

        let low_fee = make_ordered_tx(random_address(), 0, 50);
        assert!(matches!(pool.add(low_fee), Err(TxPoolError::PoolFull)));
        assert_eq!(pool.pending_count(), 2);
    }

    #[test]
    fn pool_evicts_lowest_fee_queued_on_overflow() {
        let config = PoolConfig::default().with_max_queued_txs(2);
        let pool = TransactionPool::new(config);
        let sender = random_address();

        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx2_low = make_ordered_tx(sender, 2, 10);
        let tx3_high = make_ordered_tx(sender, 3, 30);
        let tx4_mid = make_ordered_tx(sender, 4, 20);

        pool.add(tx0).unwrap();
        pool.add(tx2_low.clone()).unwrap();
        pool.add(tx3_high.clone()).unwrap();
        pool.add(tx4_mid.clone()).unwrap();

        assert_eq!(pool.queued_count(), 2);
        assert!(!pool.contains(&tx2_low.hash));
        assert!(pool.contains(&tx3_high.hash));
        assert!(pool.contains(&tx4_mid.hash));
    }

    #[test]
    fn pool_rejects_low_fee_queued_when_full() {
        let config = PoolConfig::default().with_max_queued_txs(1);
        let pool = TransactionPool::new(config);
        let sender = random_address();

        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();
        pool.add(make_ordered_tx(sender, 2, 100)).unwrap();

        let low_fee = make_ordered_tx(sender, 3, 50);
        assert!(matches!(pool.add(low_fee), Err(TxPoolError::PoolFull)));
        assert_eq!(pool.queued_count(), 1);
    }

    #[test]
    fn pool_eviction_preserves_sender_nonce_gap() {
        let config = PoolConfig::default().with_max_pending_txs(2);
        let pool = TransactionPool::new(config);
        let sender = random_address();

        let tx0_low = make_ordered_tx(sender, 0, 10);
        let tx1_high = make_ordered_tx(sender, 1, 100);
        let other = make_ordered_tx(random_address(), 0, 50);

        pool.add(tx0_low.clone()).unwrap();
        pool.add(tx1_high.clone()).unwrap();
        pool.add(other.clone()).unwrap();

        assert!(!pool.contains(&tx0_low.hash));
        assert!(pool.contains(&tx1_high.hash));
        assert_eq!(pool.pending_count(), 1);
        assert_eq!(pool.queued_count(), 1);

        let built = pool.build(10, &BTreeSet::new());
        assert_eq!(built.len(), 1);
        assert_eq!(tx_nonce(&built[0]), other.nonce);

        let tx0_replacement = make_ordered_tx(sender, 0, 200);
        pool.add(tx0_replacement.clone()).unwrap();

        let built = pool.build(10, &BTreeSet::new());
        assert_eq!(built.len(), 2);
        assert_eq!(tx_nonce(&built[0]), tx0_replacement.nonce);
        assert_eq!(tx_nonce(&built[1]), tx1_high.nonce);
    }

    #[test]
    fn pool_cleanup_removes_expired_transactions() {
        let config = PoolConfig::default().with_pending_ttl_secs(60).with_queued_ttl_secs(60 * 60);
        let pool = TransactionPool::new(config);

        let sender = random_address();
        let mut expired = make_ordered_tx(sender, 0, 100);
        expired.timestamp = current_timestamp().saturating_sub(120);
        pool.add(expired.clone()).unwrap();

        let removed = pool.cleanup();
        assert_eq!(removed, 1);
        assert!(!pool.contains(&expired.hash));
        assert!(pool.is_empty());
    }

    #[test]
    fn pool_remove() {
        let config = PoolConfig::default();
        let pool = TransactionPool::new(config);

        let sender = random_address();
        let tx = make_ordered_tx(sender, 0, 100);
        let hash = tx.hash;

        pool.add(tx).unwrap();
        assert_eq!(pool.len(), 1);

        pool.remove(&hash);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn pool_remove_confirmed_removes_queued_hashes() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx2 = make_ordered_tx(sender, 2, 100);

        pool.add(tx0.clone()).unwrap();
        pool.add(tx2.clone()).unwrap();

        assert_eq!(pool.len(), 2);
        assert_eq!(pool.pending_count(), 1);
        assert_eq!(pool.queued_count(), 1);
        assert!(pool.contains(&tx2.hash));

        pool.remove_confirmed(&sender, 2);

        assert_eq!(pool.len(), 0);
        assert_eq!(pool.pending_count(), 0);
        assert_eq!(pool.queued_count(), 0);
        assert!(!pool.contains(&tx0.hash));
        assert!(!pool.contains(&tx2.hash));
    }

    #[test]
    fn pool_remove_confirmed_preserves_queued_progress_after_gap() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx2 = make_ordered_tx(sender, 2, 100);

        pool.add(tx0).unwrap();
        pool.add(tx2.clone()).unwrap();
        pool.remove_confirmed(&sender, 0);

        assert_eq!(pool.len(), 1);
        assert!(pool.contains(&tx2.hash));
        assert!(pool.build(10, &BTreeSet::new()).is_empty());

        let tx1 = make_ordered_tx(sender, 1, 100);
        pool.add(tx1.clone()).unwrap();

        let txs = pool.build(10, &BTreeSet::new());
        assert_eq!(txs.len(), 2);
        assert_eq!(tx_nonce(&txs[0]), tx1.nonce);
        assert_eq!(tx_nonce(&txs[1]), tx2.nonce);
    }

    #[test]
    fn pool_clear() {
        let config = PoolConfig::default();
        let pool = TransactionPool::new(config);

        let sender = random_address();
        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();
        pool.add(make_ordered_tx(sender, 1, 100)).unwrap();

        pool.clear();
        assert!(pool.is_empty());
    }

    #[test]
    fn pool_prune_advances_sender_nonce() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx1 = make_ordered_tx(sender, 1, 100);
        let tx2 = make_ordered_tx(sender, 2, 100);
        let tx3 = make_ordered_tx(sender, 3, 100);

        pool.add(tx0.clone()).unwrap();
        pool.add(tx1.clone()).unwrap();
        pool.add(tx2.clone()).unwrap();
        pool.add(tx3.clone()).unwrap();

        pool.prune(&[ordered_tx_id(&tx0), ordered_tx_id(&tx1)]);

        let txs = pool.build(10, &BTreeSet::new());
        assert_eq!(txs.len(), 2);
        assert_eq!(tx_nonce(&txs[0]), tx2.nonce);
        assert_eq!(tx_nonce(&txs[1]), tx3.nonce);
    }

    #[test]
    fn pool_prune_uses_domain_tx_ids() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx1 = make_ordered_tx(sender, 1, 100);

        pool.add(tx0.clone()).unwrap();
        pool.add(tx1.clone()).unwrap();

        let built = pool.build(10, &BTreeSet::new());
        assert_eq!(built.len(), 2);

        let ids: Vec<TxId> = built.iter().map(Tx::id).collect();
        pool.prune(&ids[..1]);

        assert!(!pool.contains(&tx0.hash));
        assert!(pool.contains(&tx1.hash));
        let rebuilt = pool.build(10, &BTreeSet::new());
        assert_eq!(rebuilt.len(), 1);
        assert_eq!(tx_nonce(&rebuilt[0]), tx1.nonce);
    }

    #[test]
    fn pool_build_treats_excluded_ancestors_as_nonce_progress() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx1 = make_ordered_tx(sender, 1, 100);
        let tx2 = make_ordered_tx(sender, 2, 100);

        pool.add(tx0.clone()).unwrap();
        pool.add(tx1.clone()).unwrap();
        pool.add(tx2.clone()).unwrap();

        let excluded = BTreeSet::from([ordered_tx_id(&tx0)]);
        let txs = pool.build(10, &excluded);

        assert_eq!(txs.len(), 2);
        assert_eq!(tx_nonce(&txs[0]), tx1.nonce);
        assert_eq!(tx_nonce(&txs[1]), tx2.nonce);
    }

    #[test]
    fn pool_remove_broadcasts_tx_evicted() {
        let (events, mut receiver) = broadcast::channel(16);
        let pool = TransactionPool::new_with_events(PoolConfig::default(), events);
        let sender = random_address();
        let tx = make_ordered_tx(sender, 0, 100);
        let hash = tx.hash;

        pool.add(tx).unwrap();
        // drain the TxAdded event
        let _ = receiver.try_recv().unwrap();

        pool.remove(&hash);

        assert_eq!(receiver.try_recv().unwrap(), MempoolEvent::TxEvicted {
            hash,
            reason: "removed".to_string()
        });
    }

    #[test]
    fn pool_remove_with_reason_broadcasts_custom_reason() {
        let (events, mut receiver) = broadcast::channel(16);
        let pool = TransactionPool::new_with_events(PoolConfig::default(), events);
        let sender = random_address();
        let tx = make_ordered_tx(sender, 0, 100);
        let hash = tx.hash;

        pool.add(tx).unwrap();
        // drain the TxAdded event
        let _ = receiver.try_recv().unwrap();

        pool.remove_with_reason(&hash, "expired");

        assert_eq!(receiver.try_recv().unwrap(), MempoolEvent::TxEvicted {
            hash,
            reason: "expired".to_string()
        });
    }

    #[test]
    fn pool_prune_batches_highest_confirmed_nonce_per_sender() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender_a = random_address();
        let sender_b = random_address();
        let a0 = make_ordered_tx(sender_a, 0, 100);
        let a1 = make_ordered_tx(sender_a, 1, 100);
        let a2 = make_ordered_tx(sender_a, 2, 100);
        let a3 = make_ordered_tx(sender_a, 3, 100);
        let b0 = make_ordered_tx(sender_b, 0, 101);
        let b1 = make_ordered_tx(sender_b, 1, 101);

        for tx in [&a0, &a1, &a2, &a3, &b0, &b1] {
            pool.add(tx.clone()).unwrap();
        }

        pool.prune(&[ordered_tx_id(&a1), ordered_tx_id(&b0)]);

        assert_eq!(pool.len(), 3);
        assert!(!pool.contains(&a0.hash));
        assert!(!pool.contains(&a1.hash));
        assert!(!pool.contains(&b0.hash));
        assert!(pool.contains(&a2.hash));
        assert!(pool.contains(&a3.hash));
        assert!(pool.contains(&b1.hash));

        let sender_a_nonces: Vec<_> =
            pool.pending_for_sender(&sender_a).into_iter().map(|tx| tx.nonce).collect();
        let sender_b_nonces: Vec<_> =
            pool.pending_for_sender(&sender_b).into_iter().map(|tx| tx.nonce).collect();
        assert_eq!(sender_a_nonces, vec![2, 3]);
        assert_eq!(sender_b_nonces, vec![1]);
    }

    #[test]
    fn pool_prune_promotes_queued_transactions_after_gap_fills() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        let tx0 = make_ordered_tx(sender, 0, 100);
        let tx2 = make_ordered_tx(sender, 2, 100);

        pool.add(tx0.clone()).unwrap();
        pool.add(tx2.clone()).unwrap();
        pool.prune(&[ordered_tx_id(&tx0)]);

        assert!(pool.build(10, &BTreeSet::new()).is_empty());

        let tx1 = make_ordered_tx(sender, 1, 100);
        pool.add(tx1.clone()).unwrap();

        let txs = pool.build(10, &BTreeSet::new());
        assert_eq!(txs.len(), 2);
        assert_eq!(tx_nonce(&txs[0]), tx1.nonce);
        assert_eq!(tx_nonce(&txs[1]), tx2.nonce);
    }

    #[test]
    fn pool_build_preserves_sender_nonce_order_under_fee_pressure() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender_a = random_address();
        let sender_b = random_address();
        let a0 = make_ordered_tx(sender_a, 0, 10);
        let a1 = make_ordered_tx(sender_a, 1, 1_000);
        let b0 = make_ordered_tx(sender_b, 0, 500);

        pool.add(a0).unwrap();
        pool.add(a1).unwrap();
        pool.add(b0).unwrap();

        let txs = pool.build(10, &BTreeSet::new());
        let order: Vec<_> = txs.iter().map(tx_nonce_and_gas_price).collect();

        assert_eq!(order, vec![(0, 500), (0, 10), (1, 1_000)]);
    }

    #[test]
    fn pool_has_nonce_returns_true_for_pending_tx() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();
        pool.add(make_ordered_tx(sender, 1, 100)).unwrap();

        assert!(pool.has_nonce(&sender, 0));
        assert!(pool.has_nonce(&sender, 1));
        assert!(!pool.has_nonce(&sender, 2));
    }

    #[test]
    fn pool_has_nonce_returns_true_for_queued_tx() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();
        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();
        // nonce 2 is queued (gap at nonce 1)
        pool.add(make_ordered_tx(sender, 2, 100)).unwrap();

        assert!(pool.has_nonce(&sender, 0));
        assert!(!pool.has_nonce(&sender, 1));
        assert!(pool.has_nonce(&sender, 2));
    }

    #[test]
    fn pool_has_nonce_returns_false_for_unknown_sender() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = random_address();

        assert!(!pool.has_nonce(&sender, 0));
    }
}
