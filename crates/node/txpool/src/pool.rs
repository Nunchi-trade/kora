//! Transaction pool implementation.

use std::{
    collections::{BTreeSet, HashMap},
    time::{SystemTime, UNIX_EPOCH},
};

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::{Address, B256, Bytes, U256};
use kora_domain::{MempoolEvent, Tx, TxId};
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

            if excluded.contains(&TxId(tx.hash)) {
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
    by_sender: HashMap<Address, SenderQueue>,
    pending_count: usize,
    queued_count: usize,
}

impl PoolInner {
    fn new() -> Self {
        Self {
            by_hash: HashMap::new(),
            by_sender: HashMap::new(),
            pending_count: 0,
            queued_count: 0,
        }
    }

    fn update_counts(&mut self) {
        self.pending_count = self.by_sender.values().map(|q| q.pending_count()).sum();
        self.queued_count = self.by_sender.values().map(|q| q.queued_count()).sum();
    }
}

/// A thread-safe transaction pool with nonce ordering and fee prioritization.
#[derive(Debug)]
pub struct TransactionPool {
    inner: RwLock<PoolInner>,
    config: PoolConfig,
    events: Option<broadcast::Sender<MempoolEvent>>,
}

impl TransactionPool {
    /// Creates a new transaction pool with the given configuration.
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self { inner: RwLock::new(PoolInner::new()), config, events: None }
    }

    /// Creates a new transaction pool that broadcasts mempool lifecycle events.
    #[must_use]
    pub fn new_with_events(config: PoolConfig, events: broadcast::Sender<MempoolEvent>) -> Self {
        Self { inner: RwLock::new(PoolInner::new()), config, events: Some(events) }
    }

    /// Adds a validated transaction to the pool.
    pub fn add(&self, tx: OrderedTransaction) -> Result<(), TxPoolError> {
        let added_event = tx_added_event(&tx);
        let mut replaced_hash = None;

        {
            let mut inner = self.inner.write();

            if inner.by_hash.contains_key(&tx.hash) {
                return Err(TxPoolError::AlreadyExists);
            }

            let sender = tx.sender;
            let queue =
                inner.by_sender.entry(sender).or_insert_with(|| SenderQueue::new(sender, tx.nonce));

            if queue.total_count() >= self.config.max_txs_per_sender {
                return Err(TxPoolError::SenderFull(sender));
            }

            if let Some(replaced) = queue.insert(tx.clone()) {
                if replaced.hash == tx.hash {
                    return Err(TxPoolError::AlreadyExists);
                }
                inner.by_hash.remove(&replaced.hash);
                replaced_hash = Some(replaced.hash);
                debug!(hash = ?replaced.hash, "replaced transaction");
            }

            inner.by_hash.insert(tx.hash, tx);
            inner.update_counts();

            if inner.pending_count > self.config.max_pending_txs {
                warn!(
                    count = inner.pending_count,
                    max = self.config.max_pending_txs,
                    "pool exceeds pending limit"
                );
            }

            if inner.queued_count > self.config.max_queued_txs {
                warn!(
                    count = inner.queued_count,
                    max = self.config.max_queued_txs,
                    "pool exceeds queued limit"
                );
            }
        }

        if let Some(events) = &self.events {
            if let Some(hash) = replaced_hash {
                let _ =
                    events.send(MempoolEvent::TxEvicted { hash, reason: "replaced".to_string() });
            }
            let _ = events.send(added_event);
        }

        Ok(())
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

    /// Gets a transaction by its hash.
    pub fn get(&self, hash: &B256) -> Option<OrderedTransaction> {
        self.inner.read().by_hash.get(hash).cloned()
    }

    /// Removes a transaction by its hash, emitting a `TxEvicted` event with the
    /// provided `reason`.
    pub fn remove_with_reason(&self, hash: &B256, reason: &str) -> Option<OrderedTransaction> {
        let tx = {
            let mut inner = self.inner.write();

            let tx = inner.by_hash.remove(hash)?;
            let sender = tx.sender;

            if let Some(queue) = inner.by_sender.get_mut(&sender) {
                queue.pending.retain(|t| t.hash != *hash);
                queue.queued.retain(|t| t.hash != *hash);

                if queue.is_empty() {
                    inner.by_sender.remove(&sender);
                }
            }

            inner.update_counts();
            tx
        };

        if let Some(events) = &self.events {
            let _ =
                events.send(MempoolEvent::TxEvicted { hash: *hash, reason: reason.to_string() });
        }

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
            inner.by_hash.remove(&hash);
        }

        if let Some(queue) = inner.by_sender.get_mut(sender) {
            queue.remove_confirmed(confirmed_nonce);
            if queue.is_empty() {
                inner.by_sender.remove(sender);
            }
        }

        inner.update_counts();
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

    /// Removes all transactions from the pool.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.by_hash.clear();
        inner.by_sender.clear();
        inner.pending_count = 0;
        inner.queued_count = 0;
    }
}

impl Clone for TransactionPool {
    fn clone(&self) -> Self {
        let inner = self.inner.read();
        Self {
            inner: RwLock::new(PoolInner {
                by_hash: inner.by_hash.clone(),
                by_sender: inner.by_sender.clone(),
                pending_count: inner.pending_count,
                queued_count: inner.queued_count,
            }),
            config: self.config.clone(),
            events: self.events.clone(),
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
            return false;
        };

        match self.add(ordered) {
            Ok(()) => true,
            Err(e) => {
                trace!(?e, "failed to insert transaction");
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
                let mut raw = Vec::new();
                tx.envelope.encode_2718(&mut raw);
                result.push(Tx::new(Bytes::from(raw)));
            }
        }

        result
    }

    fn prune(&self, tx_ids: &[TxId]) {
        let mut inner = self.inner.write();

        let mut confirmed_by_sender: HashMap<Address, u64> = HashMap::new();
        for id in tx_ids {
            if let Some(tx) = inner.by_hash.get(&id.0) {
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
            inner.by_hash.remove(&hash);
        }

        for sender in senders_to_check {
            if let Some(queue) = inner.by_sender.get(&sender)
                && queue.is_empty()
            {
                inner.by_sender.remove(&sender);
            }
        }

        inner.update_counts();
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

        pool.prune(&[TxId(tx0.hash), TxId(tx1.hash)]);

        let txs = pool.build(10, &BTreeSet::new());
        assert_eq!(txs.len(), 2);
        assert_eq!(tx_nonce(&txs[0]), tx2.nonce);
        assert_eq!(tx_nonce(&txs[1]), tx3.nonce);
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

        let excluded = BTreeSet::from([TxId(tx0.hash)]);
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
        let b0 = make_ordered_tx(sender_b, 0, 100);
        let b1 = make_ordered_tx(sender_b, 1, 100);

        for tx in [&a0, &a1, &a2, &a3, &b0, &b1] {
            pool.add(tx.clone()).unwrap();
        }

        pool.prune(&[TxId(a1.hash), TxId(b0.hash)]);

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
        pool.prune(&[TxId(tx0.hash)]);

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
}
