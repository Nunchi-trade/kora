//! In-memory mempool implementation.

use std::{collections::BTreeMap, sync::Arc};

use alloy_consensus::{Transaction as _, TxEnvelope, transaction::SignerRecoverable as _};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::Address;
use kora_domain::Tx;
use parking_lot::RwLock;

use crate::traits::{Mempool, TxId};

/// Simple in-memory mempool backed by a BTreeMap.
#[derive(Debug, Clone)]
pub struct InMemoryMempool {
    inner: Arc<RwLock<BTreeMap<TxId, Tx>>>,
}

impl InMemoryMempool {
    /// Create a new empty mempool.
    #[must_use]
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(BTreeMap::new())) }
    }
}

impl Default for InMemoryMempool {
    fn default() -> Self {
        Self::new()
    }
}

fn tx_order_key(tx: &Tx) -> (u8, Address, u64) {
    let Ok(envelope) = TxEnvelope::decode_2718_exact(tx.bytes.as_ref()) else {
        return (1, Address::ZERO, u64::MAX);
    };
    let Ok(sender) = envelope.recover_signer() else {
        return (1, Address::ZERO, u64::MAX);
    };
    (0, sender, envelope.nonce())
}

impl Mempool for InMemoryMempool {
    fn insert(&self, tx: Tx) -> bool {
        let id = tx.id();
        let mut inner = self.inner.write();
        inner.insert(id, tx).is_none()
    }

    fn build(&self, max_txs: usize, excluded: &std::collections::BTreeSet<TxId>) -> Vec<Tx> {
        let inner = self.inner.read();
        let mut candidates: Vec<_> = inner
            .iter()
            .filter(|(id, _)| !excluded.contains(id))
            .map(|(id, tx)| (tx_order_key(tx), *id, tx.clone()))
            .collect();
        candidates.sort_by_key(|(order, id, _)| (*order, *id));
        candidates.into_iter().take(max_txs).map(|(_, _, tx)| tx).collect()
    }

    fn prune(&self, tx_ids: &[TxId]) {
        let mut inner = self.inner.write();
        for id in tx_ids {
            inner.remove(id);
        }
    }

    fn len(&self) -> usize {
        self.inner.read().len()
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Transaction as _, TxEnvelope, transaction::SignerRecoverable as _};
    use alloy_eips::eip2718::Decodable2718 as _;
    use alloy_primitives::{Address, U256};
    use k256::ecdsa::SigningKey;
    use kora_domain::evm::Evm;

    use super::*;

    fn signing_key_from_seed(seed: u8) -> SigningKey {
        let mut secret = [0u8; 32];
        secret[31] = seed;
        SigningKey::from_bytes((&secret).into()).expect("valid key")
    }

    fn signed_transfer(sender_seed: u8, recipient_seed: u8, nonce: u64, value: u64) -> Tx {
        let sender_key = signing_key_from_seed(sender_seed);
        let recipient_key = signing_key_from_seed(recipient_seed);
        let recipient = Evm::address_from_key(&recipient_key);
        Evm::sign_eip1559_transfer(
            &sender_key,
            1,
            recipient,
            U256::from(value),
            nonce,
            21_000,
            0,
            0,
        )
    }

    fn signed_order_key(tx: &Tx) -> (Address, u64, TxId) {
        let mut data = tx.bytes.as_ref();
        let envelope = TxEnvelope::decode_2718(&mut data).expect("signed tx");
        let sender = envelope.recover_signer().expect("recover signer");
        (sender, envelope.nonce(), tx.id())
    }

    #[test]
    fn mempool_insert_and_build() {
        let mempool = InMemoryMempool::new();

        let tx1 = Tx::new(vec![1, 2, 3].into());
        let tx2 = Tx::new(vec![4, 5, 6].into());

        assert!(mempool.insert(tx1.clone()));
        assert!(mempool.insert(tx2));
        assert!(!mempool.insert(tx1)); // Duplicate

        assert_eq!(mempool.len(), 2);

        let txs = mempool.build(10, &std::collections::BTreeSet::new());
        assert_eq!(txs.len(), 2);
    }

    #[test]
    fn mempool_prune() {
        let mempool = InMemoryMempool::new();

        let tx = Tx::new(vec![1, 2, 3].into());
        let id = tx.id();

        mempool.insert(tx);
        assert_eq!(mempool.len(), 1);

        mempool.prune(&[id]);
        assert_eq!(mempool.len(), 0);
    }

    #[test]
    fn mempool_build_with_exclusions() {
        let mempool = InMemoryMempool::new();

        let tx1 = Tx::new(vec![1, 2, 3].into());
        let tx2 = Tx::new(vec![4, 5, 6].into());
        let id1 = tx1.id();

        mempool.insert(tx1);
        mempool.insert(tx2.clone());

        let mut excluded = std::collections::BTreeSet::new();
        excluded.insert(id1);

        let txs = mempool.build(10, &excluded);
        assert_eq!(txs.len(), 1);
        assert_eq!(txs[0], tx2);
    }

    #[test]
    fn mempool_build_orders_signed_txs_by_sender_nonce_and_id() {
        let mempool = InMemoryMempool::new();
        let txs = vec![
            signed_transfer(2, 9, 1, 10),
            signed_transfer(1, 9, 0, 20),
            signed_transfer(2, 8, 0, 30),
            signed_transfer(1, 8, 0, 40),
            signed_transfer(1, 7, 1, 50),
            signed_transfer(2, 7, 0, 60),
        ];

        for tx in txs.iter().rev() {
            assert!(mempool.insert(tx.clone()));
        }

        let mut expected = txs;
        expected.sort_by_key(signed_order_key);

        let built = mempool.build(10, &std::collections::BTreeSet::new());
        let built_ids: Vec<_> = built.iter().map(Tx::id).collect();
        let expected_ids: Vec<_> = expected.iter().map(Tx::id).collect();

        assert_eq!(built_ids, expected_ids);
    }
}
