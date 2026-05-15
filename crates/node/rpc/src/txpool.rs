//! Transaction pool JSON-RPC namespace.

use std::collections::HashMap;

use alloy_consensus::{Transaction as _, TxEnvelope};
use alloy_primitives::{Address, U64, U256};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use kora_txpool::{OrderedTransaction, TransactionPool};
use serde::{Deserialize, Serialize};

use crate::types::RpcTransaction;

/// Transaction pool JSON-RPC API.
#[rpc(server, namespace = "txpool")]
pub trait TxpoolApi {
    /// Returns all pending and queued transactions grouped by sender and nonce.
    #[method(name = "content")]
    async fn content(&self) -> RpcResult<TxpoolContent>;

    /// Returns the count of pending and queued transactions.
    #[method(name = "status")]
    async fn status(&self) -> RpcResult<TxpoolStatus>;

    /// Returns a compact text summary of pending and queued transactions.
    #[method(name = "inspect")]
    async fn inspect(&self) -> RpcResult<TxpoolInspect>;
}

/// Full transaction pool contents grouped by sender and nonce.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TxpoolContent {
    /// Pending executable transactions.
    pub pending: HashMap<Address, HashMap<String, RpcTransaction>>,
    /// Queued future-nonce transactions.
    pub queued: HashMap<Address, HashMap<String, RpcTransaction>>,
}

/// Transaction pool counts.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct TxpoolStatus {
    /// Pending executable transaction count.
    pub pending: U64,
    /// Queued future-nonce transaction count.
    pub queued: U64,
}

/// Compact transaction pool inspection grouped by sender and nonce.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TxpoolInspect {
    /// Pending executable transaction summaries.
    pub pending: HashMap<Address, HashMap<String, String>>,
    /// Queued future-nonce transaction summaries.
    pub queued: HashMap<Address, HashMap<String, String>>,
}

/// Transaction pool API implementation.
#[derive(Clone, Debug)]
pub struct TxpoolApiImpl {
    pool: TransactionPool,
}

impl TxpoolApiImpl {
    /// Creates a new txpool API implementation.
    pub const fn new(pool: TransactionPool) -> Self {
        Self { pool }
    }
}

#[jsonrpsee::core::async_trait]
impl TxpoolApiServer for TxpoolApiImpl {
    async fn content(&self) -> RpcResult<TxpoolContent> {
        let snapshot = self.pool.snapshot();
        let mut pending = HashMap::new();
        let mut queued = HashMap::new();

        for (sender, (sender_pending, sender_queued)) in snapshot {
            if !sender_pending.is_empty() {
                pending.insert(
                    sender,
                    sender_pending
                        .iter()
                        .map(|tx| (nonce_key(tx.nonce), ordered_tx_to_rpc(tx)))
                        .collect(),
                );
            }
            if !sender_queued.is_empty() {
                queued.insert(
                    sender,
                    sender_queued
                        .iter()
                        .map(|tx| (nonce_key(tx.nonce), ordered_tx_to_rpc(tx)))
                        .collect(),
                );
            }
        }

        Ok(TxpoolContent { pending, queued })
    }

    async fn status(&self) -> RpcResult<TxpoolStatus> {
        Ok(TxpoolStatus {
            pending: U64::from(self.pool.pending_count() as u64),
            queued: U64::from(self.pool.queued_count() as u64),
        })
    }

    async fn inspect(&self) -> RpcResult<TxpoolInspect> {
        let snapshot = self.pool.snapshot();
        let mut pending = HashMap::new();
        let mut queued = HashMap::new();

        for (sender, (sender_pending, sender_queued)) in snapshot {
            if !sender_pending.is_empty() {
                pending.insert(
                    sender,
                    sender_pending.iter().map(|tx| (nonce_key(tx.nonce), inspect_tx(tx))).collect(),
                );
            }
            if !sender_queued.is_empty() {
                queued.insert(
                    sender,
                    sender_queued.iter().map(|tx| (nonce_key(tx.nonce), inspect_tx(tx))).collect(),
                );
            }
        }

        Ok(TxpoolInspect { pending, queued })
    }
}

fn nonce_key(nonce: u64) -> String {
    format!("{nonce:#x}")
}

fn ordered_tx_to_rpc(tx: &OrderedTransaction) -> RpcTransaction {
    let envelope = &tx.envelope;
    let signature = envelope.signature();

    RpcTransaction {
        hash: tx.hash,
        nonce: U64::from(tx.nonce),
        block_hash: None,
        block_number: None,
        transaction_index: None,
        from: tx.sender,
        to: envelope.to(),
        value: envelope.value(),
        gas: U64::from(envelope.gas_limit()),
        gas_price: U256::from(tx.effective_gas_price),
        input: envelope.input().clone(),
        tx_type: U64::from(transaction_type(envelope)),
        chain_id: envelope.chain_id().map(U64::from),
        max_fee_per_gas: max_fee_per_gas(envelope).map(U256::from),
        max_priority_fee_per_gas: max_priority_fee_per_gas(envelope).map(U256::from),
        v: U64::from(u64::from(signature.v())),
        r: signature.r(),
        s: signature.s(),
    }
}

fn inspect_tx(tx: &OrderedTransaction) -> String {
    let to = tx
        .envelope
        .to()
        .map_or_else(|| "contract creation".to_string(), |address| address.to_string());
    format!(
        "{}: {} wei + {} gas x {} wei",
        to,
        tx.envelope.value(),
        tx.envelope.gas_limit(),
        tx.effective_gas_price
    )
}

const fn transaction_type(envelope: &TxEnvelope) -> u64 {
    match envelope {
        TxEnvelope::Legacy(_) => 0,
        TxEnvelope::Eip2930(_) => 1,
        TxEnvelope::Eip1559(_) => 2,
        TxEnvelope::Eip4844(_) => 3,
        TxEnvelope::Eip7702(_) => 4,
    }
}

const fn max_fee_per_gas(envelope: &TxEnvelope) -> Option<u128> {
    match envelope {
        TxEnvelope::Legacy(_) | TxEnvelope::Eip2930(_) => None,
        TxEnvelope::Eip1559(tx) => Some(tx.tx().max_fee_per_gas),
        TxEnvelope::Eip4844(tx) => Some(tx.tx().tx().max_fee_per_gas),
        TxEnvelope::Eip7702(tx) => Some(tx.tx().max_fee_per_gas),
    }
}

const fn max_priority_fee_per_gas(envelope: &TxEnvelope) -> Option<u128> {
    match envelope {
        TxEnvelope::Legacy(_) | TxEnvelope::Eip2930(_) => None,
        TxEnvelope::Eip1559(tx) => Some(tx.tx().max_priority_fee_per_gas),
        TxEnvelope::Eip4844(tx) => Some(tx.tx().tx().max_priority_fee_per_gas),
        TxEnvelope::Eip7702(tx) => Some(tx.tx().max_priority_fee_per_gas),
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction as _, TxEip1559};
    use alloy_primitives::{B256, Bytes, Signature, TxKind};
    use kora_txpool::PoolConfig;

    use super::*;

    fn make_ordered_tx(sender: Address, nonce: u64, gas_price: u128) -> OrderedTransaction {
        let inner = TxEip1559 {
            chain_id: 1,
            nonce,
            gas_limit: 21_000,
            max_fee_per_gas: gas_price,
            max_priority_fee_per_gas: gas_price,
            to: TxKind::Call(Address::repeat_byte(0xbb)),
            value: U256::from(1),
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let sig = Signature::from_scalars_and_parity(B256::ZERO, B256::ZERO, false);
        let signed = inner.into_signed(sig);
        let envelope = TxEnvelope::from(signed);
        let mut hash = [0u8; 32];
        hash[..20].copy_from_slice(sender.as_slice());
        hash[20..28].copy_from_slice(&nonce.to_be_bytes());
        hash[28..].copy_from_slice(&(gas_price as u32).to_be_bytes());
        let hash = B256::from(hash);
        OrderedTransaction::new(hash, sender, nonce, gas_price, 0, envelope)
    }

    #[tokio::test]
    async fn txpool_status_returns_counts() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = Address::repeat_byte(0x11);

        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();
        pool.add(make_ordered_tx(sender, 2, 100)).unwrap();

        let api = TxpoolApiImpl::new(pool);
        let status = TxpoolApiServer::status(&api).await.unwrap();

        assert_eq!(status.pending, U64::from(1));
        assert_eq!(status.queued, U64::from(1));
    }

    #[tokio::test]
    async fn txpool_content_groups_by_sender_and_nonce() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender_a = Address::repeat_byte(0x11);
        let sender_b = Address::repeat_byte(0x22);

        pool.add(make_ordered_tx(sender_a, 0, 100)).unwrap();
        pool.add(make_ordered_tx(sender_a, 1, 100)).unwrap();
        pool.add(make_ordered_tx(sender_b, 0, 200)).unwrap();

        let api = TxpoolApiImpl::new(pool);
        let content = TxpoolApiServer::content(&api).await.unwrap();

        assert!(content.pending.contains_key(&sender_a));
        assert!(content.pending.contains_key(&sender_b));
        assert_eq!(content.pending[&sender_a].len(), 2);
        assert!(content.pending[&sender_a].contains_key("0x0"));
        assert!(content.pending[&sender_a].contains_key("0x1"));
        assert_eq!(content.pending[&sender_b].len(), 1);
    }

    #[tokio::test]
    async fn txpool_inspect_summarizes_transactions() {
        let pool = TransactionPool::new(PoolConfig::default());
        let sender = Address::repeat_byte(0x11);

        pool.add(make_ordered_tx(sender, 0, 100)).unwrap();

        let api = TxpoolApiImpl::new(pool);
        let inspect = TxpoolApiServer::inspect(&api).await.unwrap();
        let summary = &inspect.pending[&sender]["0x0"];

        assert!(summary.contains("21000 gas x 100 wei"));
    }
}
