//! Indexed state provider for production RPC use.
//!
//! Integrates the block indexer with ledger state to provide a complete
//! [`StateProvider`] implementation for RPC queries.

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes, U64, U256};
use async_trait::async_trait;
use kora_executor::{BlockContext, CallParams, RevmExecutor};
use kora_indexer::{BlockIndex, IndexedBlock, IndexedReceipt, IndexedTransaction, LogFilter};
use kora_traits::{StateDbError, StateDbRead};

use crate::{
    error::RpcError,
    state_provider::StateProvider,
    types::{
        BlockNumberOrTag, BlockTag, BlockTransactions, CallRequest, EMPTY_UNCLE_HASH,
        EMPTY_WITHDRAWALS_ROOT, RpcBlock, RpcLog, RpcLogFilter, RpcTransaction,
        RpcTransactionReceipt,
    },
};

/// Maximum block range allowed for a single `eth_getLogs` query.
///
/// Ranges exceeding this limit are rejected with an invalid-params error to
/// prevent unbounded iteration from monopolising the RPC thread. The value is
/// aligned with Infura's 10 000-block cap.
/// Maximum block range allowed for a single log query.
///
/// Exposed so that `eth_getFilterChanges` can pre-cap the range before
/// delegating to `get_logs`, avoiding confusing downstream errors.
pub const MAX_LOG_BLOCK_RANGE: u64 = 10_000;

/// State provider that combines indexed block data with live state queries.
///
/// Uses [`BlockIndex`] for block, transaction, and receipt lookups, delegates
/// account state queries (balance, nonce, code, storage) to a generic state
/// database implementation, and uses a [`RevmExecutor`] to serve `eth_call`
/// and `eth_estimateGas` against the live state.
#[derive(Debug)]
pub struct IndexedStateProvider<S> {
    index: Arc<BlockIndex>,
    state: S,
    executor: Arc<RevmExecutor>,
    fee_recipient: Address,
}

impl<S> IndexedStateProvider<S> {
    /// Creates a new indexed state provider with an explicit executor.
    #[must_use]
    pub const fn new(
        index: Arc<BlockIndex>,
        state: S,
        executor: Arc<RevmExecutor>,
        fee_recipient: Address,
    ) -> Self {
        Self { index, state, executor, fee_recipient }
    }

    /// Creates a new indexed state provider with a default executor for the
    /// given chain id.
    #[must_use]
    pub fn with_chain_id(index: Arc<BlockIndex>, state: S, chain_id: u64) -> Self {
        Self::new(index, state, Arc::new(RevmExecutor::new(chain_id)), Address::ZERO)
    }
}

impl<S: Clone> Clone for IndexedStateProvider<S> {
    fn clone(&self) -> Self {
        Self {
            index: Arc::clone(&self.index),
            state: self.state.clone(),
            executor: Arc::clone(&self.executor),
            fee_recipient: self.fee_recipient,
        }
    }
}

#[async_trait]
impl<S: StateDbRead + Send + Sync + 'static> StateProvider for IndexedStateProvider<S> {
    async fn balance(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> Result<U256, RpcError> {
        self.reject_historical_block(&block)?;
        match self.state.balance(&address).await {
            Ok(balance) => Ok(balance),
            Err(StateDbError::AccountNotFound(_)) => Ok(U256::ZERO),
            Err(e) => Err(state_error_to_rpc(e)),
        }
    }

    async fn nonce(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> Result<u64, RpcError> {
        self.reject_historical_block(&block)?;
        match self.state.nonce(&address).await {
            Ok(nonce) => Ok(nonce),
            Err(StateDbError::AccountNotFound(_)) => Ok(0),
            Err(e) => Err(state_error_to_rpc(e)),
        }
    }

    async fn code(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> Result<Bytes, RpcError> {
        self.reject_historical_block(&block)?;
        // EIP-1474: `eth_getCode` MUST return `0x` for unknown accounts and
        // for EOAs without code, NOT an error. Many tools branch on
        // `getCode === '0x'` to decide "is this a contract?".
        let code_hash = match self.state.code_hash(&address).await {
            Ok(hash) => hash,
            Err(StateDbError::AccountNotFound(_)) => return Ok(Bytes::new()),
            Err(e) => return Err(state_error_to_rpc(e)),
        };
        if code_hash == B256::ZERO || code_hash == alloy_primitives::KECCAK256_EMPTY {
            return Ok(Bytes::new());
        }
        match self.state.code(&code_hash).await {
            Ok(bytes) => Ok(bytes),
            Err(StateDbError::CodeNotFound(_)) => Ok(Bytes::new()),
            Err(e) => Err(state_error_to_rpc(e)),
        }
    }

    async fn storage(
        &self,
        address: Address,
        slot: U256,
        block: Option<BlockNumberOrTag>,
    ) -> Result<U256, RpcError> {
        self.reject_historical_block(&block)?;
        match self.state.storage(&address, &slot).await {
            Ok(value) => Ok(value),
            Err(StateDbError::AccountNotFound(_)) => Ok(U256::ZERO),
            Err(e) => Err(state_error_to_rpc(e)),
        }
    }

    async fn block_by_number(
        &self,
        block: BlockNumberOrTag,
        full_transactions: bool,
    ) -> Result<Option<RpcBlock>, RpcError> {
        let block_num = self.resolve_block_number(&block)?;
        let indexed = self.index.get_block_by_number(block_num);
        Ok(indexed.map(|block| self.indexed_block_to_rpc(block, full_transactions)))
    }

    async fn block_by_hash(
        &self,
        hash: B256,
        full_transactions: bool,
    ) -> Result<Option<RpcBlock>, RpcError> {
        let indexed = self.index.get_block_by_hash(&hash);
        Ok(indexed.map(|block| self.indexed_block_to_rpc(block, full_transactions)))
    }

    async fn transaction_by_hash(&self, hash: B256) -> Result<Option<RpcTransaction>, RpcError> {
        let indexed = self.index.get_transaction(&hash);
        Ok(indexed.map(indexed_tx_to_rpc))
    }

    async fn receipt_by_hash(&self, hash: B256) -> Result<Option<RpcTransactionReceipt>, RpcError> {
        let indexed = self.index.get_receipt(&hash);
        Ok(indexed.map(indexed_receipt_to_rpc))
    }

    async fn block_number(&self) -> Result<u64, RpcError> {
        Ok(self.index.head_block_number())
    }

    async fn call(
        &self,
        request: CallRequest,
        block: Option<BlockNumberOrTag>,
    ) -> Result<Bytes, RpcError> {
        self.reject_historical_block(&block)?;
        let block_ctx = self.block_context_for(block)?;
        let params = call_request_to_params(request);
        self.executor.simulate_call(&self.state, params, &block_ctx).map_err(execution_error_to_rpc)
    }

    async fn estimate_gas(
        &self,
        request: CallRequest,
        block: Option<BlockNumberOrTag>,
    ) -> Result<u64, RpcError> {
        self.reject_historical_block(&block)?;
        let block_ctx = self.block_context_for(block)?;
        let params = call_request_to_params(request);
        self.executor.estimate_gas(&self.state, params, &block_ctx).map_err(execution_error_to_rpc)
    }

    async fn receipts_by_block_hash(
        &self,
        block_hash: B256,
    ) -> Result<Vec<RpcTransactionReceipt>, RpcError> {
        Ok(self
            .index
            .get_receipts_by_block_hash(&block_hash)
            .into_iter()
            .map(indexed_receipt_to_rpc)
            .collect())
    }

    async fn get_logs(&self, filter: RpcLogFilter) -> Result<Vec<RpcLog>, RpcError> {
        // EIP-234: blockHash is mutually exclusive with fromBlock/toBlock.
        if filter.block_hash.is_some() && (filter.from_block.is_some() || filter.to_block.is_some())
        {
            return Err(RpcError::InvalidParams(
                "blockHash is mutually exclusive with fromBlock/toBlock".into(),
            ));
        }

        let mut log_filter = LogFilter::new();

        if let Some(block_hash) = &filter.block_hash {
            // Single-block query by hash per EIP-234.
            let block = self
                .index
                .get_block_by_hash(block_hash)
                .ok_or_else(|| RpcError::InvalidParams("block not found".into()))?;
            log_filter = log_filter.from_block(block.number).to_block(block.number);
        } else {
            let head = self.index.head_block_number();
            let from_block =
                filter.from_block.as_ref().map(|b| self.resolve_block_number(b)).transpose()?;
            let to_block =
                filter.to_block.as_ref().map(|b| self.resolve_block_number(b)).transpose()?;

            let from = from_block.unwrap_or(0);
            let to = to_block.unwrap_or(head).min(head);

            if from > to {
                return Err(RpcError::InvalidParams(
                    "fromBlock must not be greater than toBlock".into(),
                ));
            }
            if to.saturating_sub(from) > MAX_LOG_BLOCK_RANGE {
                return Err(RpcError::InvalidParams(format!(
                    "block range exceeds maximum of {MAX_LOG_BLOCK_RANGE}"
                )));
            }

            log_filter = log_filter.from_block(from).to_block(to);
        }

        if let Some(addr_filter) = filter.address {
            log_filter = log_filter.address(addr_filter.into_vec());
        }
        if let Some(topics) = filter.topics {
            for (i, topic_filter) in topics.into_iter().enumerate() {
                if let Some(tf) = topic_filter {
                    log_filter = log_filter.topic(i, tf.into_vec());
                }
            }
        }

        let logs = self
            .index
            .get_logs(&log_filter)
            .map_err(|e| RpcError::InvalidParams(e.to_string()))?
            .into_iter()
            .map(|log| RpcLog {
                address: log.address,
                topics: log.topics,
                data: log.data,
                block_number: U64::from(log.block_number),
                transaction_hash: log.transaction_hash,
                transaction_index: U64::from(log.transaction_index),
                block_hash: log.block_hash,
                log_index: U64::from(log.log_index),
                removed: false,
            })
            .collect();
        Ok(logs)
    }
}

impl<S> IndexedStateProvider<S> {
    /// Reject requests for historical or future state that we cannot serve.
    ///
    /// Kora uses QMDB which only maintains the latest state. We accept
    /// `None`, `latest`, `pending`, `safe`, `finalized`, and the current
    /// head block number; everything else returns an explicit error instead
    /// of silently returning the latest state.
    ///
    /// In Simplex BFT all committed blocks are immediately finalized, so
    /// `safe` and `finalized` are semantically equivalent to `latest`.
    fn reject_historical_block(&self, block: &Option<BlockNumberOrTag>) -> Result<(), RpcError> {
        match block {
            None
            | Some(BlockNumberOrTag::Latest)
            | Some(BlockNumberOrTag::Tag(
                BlockTag::Latest | BlockTag::Pending | BlockTag::Safe | BlockTag::Finalized,
            )) => Ok(()),
            Some(BlockNumberOrTag::Number(n)) => {
                let head = self.index.head_block_number();
                let requested = n.to::<u64>();
                if requested <= head {
                    // Kora only stores latest state (QMDB), so we serve
                    // current state for any known block height.  This avoids
                    // a race where the head advances between the client's
                    // `eth_blockNumber` call and the subsequent state query.
                    Ok(())
                } else {
                    Err(RpcError::InvalidBlockNumber(format!(
                        "block not yet available (requested {requested}, head {head})",
                    )))
                }
            }
            Some(BlockNumberOrTag::Tag(tag)) => {
                Err(RpcError::Unsupported(format!("historical state not available (tag {tag:?})",)))
            }
        }
    }

    fn indexed_block_to_rpc(&self, block: IndexedBlock, full_transactions: bool) -> RpcBlock {
        let transactions = if full_transactions {
            let txs = self
                .index
                .get_transactions_for_block(&block.hash)
                .into_iter()
                .map(indexed_tx_to_rpc)
                .collect();
            BlockTransactions::Full(txs)
        } else {
            BlockTransactions::Hashes(block.transaction_hashes.clone())
        };

        RpcBlock {
            hash: block.hash,
            parent_hash: block.parent_hash,
            sha3_uncles: EMPTY_UNCLE_HASH,
            number: U64::from(block.number),
            state_root: block.state_root,
            transactions_root: block.transactions_root,
            receipts_root: block.receipts_root,
            logs_bloom: Bytes::copy_from_slice(block.logs_bloom.as_slice()),
            timestamp: U64::from(block.timestamp),
            gas_limit: U64::from(block.gas_limit),
            gas_used: U64::from(block.gas_used),
            extra_data: Bytes::new(),
            mix_hash: block.mix_hash,
            nonce: Default::default(),
            base_fee_per_gas: block.base_fee_per_gas.map(U256::from),
            miner: self.fee_recipient,
            difficulty: U256::ZERO,
            total_difficulty: U256::ZERO,
            uncles: vec![],
            size: U64::from(block.size),
            transactions,
            withdrawals: vec![],
            withdrawals_root: EMPTY_WITHDRAWALS_ROOT,
        }
    }

    fn resolve_block_number(&self, block: &BlockNumberOrTag) -> Result<u64, RpcError> {
        match block {
            BlockNumberOrTag::Number(n) => Ok(n.to::<u64>()),
            BlockNumberOrTag::Tag(tag) => self.resolve_tag(*tag),
            BlockNumberOrTag::Latest => Ok(self.index.head_block_number()),
        }
    }

    fn resolve_tag(&self, tag: BlockTag) -> Result<u64, RpcError> {
        match tag {
            BlockTag::Latest | BlockTag::Safe | BlockTag::Finalized | BlockTag::Pending => {
                Ok(self.index.head_block_number())
            }
            BlockTag::Earliest => Ok(0),
        }
    }

    /// Build a `BlockContext` for the requested block tag, falling back to a
    /// generous default when no block is indexed yet (so `eth_call` against
    /// a fresh chain still works).
    fn block_context_for(&self, block: Option<BlockNumberOrTag>) -> Result<BlockContext, RpcError> {
        let block_num = match block {
            Some(b) => self.resolve_block_number(&b)?,
            None => self.index.head_block_number(),
        };
        let recent_hashes = self.index.recent_block_hashes(block_num);
        if let Some(indexed) = self.index.get_block_by_number(block_num) {
            let header = Header {
                number: indexed.number,
                timestamp: indexed.timestamp,
                gas_limit: indexed.gas_limit,
                base_fee_per_gas: indexed.base_fee_per_gas,
                ..Header::default()
            };
            Ok(BlockContext::new(header, indexed.parent_hash, B256::ZERO)
                .with_recent_block_hashes(recent_hashes))
        } else {
            let header = Header {
                number: 0,
                timestamp: 0,
                gas_limit: 30_000_000,
                base_fee_per_gas: Some(1_000_000_000),
                ..Header::default()
            };
            Ok(BlockContext::new(header, B256::ZERO, B256::ZERO))
        }
    }
}

/// Convert a JSON-RPC `CallRequest` into executor `CallParams`.
fn call_request_to_params(req: CallRequest) -> CallParams {
    let gas_price: u128 =
        req.gas_price.or(req.max_fee_per_gas).unwrap_or_default().try_into().unwrap_or(u128::MAX);
    CallParams {
        from: req.from.unwrap_or_default(),
        to: req.to,
        value: req.value.unwrap_or_default(),
        data: req.input.or(req.data).unwrap_or_default(),
        gas_limit: req.gas.map(|g| g.to::<u64>()),
        gas_price,
        nonce: req.nonce.map(|n| n.to::<u64>()).unwrap_or(0),
    }
}

/// Map an executor `ExecutionError` into an `RpcError` for the JSON-RPC layer.
fn execution_error_to_rpc(err: kora_executor::ExecutionError) -> RpcError {
    use kora_executor::ExecutionError as E;
    match err {
        E::Revert(data) => RpcError::ExecutionReverted(Some(data)),
        E::TxExecution(msg) | E::InvalidTx(msg) | E::TxDecode(msg) | E::BlockValidation(msg) => {
            RpcError::ExecutionFailed(msg)
        }
        E::State(s) => state_error_to_rpc(s),
        E::CodeNotFound(h) => RpcError::StateError(format!("code not found: {h}")),
        E::StateCommit => {
            RpcError::Internal("QMDB commit failed during block execution".to_string())
        }
    }
}

fn state_error_to_rpc(err: StateDbError) -> RpcError {
    match err {
        StateDbError::AccountNotFound(addr) => RpcError::AccountNotFound(addr.to_string()),
        StateDbError::CodeNotFound(hash) => RpcError::StateError(format!("code not found: {hash}")),
        StateDbError::Storage(msg) => RpcError::StateError(msg),
        StateDbError::LockPoisoned => RpcError::Internal("lock poisoned".to_string()),
        StateDbError::RootComputation(msg) => RpcError::StateError(msg),
    }
}

fn indexed_tx_to_rpc(tx: IndexedTransaction) -> RpcTransaction {
    RpcTransaction {
        hash: tx.hash,
        nonce: U64::from(tx.nonce),
        block_hash: Some(tx.block_hash),
        block_number: Some(U64::from(tx.block_number)),
        transaction_index: Some(U64::from(tx.index)),
        from: tx.from,
        to: tx.to,
        value: tx.value,
        gas: U64::from(tx.gas_limit),
        gas_price: U256::from(tx.gas_price),
        input: tx.input,
        tx_type: U64::from(tx.tx_type),
        chain_id: tx.chain_id.map(U64::from),
        max_fee_per_gas: tx.max_fee_per_gas.map(U256::from),
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas.map(U256::from),
        v: U256::from(tx.v),
        r: tx.r,
        s: tx.s,
    }
}

fn indexed_receipt_to_rpc(receipt: IndexedReceipt) -> RpcTransactionReceipt {
    let logs_bloom = Bytes::copy_from_slice(receipt.logs_bloom.as_slice());
    let logs = receipt
        .logs
        .into_iter()
        .map(|log| RpcLog {
            address: log.address,
            topics: log.topics,
            data: log.data,
            block_number: U64::from(log.block_number),
            transaction_hash: log.transaction_hash,
            transaction_index: U64::from(log.transaction_index),
            block_hash: log.block_hash,
            log_index: U64::from(log.log_index),
            removed: false,
        })
        .collect();

    RpcTransactionReceipt {
        transaction_hash: receipt.transaction_hash,
        transaction_index: U64::from(receipt.transaction_index),
        block_hash: receipt.block_hash,
        block_number: U64::from(receipt.block_number),
        from: receipt.from,
        to: receipt.to,
        cumulative_gas_used: U64::from(receipt.cumulative_gas_used),
        gas_used: U64::from(receipt.gas_used),
        contract_address: receipt.contract_address,
        logs,
        logs_bloom,
        tx_type: U64::from(receipt.tx_type),
        status: if receipt.status { U64::from(1) } else { U64::ZERO },
        effective_gas_price: U256::from(receipt.effective_gas_price),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Bloom;
    use kora_indexer::IndexedLog;

    use super::*;

    #[derive(Clone)]
    struct MockState;

    impl StateDbRead for MockState {
        async fn nonce(&self, _address: &Address) -> Result<u64, StateDbError> {
            Ok(42)
        }

        async fn balance(&self, _address: &Address) -> Result<U256, StateDbError> {
            Ok(U256::from(1000))
        }

        async fn code_hash(&self, _address: &Address) -> Result<B256, StateDbError> {
            Ok(B256::ZERO)
        }

        async fn code(&self, _code_hash: &B256) -> Result<Bytes, StateDbError> {
            Ok(Bytes::from_static(&[0x60, 0x00]))
        }

        async fn storage(&self, _address: &Address, _slot: &U256) -> Result<U256, StateDbError> {
            Ok(U256::from(123))
        }
    }

    #[derive(Clone)]
    struct MissingAccountState;

    impl StateDbRead for MissingAccountState {
        async fn nonce(&self, _address: &Address) -> Result<u64, StateDbError> {
            Err(StateDbError::AccountNotFound(Address::ZERO))
        }

        async fn balance(&self, _address: &Address) -> Result<U256, StateDbError> {
            Err(StateDbError::AccountNotFound(Address::ZERO))
        }

        async fn code_hash(&self, address: &Address) -> Result<B256, StateDbError> {
            Err(StateDbError::AccountNotFound(*address))
        }

        async fn code(&self, _code_hash: &B256) -> Result<Bytes, StateDbError> {
            Err(StateDbError::CodeNotFound(B256::ZERO))
        }

        async fn storage(&self, _address: &Address, _slot: &U256) -> Result<U256, StateDbError> {
            Err(StateDbError::AccountNotFound(Address::ZERO))
        }
    }

    fn create_test_block(number: u64, hash: B256) -> IndexedBlock {
        IndexedBlock {
            hash,
            number,
            parent_hash: B256::ZERO,
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            timestamp: 1000 + number,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            base_fee_per_gas: Some(1_000_000_000),
            mix_hash: B256::ZERO,
            logs_bloom: Bloom::ZERO,
            size: 508,
            transaction_hashes: vec![],
        }
    }

    fn create_test_tx(hash: B256, block_hash: B256, block_number: u64) -> IndexedTransaction {
        IndexedTransaction {
            hash,
            block_hash,
            block_number,
            index: 0,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            value: U256::ZERO,
            gas_limit: 21_000,
            gas_price: 1_000_000_000,
            tx_type: 0,
            chain_id: Some(1337),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            v: 27,
            r: U256::from(1),
            s: U256::from(2),
            input: Bytes::new(),
            nonce: 0,
        }
    }

    fn create_test_receipt(tx_hash: B256, block_hash: B256, block_number: u64) -> IndexedReceipt {
        IndexedReceipt {
            transaction_hash: tx_hash,
            block_hash,
            block_number,
            transaction_index: 0,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            cumulative_gas_used: 21_000,
            gas_used: 21_000,
            contract_address: None,
            logs: vec![IndexedLog {
                address: Address::ZERO,
                topics: vec![],
                data: Bytes::new(),
                log_index: 0,
                block_number,
                block_hash,
                transaction_hash: tx_hash,
                transaction_index: 0,
            }],
            logs_bloom: Bloom::ZERO,
            tx_type: 0,
            effective_gas_price: 1_000_000_000,
            status: true,
        }
    }

    #[test]
    fn indexed_tx_preserves_eip1559_fields() {
        let block_hash = B256::repeat_byte(1);
        let tx_hash = B256::repeat_byte(2);
        let tx = IndexedTransaction {
            hash: tx_hash,
            block_hash,
            block_number: 7,
            index: 3,
            from: Address::repeat_byte(0xaa),
            to: Some(Address::repeat_byte(0xbb)),
            value: U256::from(10),
            gas_limit: 50_000,
            gas_price: 20_000_000_000,
            tx_type: 2,
            chain_id: Some(1337),
            max_fee_per_gas: Some(20_000_000_000),
            max_priority_fee_per_gas: Some(1_500_000_000),
            v: 1,
            r: U256::from(123),
            s: U256::from(456),
            input: Bytes::from_static(&[0xde, 0xad]),
            nonce: 9,
        };

        let rpc_tx = indexed_tx_to_rpc(tx);

        assert_eq!(rpc_tx.hash, tx_hash);
        assert_eq!(rpc_tx.block_hash, Some(block_hash));
        assert_eq!(rpc_tx.transaction_index, Some(U64::from(3)));
        assert_eq!(rpc_tx.tx_type, U64::from(2));
        assert_eq!(rpc_tx.chain_id, Some(U64::from(1337)));
        assert_eq!(rpc_tx.max_fee_per_gas, Some(U256::from(20_000_000_000u64)));
        assert_eq!(rpc_tx.max_priority_fee_per_gas, Some(U256::from(1_500_000_000u64)));
        assert_eq!(rpc_tx.v, U256::from(1));
        assert_eq!(rpc_tx.r, U256::from(123));
        assert_eq!(rpc_tx.s, U256::from(456));
    }

    #[test]
    fn indexed_receipt_preserves_fee_type_bloom_and_log_metadata() {
        let block_hash = B256::repeat_byte(1);
        let tx_hash = B256::repeat_byte(2);
        let receipt = IndexedReceipt {
            transaction_hash: tx_hash,
            block_hash,
            block_number: 5,
            transaction_index: 1,
            from: Address::repeat_byte(0xaa),
            to: Some(Address::repeat_byte(0xbb)),
            cumulative_gas_used: 50_000,
            gas_used: 29_000,
            contract_address: None,
            logs: vec![IndexedLog {
                address: Address::repeat_byte(0xcc),
                topics: vec![B256::repeat_byte(0xdd)],
                data: Bytes::from_static(&[0x01, 0x02]),
                log_index: 4,
                block_number: 5,
                block_hash,
                transaction_hash: tx_hash,
                transaction_index: 1,
            }],
            logs_bloom: Bloom::repeat_byte(0xab),
            tx_type: 2,
            effective_gas_price: 12_000_000_000,
            status: true,
        };

        let rpc_receipt = indexed_receipt_to_rpc(receipt);

        assert_eq!(rpc_receipt.tx_type, U64::from(2));
        assert_eq!(rpc_receipt.effective_gas_price, U256::from(12_000_000_000u64));
        assert_eq!(rpc_receipt.logs_bloom.len(), 256);
        assert_eq!(rpc_receipt.logs_bloom[0], 0xab);
        assert_eq!(rpc_receipt.logs[0].block_number, U64::from(5));
        assert_eq!(rpc_receipt.logs[0].block_hash, block_hash);
        assert_eq!(rpc_receipt.logs[0].transaction_hash, tx_hash);
        assert_eq!(rpc_receipt.logs[0].transaction_index, U64::from(1));
    }

    #[tokio::test]
    async fn test_balance() {
        let index = Arc::new(BlockIndex::new());
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let balance = provider.balance(Address::ZERO, None).await.unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn test_nonce() {
        let index = Arc::new(BlockIndex::new());
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let nonce = provider.nonce(Address::ZERO, None).await.unwrap();
        assert_eq!(nonce, 42);
    }

    #[tokio::test]
    async fn test_missing_account_balance_returns_zero() {
        let index = Arc::new(BlockIndex::new());
        let provider = IndexedStateProvider::with_chain_id(index, MissingAccountState, 1337);

        let balance = provider.balance(Address::repeat_byte(0xaa), None).await.unwrap();
        assert_eq!(balance, U256::ZERO);
    }

    #[tokio::test]
    async fn test_missing_account_nonce_returns_zero() {
        let index = Arc::new(BlockIndex::new());
        let provider = IndexedStateProvider::with_chain_id(index, MissingAccountState, 1337);

        let nonce = provider.nonce(Address::repeat_byte(0xaa), None).await.unwrap();
        assert_eq!(nonce, 0);
    }

    #[tokio::test]
    async fn test_missing_account_storage_returns_zero() {
        let index = Arc::new(BlockIndex::new());
        let provider = IndexedStateProvider::with_chain_id(index, MissingAccountState, 1337);

        let value =
            provider.storage(Address::repeat_byte(0xaa), U256::from(7), None).await.unwrap();
        assert_eq!(value, U256::ZERO);
    }

    #[tokio::test]
    async fn test_block_by_number() {
        let index = Arc::new(BlockIndex::new());
        let block_hash = B256::repeat_byte(1);
        index.insert_block(create_test_block(1, block_hash), vec![], vec![]);

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let block =
            provider.block_by_number(BlockNumberOrTag::Number(U64::from(1)), false).await.unwrap();
        assert!(block.is_some());
        assert_eq!(block.unwrap().hash, block_hash);
    }

    #[tokio::test]
    async fn test_block_by_hash() {
        let index = Arc::new(BlockIndex::new());
        let block_hash = B256::repeat_byte(1);
        index.insert_block(create_test_block(1, block_hash), vec![], vec![]);

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let block = provider.block_by_hash(block_hash, false).await.unwrap();
        assert!(block.is_some());
        assert_eq!(block.unwrap().number, U64::from(1));
    }

    #[tokio::test]
    async fn test_block_by_number_full_transactions() {
        let index = Arc::new(BlockIndex::new());
        let block_hash = B256::repeat_byte(1);
        let tx_hash = B256::repeat_byte(2);
        let mut block = create_test_block(1, block_hash);
        block.transaction_hashes = vec![tx_hash];
        index.insert_block(block, vec![create_test_tx(tx_hash, block_hash, 1)], vec![]);

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let block =
            provider.block_by_number(BlockNumberOrTag::Number(U64::from(1)), true).await.unwrap();
        let block = block.expect("block should exist");
        match block.transactions {
            BlockTransactions::Full(txs) => {
                assert_eq!(txs.len(), 1);
                assert_eq!(txs[0].hash, tx_hash);
            }
            BlockTransactions::Hashes(_) => panic!("expected full transactions"),
        }
    }

    #[tokio::test]
    async fn test_code_missing_account_returns_empty() {
        let index = Arc::new(BlockIndex::new());
        let provider = IndexedStateProvider::with_chain_id(index, MissingAccountState, 1337);

        let code = provider.code(Address::repeat_byte(0xaa), None).await.unwrap();
        assert!(code.is_empty());
    }

    #[tokio::test]
    async fn test_transaction_by_hash() {
        let index = Arc::new(BlockIndex::new());
        let block_hash = B256::repeat_byte(1);
        let tx_hash = B256::repeat_byte(2);
        index.insert_block(
            create_test_block(1, block_hash),
            vec![create_test_tx(tx_hash, block_hash, 1)],
            vec![],
        );

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let tx = provider.transaction_by_hash(tx_hash).await.unwrap();
        assert!(tx.is_some());
        assert_eq!(tx.unwrap().hash, tx_hash);
    }

    #[tokio::test]
    async fn test_receipt_by_hash() {
        let index = Arc::new(BlockIndex::new());
        let block_hash = B256::repeat_byte(1);
        let tx_hash = B256::repeat_byte(2);
        index.insert_block(
            create_test_block(1, block_hash),
            vec![create_test_tx(tx_hash, block_hash, 1)],
            vec![create_test_receipt(tx_hash, block_hash, 1)],
        );

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let receipt = provider.receipt_by_hash(tx_hash).await.unwrap();
        assert!(receipt.is_some());
        let receipt = receipt.unwrap();
        assert_eq!(receipt.transaction_hash, tx_hash);
        assert_eq!(receipt.logs.len(), 1);
    }

    #[tokio::test]
    async fn get_logs_returns_indexed_block_and_transaction_metadata() {
        let index = Arc::new(BlockIndex::new());
        let block_hash = B256::repeat_byte(5);
        let tx_hash = B256::repeat_byte(2);
        let log_address = Address::repeat_byte(0xcc);
        let receipt = IndexedReceipt {
            transaction_hash: tx_hash,
            block_hash,
            block_number: 5,
            transaction_index: 2,
            from: Address::repeat_byte(0xaa),
            to: Some(Address::repeat_byte(0xbb)),
            cumulative_gas_used: 42_000,
            gas_used: 21_000,
            contract_address: None,
            logs: vec![IndexedLog {
                address: log_address,
                topics: vec![B256::repeat_byte(0xdd)],
                data: Bytes::from_static(&[0x01]),
                log_index: 9,
                block_number: 5,
                block_hash,
                transaction_hash: tx_hash,
                transaction_index: 2,
            }],
            logs_bloom: Bloom::ZERO,
            tx_type: 2,
            effective_gas_price: 12_000_000_000,
            status: true,
        };
        index.insert_block(
            create_test_block(5, block_hash),
            vec![create_test_tx(tx_hash, block_hash, 5)],
            vec![receipt],
        );
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);
        let logs = provider
            .get_logs(RpcLogFilter {
                from_block: Some(BlockNumberOrTag::Number(U64::from(5))),
                to_block: Some(BlockNumberOrTag::Number(U64::from(5))),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].address, log_address);
        assert_eq!(logs[0].block_number, U64::from(5));
        assert_eq!(logs[0].block_hash, block_hash);
        assert_eq!(logs[0].transaction_hash, tx_hash);
        assert_eq!(logs[0].transaction_index, U64::from(2));
        assert_eq!(logs[0].log_index, U64::from(9));
    }

    #[tokio::test]
    async fn test_block_number() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let num = provider.block_number().await.unwrap();
        assert_eq!(num, 5);
    }

    #[tokio::test]
    async fn test_resolve_block_tags() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);

        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let block =
            provider.block_by_number(BlockNumberOrTag::Tag(BlockTag::Latest), false).await.unwrap();
        assert!(block.is_some());
        assert_eq!(block.unwrap().number, U64::from(10));

        let block = provider
            .block_by_number(BlockNumberOrTag::Tag(BlockTag::Earliest), false)
            .await
            .unwrap();
        assert!(block.is_none());
    }

    // --- reject_historical_block tests ---

    #[tokio::test]
    async fn balance_with_none_block_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let balance = provider.balance(Address::ZERO, None).await.unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn balance_with_latest_tag_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let balance = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Tag(BlockTag::Latest)))
            .await
            .unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn balance_with_latest_default_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let balance =
            provider.balance(Address::ZERO, Some(BlockNumberOrTag::Latest)).await.unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn balance_with_pending_tag_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let balance = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Tag(BlockTag::Pending)))
            .await
            .unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn balance_with_current_block_number_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let balance = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Number(U64::from(5))))
            .await
            .unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn balance_with_historical_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Number(U64::from(5))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
        assert!(err.to_string().contains("historical state not available"));
    }

    #[tokio::test]
    async fn balance_with_future_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Number(U64::from(20))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::InvalidBlockNumber(_)));
        assert!(err.to_string().contains("block not yet available"));
    }

    #[tokio::test]
    async fn nonce_with_historical_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .nonce(Address::ZERO, Some(BlockNumberOrTag::Number(U64::from(3))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
    }

    #[tokio::test]
    async fn code_with_historical_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .code(Address::ZERO, Some(BlockNumberOrTag::Number(U64::from(3))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
    }

    #[tokio::test]
    async fn storage_with_historical_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .storage(Address::ZERO, U256::from(1), Some(BlockNumberOrTag::Number(U64::from(3))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
    }

    #[tokio::test]
    async fn call_with_historical_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .call(CallRequest::default(), Some(BlockNumberOrTag::Number(U64::from(3))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
        assert!(err.to_string().contains("historical state not available"));
    }

    #[tokio::test]
    async fn call_with_future_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .call(CallRequest::default(), Some(BlockNumberOrTag::Number(U64::from(20))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::InvalidBlockNumber(_)));
        assert!(err.to_string().contains("block not yet available"));
    }

    #[tokio::test]
    async fn estimate_gas_with_historical_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .estimate_gas(CallRequest::default(), Some(BlockNumberOrTag::Number(U64::from(3))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
        assert!(err.to_string().contains("historical state not available"));
    }

    #[tokio::test]
    async fn estimate_gas_with_future_block_number_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(10, B256::repeat_byte(10)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .estimate_gas(CallRequest::default(), Some(BlockNumberOrTag::Number(U64::from(20))))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::InvalidBlockNumber(_)));
        assert!(err.to_string().contains("block not yet available"));
    }

    #[tokio::test]
    async fn balance_with_earliest_tag_returns_error() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let err = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Tag(BlockTag::Earliest)))
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Unsupported(_)));
        assert!(err.to_string().contains("historical state not available"));
    }

    #[tokio::test]
    async fn balance_with_safe_tag_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        // In BFT consensus all committed blocks are immediately finalized,
        // so "safe" is semantically equivalent to "latest".
        let balance = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Tag(BlockTag::Safe)))
            .await
            .unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn balance_with_finalized_tag_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        // In BFT consensus all committed blocks are immediately finalized,
        // so "finalized" is semantically equivalent to "latest".
        let balance = provider
            .balance(Address::ZERO, Some(BlockNumberOrTag::Tag(BlockTag::Finalized)))
            .await
            .unwrap();
        assert_eq!(balance, U256::from(1000));
    }

    #[tokio::test]
    async fn nonce_with_none_block_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let nonce = provider.nonce(Address::ZERO, None).await.unwrap();
        assert_eq!(nonce, 42);
    }

    #[tokio::test]
    async fn storage_with_none_block_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        let value = provider.storage(Address::ZERO, U256::from(1), None).await.unwrap();
        assert_eq!(value, U256::from(123));
    }

    #[tokio::test]
    async fn code_with_none_block_succeeds() {
        let index = Arc::new(BlockIndex::new());
        index.insert_block(create_test_block(5, B256::repeat_byte(5)), vec![], vec![]);
        let provider = IndexedStateProvider::with_chain_id(index, MockState, 1337);

        // MockState returns B256::ZERO code_hash, so code() returns empty bytes
        let code = provider.code(Address::ZERO, None).await.unwrap();
        assert!(code.is_empty());
    }
}
