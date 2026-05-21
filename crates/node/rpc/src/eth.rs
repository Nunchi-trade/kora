//! Ethereum JSON-RPC API implementation.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use alloy_consensus::{Transaction as _, TxEnvelope, transaction::SignerRecoverable as _};
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::{Address, B256, Bytes, U64, U256};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use tokio::sync::RwLock;

use crate::{
    error::RpcError,
    state_provider::StateProvider,
    types::{
        BlockNumberOrTag, BlockTag, BlockTransactions, CallRequest, RpcBlock, RpcLog, RpcLogFilter,
        RpcTransaction, RpcTransactionReceipt,
    },
};

const DEFAULT_GAS_ORACLE_BLOCKS: usize = 20;
const DEFAULT_GAS_ORACLE_PERCENTILE: u8 = 60;
const GWEI: u64 = 1_000_000_000;
const DEFAULT_MAX_GAS_PRICE: u64 = 500 * GWEI;

/// Ethereum JSON-RPC API trait.
///
/// Defines the core eth_* methods required for Ethereum compatibility.
#[rpc(server, namespace = "eth")]
pub trait EthApi {
    /// Returns the chain ID.
    #[method(name = "chainId")]
    async fn chain_id(&self) -> RpcResult<U64>;

    /// Returns the current block number.
    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<U64>;

    /// Returns the balance of an account.
    #[method(name = "getBalance")]
    async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U256>;

    /// Returns the nonce (transaction count) of an account.
    #[method(name = "getTransactionCount")]
    async fn get_transaction_count(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U64>;

    /// Returns the code at an address.
    #[method(name = "getCode")]
    async fn get_code(&self, address: Address, block: Option<BlockNumberOrTag>)
    -> RpcResult<Bytes>;

    /// Returns the value of a storage slot.
    #[method(name = "getStorageAt")]
    async fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U256>;

    /// Submits a raw transaction to the mempool.
    #[method(name = "sendRawTransaction")]
    async fn send_raw_transaction(&self, data: Bytes) -> RpcResult<B256>;

    /// Executes a call without creating a transaction.
    #[method(name = "call")]
    async fn call(&self, request: CallRequest, block: Option<BlockNumberOrTag>)
    -> RpcResult<Bytes>;

    /// Estimates gas for a transaction.
    #[method(name = "estimateGas")]
    async fn estimate_gas(
        &self,
        request: CallRequest,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U64>;

    /// Returns a block by number.
    #[method(name = "getBlockByNumber")]
    async fn get_block_by_number(
        &self,
        block: BlockNumberOrTag,
        full_transactions: bool,
    ) -> RpcResult<Option<RpcBlock>>;

    /// Returns a block by hash.
    #[method(name = "getBlockByHash")]
    async fn get_block_by_hash(
        &self,
        hash: B256,
        full_transactions: bool,
    ) -> RpcResult<Option<RpcBlock>>;

    /// Returns a transaction by hash.
    #[method(name = "getTransactionByHash")]
    async fn get_transaction_by_hash(&self, hash: B256) -> RpcResult<Option<RpcTransaction>>;

    /// Returns a transaction receipt by hash.
    #[method(name = "getTransactionReceipt")]
    async fn get_transaction_receipt(&self, hash: B256)
    -> RpcResult<Option<RpcTransactionReceipt>>;

    /// Returns the current gas price.
    #[method(name = "gasPrice")]
    async fn gas_price(&self) -> RpcResult<U256>;

    /// Returns the max priority fee per gas.
    #[method(name = "maxPriorityFeePerGas")]
    async fn max_priority_fee_per_gas(&self) -> RpcResult<U256>;

    /// Returns fee history.
    #[method(name = "feeHistory")]
    async fn fee_history(
        &self,
        block_count: U64,
        newest_block: BlockNumberOrTag,
        reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory>;

    /// Returns the accounts owned by the client (empty for non-wallet nodes).
    #[method(name = "accounts")]
    async fn accounts(&self) -> RpcResult<Vec<Address>>;

    /// Returns protocol version.
    #[method(name = "protocolVersion")]
    async fn protocol_version(&self) -> RpcResult<String>;

    /// Returns syncing status.
    #[method(name = "syncing")]
    async fn syncing(&self) -> RpcResult<bool>;

    /// Returns logs matching the given filter.
    #[method(name = "getLogs")]
    async fn get_logs(&self, filter: RpcLogFilter) -> RpcResult<Vec<RpcLog>>;
}

/// Net namespace API.
#[rpc(server, namespace = "net")]
pub trait NetApi {
    /// Returns the network ID.
    #[method(name = "version")]
    fn version(&self) -> RpcResult<String>;

    /// Returns true if the client is listening for connections.
    #[method(name = "listening")]
    fn listening(&self) -> RpcResult<bool>;

    /// Returns the number of connected peers.
    #[method(name = "peerCount")]
    fn peer_count(&self) -> RpcResult<U64>;
}

/// Web3 namespace API.
#[rpc(server, namespace = "web3")]
pub trait Web3Api {
    /// Returns the client version.
    #[method(name = "clientVersion")]
    fn client_version(&self) -> RpcResult<String>;

    /// Returns the Keccak-256 hash of the given data.
    #[method(name = "sha3")]
    fn sha3(&self, data: Bytes) -> RpcResult<B256>;
}

/// Fee history response.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeHistory {
    /// Base fee per gas for each block.
    pub base_fee_per_gas: Vec<U256>,
    /// Gas used ratio for each block.
    pub gas_used_ratio: Vec<f64>,
    /// Oldest block number.
    pub oldest_block: U64,
    /// Reward percentiles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reward: Option<Vec<Vec<U256>>>,
}

/// Transaction submission callback type.
///
/// Called when a raw transaction is submitted via `eth_sendRawTransaction`.
/// Resolves successfully only if the transaction was accepted.
pub type TxSubmitFuture = Pin<Box<dyn Future<Output = Result<(), RpcError>> + Send>>;

/// Async transaction submission callback type.
pub type TxSubmitCallback = Arc<dyn Fn(Bytes) -> TxSubmitFuture + Send + Sync>;

/// Configuration for recent-block fee estimation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GasOracleConfig {
    /// Number of recent blocks sampled by the oracle.
    pub blocks: usize,
    /// Percentile used when selecting sampled gas prices and priority fees.
    pub percentile: u8,
    /// Minimum total gas price returned by `eth_gasPrice`.
    pub min_price: U256,
    /// Maximum total gas price returned by `eth_gasPrice`.
    pub max_price: U256,
    /// Minimum priority fee returned by `eth_maxPriorityFeePerGas`.
    pub min_priority_fee: U256,
}

impl Default for GasOracleConfig {
    fn default() -> Self {
        Self {
            blocks: DEFAULT_GAS_ORACLE_BLOCKS,
            percentile: DEFAULT_GAS_ORACLE_PERCENTILE,
            min_price: U256::from(GWEI),
            max_price: U256::from(DEFAULT_MAX_GAS_PRICE),
            min_priority_fee: U256::from(GWEI),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct GasOracleEstimate {
    gas_price: U256,
    priority_fee: U256,
}

#[derive(Clone, Copy, Debug)]
struct CachedGasOracleEstimate {
    head: u64,
    estimate: GasOracleEstimate,
}

/// Ethereum API implementation with state provider.
pub struct EthApiImpl<S: StateProvider> {
    chain_id: u64,
    block_height: Arc<std::sync::atomic::AtomicU64>,
    tx_submit: Option<TxSubmitCallback>,
    state_provider: Arc<RwLock<S>>,
    pending_txs: Arc<RwLock<HashMap<B256, RpcTransaction>>>,
    gas_oracle_config: GasOracleConfig,
    gas_oracle_cache: Arc<RwLock<Option<CachedGasOracleEstimate>>>,
}

impl<S: StateProvider> std::fmt::Debug for EthApiImpl<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthApiImpl")
            .field("chain_id", &self.chain_id)
            .field("block_height", &self.block_height)
            .field("tx_submit", &self.tx_submit.is_some())
            .field("gas_oracle_config", &self.gas_oracle_config)
            .finish()
    }
}

impl<S: StateProvider + 'static> EthApiImpl<S> {
    /// Create a new Ethereum API implementation with a state provider.
    pub fn new(chain_id: u64, state_provider: S) -> Self {
        Self::from_parts(chain_id, state_provider, None, GasOracleConfig::default())
    }

    /// Create a new Ethereum API implementation with a transaction submission callback.
    pub fn with_tx_submit(chain_id: u64, state_provider: S, tx_submit: TxSubmitCallback) -> Self {
        Self::from_parts(chain_id, state_provider, Some(tx_submit), GasOracleConfig::default())
    }

    fn from_parts(
        chain_id: u64,
        state_provider: S,
        tx_submit: Option<TxSubmitCallback>,
        gas_oracle_config: GasOracleConfig,
    ) -> Self {
        Self {
            chain_id,
            block_height: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            tx_submit,
            state_provider: Arc::new(RwLock::new(state_provider)),
            pending_txs: Arc::new(RwLock::new(HashMap::new())),
            gas_oracle_config,
            gas_oracle_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Override the default recent-block gas oracle configuration.
    pub fn with_gas_oracle_config(mut self, gas_oracle_config: GasOracleConfig) -> Self {
        self.gas_oracle_config = gas_oracle_config;
        self.gas_oracle_cache = Arc::new(RwLock::new(None));
        self
    }

    /// Get a handle to update the block height.
    pub fn block_height_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.block_height.clone()
    }

    /// Update the current block height.
    pub fn set_block_height(&self, height: u64) {
        self.block_height.store(height, std::sync::atomic::Ordering::Relaxed);
    }

    async fn recent_fee_estimate(&self) -> RpcResult<GasOracleEstimate> {
        let provider = self.state_provider.read().await;
        let head = provider
            .block_number()
            .await
            .unwrap_or_else(|_| self.block_height.load(std::sync::atomic::Ordering::Relaxed));

        if let Some(cached) = *self.gas_oracle_cache.read().await
            && cached.head == head
        {
            return Ok(cached.estimate);
        }

        let estimate = estimate_recent_fees(&*provider, head, self.gas_oracle_config).await;
        *self.gas_oracle_cache.write().await = Some(CachedGasOracleEstimate { head, estimate });
        Ok(estimate)
    }
}

#[jsonrpsee::core::async_trait]
impl<S: StateProvider + 'static> EthApiServer for EthApiImpl<S> {
    async fn chain_id(&self) -> RpcResult<U64> {
        Ok(U64::from(self.chain_id))
    }

    async fn block_number(&self) -> RpcResult<U64> {
        let provider = self.state_provider.read().await;
        provider.block_number().await.map_or_else(
            |_| {
                let height = self.block_height.load(std::sync::atomic::Ordering::Relaxed);
                Ok(U64::from(height))
            },
            |height| Ok(U64::from(height)),
        )
    }

    async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U256> {
        let provider = self.state_provider.read().await;
        provider.balance(address, block).await.map_err(Into::into)
    }

    async fn get_transaction_count(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U64> {
        let provider = self.state_provider.read().await;
        let nonce = provider.nonce(address, block).await?;
        Ok(U64::from(nonce))
    }

    async fn get_code(
        &self,
        address: Address,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<Bytes> {
        let provider = self.state_provider.read().await;
        provider.code(address, block).await.map_err(Into::into)
    }

    async fn get_storage_at(
        &self,
        address: Address,
        slot: U256,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U256> {
        let provider = self.state_provider.read().await;
        provider.storage(address, slot, block).await.map_err(Into::into)
    }

    async fn send_raw_transaction(&self, data: Bytes) -> RpcResult<B256> {
        let tx_hash = alloy_primitives::keccak256(&data);
        let pending_tx = raw_tx_to_pending_rpc(&data)?;

        if let Some(ref submit) = self.tx_submit {
            submit(data).await?;
        }

        self.pending_txs.write().await.insert(tx_hash, pending_tx);
        Ok(tx_hash)
    }

    async fn call(
        &self,
        request: CallRequest,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<Bytes> {
        let provider = self.state_provider.read().await;
        provider.call(request, block).await.map_err(Into::into)
    }

    async fn estimate_gas(
        &self,
        request: CallRequest,
        block: Option<BlockNumberOrTag>,
    ) -> RpcResult<U64> {
        let provider = self.state_provider.read().await;
        let gas = provider.estimate_gas(request, block).await?;
        Ok(U64::from(gas))
    }

    async fn get_block_by_number(
        &self,
        block: BlockNumberOrTag,
        full_transactions: bool,
    ) -> RpcResult<Option<RpcBlock>> {
        let provider = self.state_provider.read().await;
        provider.block_by_number(block, full_transactions).await.map_err(Into::into)
    }

    async fn get_block_by_hash(
        &self,
        hash: B256,
        full_transactions: bool,
    ) -> RpcResult<Option<RpcBlock>> {
        let provider = self.state_provider.read().await;
        provider.block_by_hash(hash, full_transactions).await.map_err(Into::into)
    }

    async fn get_transaction_by_hash(&self, hash: B256) -> RpcResult<Option<RpcTransaction>> {
        let provider = self.state_provider.read().await;
        let indexed = provider.transaction_by_hash(hash).await?;
        if indexed.is_some() {
            self.pending_txs.write().await.remove(&hash);
            return Ok(indexed);
        }
        Ok(self.pending_txs.read().await.get(&hash).cloned())
    }

    async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> RpcResult<Option<RpcTransactionReceipt>> {
        let provider = self.state_provider.read().await;
        provider.receipt_by_hash(hash).await.map_err(Into::into)
    }

    async fn gas_price(&self) -> RpcResult<U256> {
        Ok(self.recent_fee_estimate().await?.gas_price)
    }

    async fn max_priority_fee_per_gas(&self) -> RpcResult<U256> {
        Ok(self.recent_fee_estimate().await?.priority_fee)
    }

    async fn fee_history(
        &self,
        block_count: U64,
        newest_block: BlockNumberOrTag,
        reward_percentiles: Option<Vec<f64>>,
    ) -> RpcResult<FeeHistory> {
        let provider = self.state_provider.read().await;
        let head = provider
            .block_number()
            .await
            .unwrap_or_else(|_| self.block_height.load(std::sync::atomic::Ordering::Relaxed));
        let newest = resolve_fee_history_newest(newest_block, head);
        let requested = block_count.to::<u64>().min(1024);
        let count = requested.min(newest.saturating_add(1)) as usize;
        let oldest = newest.saturating_add(1).saturating_sub(count as u64);

        let mut base_fee_per_gas = Vec::with_capacity(count + 1);
        let mut gas_used_ratio = Vec::with_capacity(count);
        let mut reward = reward_percentiles.as_ref().map(|_| Vec::with_capacity(count));
        let mut last_base_fee = None;
        let mut last_gas_used = 0;
        let mut last_gas_limit = 0;

        for block_number in oldest..oldest + count as u64 {
            let block = block_by_number_or_none(&*provider, block_number, reward.is_some()).await;
            let base_fee = block
                .as_ref()
                .and_then(|block| block.base_fee_per_gas)
                .or(last_base_fee)
                .unwrap_or_else(default_base_fee);
            base_fee_per_gas.push(base_fee);

            if let Some(block) = block {
                let gas_used = block.gas_used.to::<u64>();
                let gas_limit = block.gas_limit.to::<u64>();
                gas_used_ratio.push(block_gas_used_ratio(gas_used, gas_limit));

                if let (Some(percentiles), Some(rows)) = (&reward_percentiles, reward.as_mut()) {
                    rows.push(compute_reward_percentiles(&block, percentiles));
                }

                last_base_fee = Some(base_fee);
                last_gas_used = gas_used;
                last_gas_limit = gas_limit;
            } else {
                gas_used_ratio.push(0.0);
                if let (Some(percentiles), Some(rows)) = (&reward_percentiles, reward.as_mut()) {
                    rows.push(vec![U256::ZERO; percentiles.len()]);
                }
            }
        }

        let next_base_fee = last_base_fee
            .map(|base_fee| calculate_next_base_fee(base_fee, last_gas_used, last_gas_limit))
            .unwrap_or_else(default_base_fee);
        base_fee_per_gas.push(next_base_fee);

        Ok(FeeHistory { base_fee_per_gas, gas_used_ratio, oldest_block: U64::from(oldest), reward })
    }

    async fn accounts(&self) -> RpcResult<Vec<Address>> {
        Ok(Vec::new())
    }

    async fn protocol_version(&self) -> RpcResult<String> {
        Ok("0x44".to_string())
    }

    async fn syncing(&self) -> RpcResult<bool> {
        Ok(false)
    }

    async fn get_logs(&self, filter: RpcLogFilter) -> RpcResult<Vec<RpcLog>> {
        let provider = self.state_provider.read().await;
        provider.get_logs(filter).await.map_err(Into::into)
    }
}

/// Net API implementation.
pub struct NetApiImpl {
    chain_id: u64,
    peer_count: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for NetApiImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetApiImpl")
            .field("chain_id", &self.chain_id)
            .field("peer_count", &self.peer_count.load(std::sync::atomic::Ordering::Relaxed))
            .finish()
    }
}

impl NetApiImpl {
    /// Create a new Net API implementation.
    pub fn new(chain_id: u64) -> Self {
        Self { chain_id, peer_count: Arc::new(std::sync::atomic::AtomicU64::new(0)) }
    }

    /// Get a handle to update the peer count.
    pub fn peer_count_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.peer_count.clone()
    }

    /// Update the peer count.
    pub fn set_peer_count(&self, count: u64) {
        self.peer_count.store(count, std::sync::atomic::Ordering::Relaxed);
    }
}

impl NetApiServer for NetApiImpl {
    fn version(&self) -> RpcResult<String> {
        Ok(self.chain_id.to_string())
    }

    fn listening(&self) -> RpcResult<bool> {
        Ok(true)
    }

    fn peer_count(&self) -> RpcResult<U64> {
        let count = self.peer_count.load(std::sync::atomic::Ordering::Relaxed);
        Ok(U64::from(count))
    }
}

/// Web3 API implementation.
#[derive(Clone, Debug, Default)]
pub struct Web3ApiImpl;

impl Web3ApiImpl {
    /// Create a new Web3 API implementation.
    pub const fn new() -> Self {
        Self
    }
}

impl Web3ApiServer for Web3ApiImpl {
    fn client_version(&self) -> RpcResult<String> {
        Ok(format!("kora/{}", env!("CARGO_PKG_VERSION")))
    }

    fn sha3(&self, data: Bytes) -> RpcResult<B256> {
        Ok(alloy_primitives::keccak256(&data))
    }
}

async fn estimate_recent_fees<S: StateProvider>(
    provider: &S,
    head: u64,
    config: GasOracleConfig,
) -> GasOracleEstimate {
    let block_count = config.blocks.max(1);
    let start = head.saturating_sub(block_count.saturating_sub(1) as u64);
    let mut gas_prices = Vec::new();
    let mut priority_fees = Vec::new();
    let mut latest_base_fee = None;

    for block_number in start..=head {
        let Some(block) = block_by_number_or_none(provider, block_number, true).await else {
            continue;
        };
        let base_fee = block.base_fee_per_gas.unwrap_or_else(default_base_fee);
        latest_base_fee = Some(base_fee);

        if let BlockTransactions::Full(txs) = &block.transactions {
            gas_prices.extend(txs.iter().map(|tx| effective_gas_price_for_sampling(tx, base_fee)));
            priority_fees.extend(txs.iter().map(|tx| effective_priority_fee(tx, base_fee)));
        }
    }

    let priority_fee =
        percentile_value(&mut priority_fees, config.percentile).unwrap_or(config.min_priority_fee);
    let priority_fee = priority_fee.max(config.min_priority_fee);
    let latest_base_fee = latest_base_fee.unwrap_or_else(default_base_fee);

    // Clamp priority fee so that base_fee + priority_fee does not exceed
    // max_price (when the base fee alone is still under the cap).
    let priority_fee = if latest_base_fee < config.max_price {
        priority_fee.min(config.max_price.saturating_sub(latest_base_fee))
    } else {
        priority_fee
    };

    let min_gas_price = config.min_price.max(latest_base_fee.saturating_add(priority_fee));
    let gas_price = percentile_value(&mut gas_prices, config.percentile).unwrap_or(min_gas_price);
    let gas_price = gas_price.max(min_gas_price);

    // Always enforce the hard cap unless the base fee alone exceeds it --
    // in that case the chain's base fee is already above the configured
    // maximum and we must still return a usable price.
    let gas_price = if latest_base_fee <= config.max_price {
        gas_price.min(config.max_price)
    } else {
        gas_price
    };

    GasOracleEstimate { gas_price, priority_fee }
}

async fn block_by_number_or_none<S: StateProvider>(
    provider: &S,
    block_number: u64,
    full_transactions: bool,
) -> Option<RpcBlock> {
    provider
        .block_by_number(BlockNumberOrTag::Number(U64::from(block_number)), full_transactions)
        .await
        .ok()
        .flatten()
}

fn resolve_fee_history_newest(newest_block: BlockNumberOrTag, head: u64) -> u64 {
    match newest_block {
        BlockNumberOrTag::Number(n) => n.to::<u64>().min(head),
        BlockNumberOrTag::Tag(BlockTag::Earliest) => 0,
        BlockNumberOrTag::Tag(_) | BlockNumberOrTag::Latest => head,
    }
}

fn default_base_fee() -> U256 {
    U256::from(GWEI)
}

fn percentile_value(values: &mut [U256], percentile: u8) -> Option<U256> {
    if values.is_empty() {
        return None;
    }

    values.sort_unstable();
    let percentile = usize::from(percentile.min(100));
    let index = (values.len() * percentile / 100).min(values.len() - 1);
    Some(values[index])
}

fn block_gas_used_ratio(gas_used: u64, gas_limit: u64) -> f64 {
    if gas_limit == 0 {
        return 0.0;
    }
    (gas_used as f64 / gas_limit as f64).clamp(0.0, 1.0)
}

fn compute_reward_percentiles(block: &RpcBlock, percentiles: &[f64]) -> Vec<U256> {
    let BlockTransactions::Full(txs) = &block.transactions else {
        return vec![U256::ZERO; percentiles.len()];
    };
    if txs.is_empty() {
        return vec![U256::ZERO; percentiles.len()];
    }

    let base_fee = block.base_fee_per_gas.unwrap_or_default();
    let mut rewards = txs
        .iter()
        .map(|tx| (effective_priority_fee(tx, base_fee), tx.gas.to::<u64>()))
        .filter(|(_, gas)| *gas > 0)
        .collect::<Vec<_>>();
    if rewards.is_empty() {
        return vec![U256::ZERO; percentiles.len()];
    }

    rewards.sort_by_key(|(tip, _)| *tip);
    let total_gas = rewards.iter().map(|(_, gas)| u128::from(*gas)).sum();

    percentiles
        .iter()
        .map(|percentile| weighted_percentile_reward(&rewards, total_gas, *percentile))
        .collect()
}

fn weighted_percentile_reward(rewards: &[(U256, u64)], total_gas: u128, percentile: f64) -> U256 {
    let threshold = percentile_threshold(total_gas, percentile);
    let mut cumulative_gas = 0u128;

    for (tip, gas) in rewards {
        cumulative_gas = cumulative_gas.saturating_add(u128::from(*gas));
        if cumulative_gas >= threshold {
            return *tip;
        }
    }

    rewards.last().map(|(tip, _)| *tip).unwrap_or_default()
}

fn percentile_threshold(total_gas: u128, percentile: f64) -> u128 {
    if total_gas == 0 {
        return 0;
    }

    let percentile = if percentile.is_finite() { percentile.clamp(0.0, 100.0) } else { 0.0 };
    ((total_gas as f64 * percentile / 100.0).ceil() as u128).min(total_gas)
}

fn effective_priority_fee(tx: &RpcTransaction, base_fee: U256) -> U256 {
    match (tx.max_fee_per_gas, tx.max_priority_fee_per_gas) {
        (Some(max_fee), Some(max_priority_fee)) => {
            max_priority_fee.min(max_fee.saturating_sub(base_fee))
        }
        _ if is_dynamic_fee_type(tx) => {
            // Indexed EIP-1559 (or later) tx without populated EIP-1559
            // fields: `gas_price` may represent `max_fee_per_gas`, so we
            // cannot reliably derive the tip. Return zero to avoid
            // inflating estimates.
            U256::ZERO
        }
        _ => tx.gas_price.saturating_sub(base_fee),
    }
}

/// Returns `true` when the transaction type uses dynamic-fee semantics
/// (types 2, 3, 4 -- EIP-1559, EIP-4844, EIP-7702).
fn is_dynamic_fee_type(tx: &RpcTransaction) -> bool {
    tx.tx_type.to::<u64>() >= 2
}

/// Derives the effective gas price a transaction actually paid, accounting
/// for the difference between legacy `gas_price` and EIP-1559
/// `min(max_fee, base_fee + tip)`.
fn effective_gas_price_for_sampling(tx: &RpcTransaction, base_fee: U256) -> U256 {
    match (tx.max_fee_per_gas, tx.max_priority_fee_per_gas) {
        (Some(max_fee), Some(max_priority_fee)) => {
            let tip = max_priority_fee.min(max_fee.saturating_sub(base_fee));
            base_fee.saturating_add(tip).min(max_fee)
        }
        _ => tx.gas_price,
    }
}

fn calculate_next_base_fee(
    parent_base_fee: U256,
    parent_gas_used: u64,
    parent_gas_limit: u64,
) -> U256 {
    let parent_gas_target = parent_gas_limit / 2;
    if parent_gas_target == 0 || parent_gas_used == parent_gas_target {
        return parent_base_fee;
    }

    if parent_gas_used > parent_gas_target {
        let gas_used_delta = parent_gas_used - parent_gas_target;
        let base_fee_delta = parent_base_fee * U256::from(gas_used_delta)
            / U256::from(parent_gas_target)
            / U256::from(8);
        parent_base_fee.saturating_add(base_fee_delta.max(U256::from(1)))
    } else {
        let gas_used_delta = parent_gas_target - parent_gas_used;
        let base_fee_delta = parent_base_fee * U256::from(gas_used_delta)
            / U256::from(parent_gas_target)
            / U256::from(8);
        parent_base_fee.saturating_sub(base_fee_delta)
    }
}

fn raw_tx_to_pending_rpc(data: &Bytes) -> Result<RpcTransaction, RpcError> {
    let envelope = TxEnvelope::decode_2718(&mut data.as_ref())
        .map_err(|err| RpcError::InvalidTransaction(format!("failed to decode: {err}")))?;
    let from = envelope
        .recover_signer()
        .map_err(|err| RpcError::InvalidTransaction(format!("failed to recover signer: {err}")))?;
    let signature = envelope.signature();
    let hash = alloy_primitives::keccak256(data);

    Ok(RpcTransaction {
        hash,
        nonce: U64::from(envelope.nonce()),
        block_hash: None,
        block_number: None,
        transaction_index: None,
        from,
        to: envelope.to(),
        value: envelope.value(),
        gas: U64::from(envelope.gas_limit()),
        gas_price: U256::from(effective_gas_price(&envelope)),
        input: envelope.input().clone(),
        tx_type: U64::from(transaction_type(&envelope)),
        chain_id: envelope.chain_id().map(U64::from),
        max_fee_per_gas: max_fee_per_gas(&envelope).map(U256::from),
        max_priority_fee_per_gas: max_priority_fee_per_gas(&envelope).map(U256::from),
        v: U64::from(u64::from(signature.v())),
        r: signature.r(),
        s: signature.s(),
    })
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

const fn effective_gas_price(envelope: &TxEnvelope) -> u128 {
    match envelope {
        TxEnvelope::Legacy(tx) => tx.tx().gas_price,
        TxEnvelope::Eip2930(tx) => tx.tx().gas_price,
        TxEnvelope::Eip1559(tx) => tx.tx().max_fee_per_gas,
        TxEnvelope::Eip4844(tx) => tx.tx().tx().max_fee_per_gas,
        TxEnvelope::Eip7702(tx) => tx.tx().max_fee_per_gas,
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
    use std::collections::HashMap;

    use alloy_consensus::{SignableTransaction as _, TxEip1559};
    use alloy_eips::eip2718::Encodable2718 as _;
    use alloy_primitives::{Signature, TxKind};
    use async_trait::async_trait;
    use k256::ecdsa::SigningKey;
    use sha3::{Digest as _, Keccak256};

    use super::*;
    use crate::state_provider::{NoopStateProvider, StateProvider};

    #[derive(Clone, Debug)]
    struct MockFeeStateProvider {
        blocks: HashMap<u64, RpcBlock>,
        head: u64,
    }

    impl MockFeeStateProvider {
        fn new(blocks: Vec<RpcBlock>) -> Self {
            let head = blocks.iter().map(|block| block.number.to::<u64>()).max().unwrap_or(0);
            let blocks =
                blocks.into_iter().map(|block| (block.number.to::<u64>(), block)).collect();
            Self { blocks, head }
        }

        fn resolve_block_number(&self, block: BlockNumberOrTag) -> u64 {
            match block {
                BlockNumberOrTag::Number(number) => number.to::<u64>(),
                BlockNumberOrTag::Tag(BlockTag::Earliest) => 0,
                BlockNumberOrTag::Tag(_) | BlockNumberOrTag::Latest => self.head,
            }
        }

        fn block_with_transaction_shape(
            &self,
            number: u64,
            full_transactions: bool,
        ) -> Option<RpcBlock> {
            let mut block = self.blocks.get(&number).cloned()?;
            if !full_transactions && let BlockTransactions::Full(txs) = &block.transactions {
                block.transactions =
                    BlockTransactions::Hashes(txs.iter().map(|tx| tx.hash).collect());
            }
            Some(block)
        }
    }

    #[async_trait]
    impl StateProvider for MockFeeStateProvider {
        async fn balance(
            &self,
            _address: Address,
            _block: Option<BlockNumberOrTag>,
        ) -> Result<U256, RpcError> {
            Ok(U256::ZERO)
        }

        async fn nonce(
            &self,
            _address: Address,
            _block: Option<BlockNumberOrTag>,
        ) -> Result<u64, RpcError> {
            Ok(0)
        }

        async fn code(
            &self,
            _address: Address,
            _block: Option<BlockNumberOrTag>,
        ) -> Result<Bytes, RpcError> {
            Ok(Bytes::new())
        }

        async fn storage(
            &self,
            _address: Address,
            _slot: U256,
            _block: Option<BlockNumberOrTag>,
        ) -> Result<U256, RpcError> {
            Ok(U256::ZERO)
        }

        async fn block_by_number(
            &self,
            block: BlockNumberOrTag,
            full_transactions: bool,
        ) -> Result<Option<RpcBlock>, RpcError> {
            Ok(self
                .block_with_transaction_shape(self.resolve_block_number(block), full_transactions))
        }

        async fn block_by_hash(
            &self,
            hash: B256,
            full_transactions: bool,
        ) -> Result<Option<RpcBlock>, RpcError> {
            let number = self
                .blocks
                .values()
                .find(|block| block.hash == hash)
                .map(|block| block.number.to::<u64>());
            Ok(number
                .and_then(|number| self.block_with_transaction_shape(number, full_transactions)))
        }

        async fn transaction_by_hash(
            &self,
            hash: B256,
        ) -> Result<Option<RpcTransaction>, RpcError> {
            Ok(self.blocks.values().find_map(|block| match &block.transactions {
                BlockTransactions::Full(txs) => txs.iter().find(|tx| tx.hash == hash).cloned(),
                BlockTransactions::Hashes(_) => None,
            }))
        }

        async fn receipt_by_hash(
            &self,
            _hash: B256,
        ) -> Result<Option<RpcTransactionReceipt>, RpcError> {
            Ok(None)
        }

        async fn block_number(&self) -> Result<u64, RpcError> {
            Ok(self.head)
        }
    }

    fn gwei(value: u64) -> U256 {
        U256::from(value * GWEI)
    }

    /// EIP-1559 transaction parameters for test block construction.
    struct Eip1559TxParams {
        max_fee: U256,
        max_priority_fee: U256,
    }

    fn make_fee_block(
        number: u64,
        base_fee_per_gas: U256,
        gas_used: u64,
        gas_limit: u64,
        gas_prices: Vec<U256>,
    ) -> RpcBlock {
        let block_hash = B256::repeat_byte(number as u8);
        let transactions = gas_prices
            .into_iter()
            .enumerate()
            .map(|(index, gas_price)| RpcTransaction {
                hash: B256::repeat_byte((number as u8).wrapping_mul(16).wrapping_add(index as u8)),
                nonce: U64::from(index as u64),
                block_hash: Some(block_hash),
                block_number: Some(U64::from(number)),
                transaction_index: Some(U64::from(index as u64)),
                from: Address::repeat_byte(0x11),
                to: Some(Address::repeat_byte(0x22)),
                value: U256::ZERO,
                gas: U64::from(21_000),
                gas_price,
                input: Bytes::new(),
                tx_type: U64::ZERO,
                chain_id: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                v: U64::ZERO,
                r: U256::ZERO,
                s: U256::ZERO,
            })
            .collect();

        RpcBlock {
            hash: block_hash,
            parent_hash: B256::ZERO,
            number: U64::from(number),
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bytes::new(),
            timestamp: U64::from(number),
            gas_limit: U64::from(gas_limit),
            gas_used: U64::from(gas_used),
            extra_data: Bytes::new(),
            mix_hash: B256::ZERO,
            nonce: Default::default(),
            base_fee_per_gas: Some(base_fee_per_gas),
            miner: Address::ZERO,
            difficulty: U256::ZERO,
            total_difficulty: U256::ZERO,
            uncles: vec![],
            size: U64::ZERO,
            transactions: BlockTransactions::Full(transactions),
        }
    }

    /// Build a block containing EIP-1559 (type 2) transactions with explicit
    /// `max_fee_per_gas` and `max_priority_fee_per_gas` fields.
    fn make_eip1559_fee_block(
        number: u64,
        base_fee_per_gas: U256,
        gas_used: u64,
        gas_limit: u64,
        txs: Vec<Eip1559TxParams>,
    ) -> RpcBlock {
        let block_hash = B256::repeat_byte(number as u8);
        let transactions = txs
            .into_iter()
            .enumerate()
            .map(|(index, params)| {
                // Effective gas price = min(max_fee, base_fee + tip)
                let tip =
                    params.max_priority_fee.min(params.max_fee.saturating_sub(base_fee_per_gas));
                let gas_price = base_fee_per_gas.saturating_add(tip).min(params.max_fee);
                RpcTransaction {
                    hash: B256::repeat_byte(
                        (number as u8).wrapping_mul(16).wrapping_add(index as u8),
                    ),
                    nonce: U64::from(index as u64),
                    block_hash: Some(block_hash),
                    block_number: Some(U64::from(number)),
                    transaction_index: Some(U64::from(index as u64)),
                    from: Address::repeat_byte(0x11),
                    to: Some(Address::repeat_byte(0x22)),
                    value: U256::ZERO,
                    gas: U64::from(21_000),
                    gas_price,
                    input: Bytes::new(),
                    tx_type: U64::from(2),
                    chain_id: Some(U64::from(1)),
                    max_fee_per_gas: Some(params.max_fee),
                    max_priority_fee_per_gas: Some(params.max_priority_fee),
                    v: U64::ZERO,
                    r: U256::ZERO,
                    s: U256::ZERO,
                }
            })
            .collect();

        RpcBlock {
            hash: block_hash,
            parent_hash: B256::ZERO,
            number: U64::from(number),
            state_root: B256::ZERO,
            transactions_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bytes::new(),
            timestamp: U64::from(number),
            gas_limit: U64::from(gas_limit),
            gas_used: U64::from(gas_used),
            extra_data: Bytes::new(),
            mix_hash: B256::ZERO,
            nonce: Default::default(),
            base_fee_per_gas: Some(base_fee_per_gas),
            miner: Address::ZERO,
            difficulty: U256::ZERO,
            total_difficulty: U256::ZERO,
            uncles: vec![],
            size: U64::ZERO,
            transactions: BlockTransactions::Full(transactions),
        }
    }

    fn signed_test_tx(chain_id: u64, nonce: u64) -> Bytes {
        let mut secret = [0u8; 32];
        secret[31] = 1;
        let key = SigningKey::from_bytes((&secret).into()).expect("valid key");
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 21_000,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(Address::repeat_byte(0xbb)),
            value: U256::from(1),
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let digest = Keccak256::new_with_prefix(tx.encoded_for_signing());
        let (sig, recid) = key.sign_digest_recoverable(digest).expect("sign tx");
        let signature = Signature::from((sig, recid));
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        let mut raw = Vec::new();
        envelope.encode_2718(&mut raw);
        Bytes::from(raw)
    }

    #[test]
    fn web3_client_version() {
        let api = Web3ApiImpl::new();
        let version = Web3ApiServer::client_version(&api).unwrap();
        assert!(version.starts_with("kora/"));
    }

    #[tokio::test]
    async fn eth_chain_id() {
        let api = EthApiImpl::new(1337, NoopStateProvider);
        let chain_id = EthApiServer::chain_id(&api).await.unwrap();
        assert_eq!(chain_id, U64::from(1337));
    }

    #[test]
    fn net_version() {
        let api = NetApiImpl::new(1337);
        let version = NetApiServer::version(&api).unwrap();
        assert_eq!(version, "1337");
    }

    #[tokio::test]
    async fn eth_block_number() {
        let api = EthApiImpl::new(1, NoopStateProvider);
        api.set_block_height(42);
        let block_number = EthApiServer::block_number(&api).await.unwrap();
        assert_eq!(block_number, U64::from(42));
    }

    #[tokio::test]
    async fn gas_price_reflects_recent_transactions() {
        let provider = MockFeeStateProvider::new(vec![
            make_fee_block(0, gwei(1), 21_000, 30_000_000, vec![gwei(2)]),
            make_fee_block(1, gwei(1), 21_000, 30_000_000, vec![gwei(4)]),
            make_fee_block(2, gwei(1), 21_000, 30_000_000, vec![gwei(6)]),
        ]);
        let api = EthApiImpl::new(1, provider);

        let gas_price = EthApiServer::gas_price(&api).await.unwrap();
        let priority_fee = EthApiServer::max_priority_fee_per_gas(&api).await.unwrap();

        assert_eq!(gas_price, gwei(4));
        assert_eq!(priority_fee, gwei(3));
    }

    #[tokio::test]
    async fn gas_price_falls_back_to_base_fee_plus_min_tip_without_transactions() {
        let provider =
            MockFeeStateProvider::new(vec![make_fee_block(0, gwei(5), 0, 30_000_000, vec![])]);
        let api = EthApiImpl::new(1, provider);

        let gas_price = EthApiServer::gas_price(&api).await.unwrap();
        let priority_fee = EthApiServer::max_priority_fee_per_gas(&api).await.unwrap();

        assert_eq!(gas_price, gwei(6));
        assert_eq!(priority_fee, gwei(1));
    }

    #[tokio::test]
    async fn fee_history_uses_indexed_base_fee_and_gas_ratio() {
        let provider = MockFeeStateProvider::new(vec![make_fee_block(
            0,
            gwei(7),
            15_000_000,
            30_000_000,
            vec![],
        )]);
        let api = EthApiImpl::new(1, provider);

        let history = EthApiServer::fee_history(&api, U64::from(1), BlockNumberOrTag::Latest, None)
            .await
            .unwrap();

        assert_eq!(history.oldest_block, U64::ZERO);
        assert_eq!(history.base_fee_per_gas, vec![gwei(7), gwei(7)]);
        assert_eq!(history.gas_used_ratio, vec![0.5]);
        assert!(history.reward.is_none());
    }

    #[tokio::test]
    async fn fee_history_rewards_reflect_actual_tips() {
        let provider = MockFeeStateProvider::new(vec![make_fee_block(
            0,
            gwei(1),
            42_000,
            30_000_000,
            vec![gwei(3), gwei(5)],
        )]);
        let api = EthApiImpl::new(1, provider);

        let history = EthApiServer::fee_history(
            &api,
            U64::from(1),
            BlockNumberOrTag::Latest,
            Some(vec![50.0]),
        )
        .await
        .unwrap();

        let rewards = history.reward.unwrap();
        assert_eq!(rewards, vec![vec![gwei(2)]]);
    }

    #[tokio::test]
    async fn fee_history_rewards_are_zero_for_empty_blocks() {
        let provider =
            MockFeeStateProvider::new(vec![make_fee_block(0, gwei(1), 0, 30_000_000, vec![])]);
        let api = EthApiImpl::new(1, provider);

        let history = EthApiServer::fee_history(
            &api,
            U64::from(1),
            BlockNumberOrTag::Latest,
            Some(vec![25.0, 75.0]),
        )
        .await
        .unwrap();

        let rewards = history.reward.unwrap();
        assert_eq!(rewards, vec![vec![U256::ZERO, U256::ZERO]]);
    }

    #[test]
    fn effective_priority_fee_eip1559_uses_min_of_tip_and_headroom() {
        // EIP-1559 tx: max_fee=10 gwei, max_priority_fee=3 gwei, base_fee=2 gwei.
        // headroom = max_fee - base_fee = 8 gwei
        // effective tip = min(3, 8) = 3 gwei
        let tx = RpcTransaction {
            hash: B256::ZERO,
            nonce: U64::ZERO,
            block_hash: None,
            block_number: None,
            transaction_index: None,
            from: Address::ZERO,
            to: None,
            value: U256::ZERO,
            gas: U64::from(21_000),
            gas_price: gwei(10),
            input: Bytes::new(),
            tx_type: U64::from(2),
            chain_id: Some(U64::from(1)),
            max_fee_per_gas: Some(gwei(10)),
            max_priority_fee_per_gas: Some(gwei(3)),
            v: U64::ZERO,
            r: U256::ZERO,
            s: U256::ZERO,
        };

        assert_eq!(effective_priority_fee(&tx, gwei(2)), gwei(3));
    }

    #[test]
    fn effective_priority_fee_eip1559_caps_at_headroom() {
        // EIP-1559 tx: max_fee=5 gwei, max_priority_fee=4 gwei, base_fee=3 gwei.
        // headroom = 5 - 3 = 2 gwei
        // effective tip = min(4, 2) = 2 gwei
        let tx = RpcTransaction {
            hash: B256::ZERO,
            nonce: U64::ZERO,
            block_hash: None,
            block_number: None,
            transaction_index: None,
            from: Address::ZERO,
            to: None,
            value: U256::ZERO,
            gas: U64::from(21_000),
            gas_price: gwei(5),
            input: Bytes::new(),
            tx_type: U64::from(2),
            chain_id: Some(U64::from(1)),
            max_fee_per_gas: Some(gwei(5)),
            max_priority_fee_per_gas: Some(gwei(4)),
            v: U64::ZERO,
            r: U256::ZERO,
            s: U256::ZERO,
        };

        assert_eq!(effective_priority_fee(&tx, gwei(3)), gwei(2));
    }

    #[test]
    fn effective_priority_fee_indexed_eip1559_without_fields_returns_zero() {
        // Indexed EIP-1559 tx where max_fee_per_gas/max_priority_fee_per_gas
        // are not populated (None), but gas_price holds max_fee_per_gas.
        // The fallback should return zero rather than inflating the tip.
        let tx = RpcTransaction {
            hash: B256::ZERO,
            nonce: U64::ZERO,
            block_hash: None,
            block_number: None,
            transaction_index: None,
            from: Address::ZERO,
            to: None,
            value: U256::ZERO,
            gas: U64::from(21_000),
            gas_price: gwei(20),
            input: Bytes::new(),
            tx_type: U64::from(2),
            chain_id: Some(U64::from(1)),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            v: U64::ZERO,
            r: U256::ZERO,
            s: U256::ZERO,
        };

        // Without the fix this would return gwei(19), inflating estimates.
        assert_eq!(effective_priority_fee(&tx, gwei(1)), U256::ZERO);
    }

    #[tokio::test]
    async fn gas_price_eip1559_uses_effective_price_not_max_fee() {
        // base_fee = 2 gwei. A type-2 tx with max_fee=20 gwei, tip=3 gwei.
        // Effective price = base_fee + min(tip, max_fee - base_fee) = 2+3 = 5 gwei.
        // Without the fix, the oracle would sample gas_price = max_fee = 20 gwei.
        let provider = MockFeeStateProvider::new(vec![make_eip1559_fee_block(
            0,
            gwei(2),
            21_000,
            30_000_000,
            vec![Eip1559TxParams { max_fee: gwei(20), max_priority_fee: gwei(3) }],
        )]);
        let api = EthApiImpl::new(1, provider);

        let gas_price = EthApiServer::gas_price(&api).await.unwrap();
        // Should be 5 gwei (base + tip), not 20 gwei (max_fee).
        assert_eq!(gas_price, gwei(5));
    }

    #[tokio::test]
    async fn gas_price_never_exceeds_max_price() {
        // Set up a scenario where min_gas_price (base_fee + priority_fee)
        // would exceed max_price. Ensure the oracle respects the cap.
        let config = GasOracleConfig {
            blocks: 1,
            percentile: 60,
            min_price: U256::from(GWEI),
            max_price: gwei(10),
            min_priority_fee: U256::from(GWEI),
        };
        // base_fee = 8 gwei, tx gas_price = 12 gwei
        // Without fix: min_gas_price = base_fee + priority_fee could exceed max_price
        let provider = MockFeeStateProvider::new(vec![make_fee_block(
            0,
            gwei(8),
            21_000,
            30_000_000,
            vec![gwei(12)],
        )]);
        let api = EthApiImpl::new(1, provider).with_gas_oracle_config(config);

        let gas_price = EthApiServer::gas_price(&api).await.unwrap();
        assert!(gas_price <= gwei(10), "gas_price {gas_price} should not exceed max_price 10 gwei");
    }

    #[tokio::test]
    async fn gas_price_allows_exceeding_max_when_base_fee_above_cap() {
        // When the base fee alone is above max_price, the oracle must still
        // return a usable price rather than clamping to max_price.
        let config = GasOracleConfig {
            blocks: 1,
            percentile: 60,
            min_price: U256::from(GWEI),
            max_price: gwei(5),
            min_priority_fee: U256::from(GWEI),
        };
        // base_fee = 10 gwei (above max_price of 5 gwei)
        let provider = MockFeeStateProvider::new(vec![make_fee_block(
            0,
            gwei(10),
            21_000,
            30_000_000,
            vec![gwei(12)],
        )]);
        let api = EthApiImpl::new(1, provider).with_gas_oracle_config(config);

        let gas_price = EthApiServer::gas_price(&api).await.unwrap();
        // Must be at least base_fee + min_priority_fee
        assert!(
            gas_price >= gwei(11),
            "gas_price {gas_price} should be at least base_fee + min_priority_fee when base_fee exceeds cap"
        );
    }

    #[test]
    fn web3_sha3() {
        let api = Web3ApiImpl::new();
        let hash = Web3ApiServer::sha3(&api, Bytes::from_static(b"hello")).unwrap();
        assert_eq!(hash, alloy_primitives::keccak256(b"hello"));
    }

    #[tokio::test]
    async fn eth_send_raw_transaction() {
        let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let submitted_clone = submitted.clone();
        let callback: TxSubmitCallback = Arc::new(move |_| {
            submitted_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        });

        let api = EthApiImpl::with_tx_submit(1, NoopStateProvider, callback);
        let tx_data = signed_test_tx(1, 0);
        let result = EthApiServer::send_raw_transaction(&api, tx_data.clone()).await;

        assert!(result.is_ok());
        assert!(submitted.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(result.unwrap(), alloy_primitives::keccak256(&tx_data));
    }

    #[tokio::test]
    async fn eth_get_transaction_by_hash_returns_pending_submission() {
        let callback: TxSubmitCallback = Arc::new(move |_| Box::pin(async { Ok(()) }));
        let api = EthApiImpl::with_tx_submit(1, NoopStateProvider, callback);
        let tx_data = signed_test_tx(1, 7);
        let hash = EthApiServer::send_raw_transaction(&api, tx_data).await.unwrap();

        let tx = EthApiServer::get_transaction_by_hash(&api, hash).await.unwrap();
        let tx = tx.expect("pending transaction should be visible");
        assert_eq!(tx.hash, hash);
        assert_eq!(tx.nonce, U64::from(7));
        assert!(tx.block_hash.is_none());
    }

    /// Regression: when no `tx_submit` callback is wired, `send_raw_transaction`
    /// silently accepts the tx and returns the hash, but the tx goes nowhere —
    /// no mempool, no producer, no block. This is exactly the failure mode
    /// observed on devnet 1337 (`http://65.109.61.210:8545`) where the deployed
    /// kora binary predates the runner's `with_tx_submit(...)` wiring (commit
    /// `beb637a`): every tx submitted via JSON-RPC was accepted, hash returned,
    /// but never included in any block.
    ///
    /// The fix lives in the runner: always wire `tx_submit` to a real mempool.
    /// The downstream observability fix (warn-log when build_block produces an
    /// empty block while the mempool is non-empty) lives in `app.rs`.
    #[tokio::test]
    async fn send_raw_transaction_with_no_callback_silently_accepts_but_drops() {
        let api = EthApiImpl::new(1, NoopStateProvider); // no tx_submit
        let tx_data = signed_test_tx(1, 0);
        let result = EthApiServer::send_raw_transaction(&api, tx_data.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), alloy_primitives::keccak256(&tx_data));
        // The tx is in pending_txs (so getTransactionByHash returns something) —
        // that's exactly what makes the bug invisible to operators.
        let cached =
            EthApiServer::get_transaction_by_hash(&api, alloy_primitives::keccak256(&tx_data))
                .await
                .unwrap();
        assert!(
            cached.is_some(),
            "RPC caches the tx for visibility even though it has nowhere to send it"
        );
    }

    /// Regression: the existing `eth_send_raw_transaction` test only verifies
    /// that the callback is invoked (a boolean flag). It does not verify that
    /// the bytes passed to the callback are the same bytes the caller sent.
    /// A regression that mangled the body (e.g. dropped the chainId, re-encoded
    /// the envelope, sent a partial slice) would still pass that test. This
    /// one captures the actual bytes and compares them.
    #[tokio::test]
    async fn send_raw_transaction_passes_full_tx_bytes_to_callback() {
        let captured: Arc<RwLock<Vec<Bytes>>> = Arc::new(RwLock::new(Vec::new()));
        let captured_clone = captured.clone();
        let callback: TxSubmitCallback = Arc::new(move |data| {
            let captured_clone = captured_clone.clone();
            Box::pin(async move {
                captured_clone.write().await.push(data);
                Ok(())
            })
        });
        let api = EthApiImpl::with_tx_submit(1, NoopStateProvider, callback);
        let tx_data = signed_test_tx(1, 42);
        let _ = EthApiServer::send_raw_transaction(&api, tx_data.clone()).await.unwrap();
        let inner = captured.read().await;
        assert_eq!(inner.len(), 1, "callback invoked exactly once");
        assert_eq!(
            &inner[0][..],
            &tx_data[..],
            "callback receives the caller's tx bytes verbatim — no re-encoding, no truncation"
        );
    }
}
