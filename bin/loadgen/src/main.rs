#![doc = include_str!("../README.md")]
//! Load generator for Kora devnet.
//!
//! Sends high volumes of EIP-1559 transactions to stress test the network.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_consensus::{SignableTransaction as _, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256, keccak256};
use clap::Parser;
use eyre::{Result, WrapErr as _};
use k256::ecdsa::SigningKey;
use sha3::{Digest as _, Keccak256};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

const MIN_LOADGEN_ACCOUNTS: usize = 1;
const MAX_LOADGEN_ACCOUNTS: usize = u8::MAX as usize;

/// Intrinsic gas for a simple ETH transfer (21,000).
const TRANSFER_GAS_LIMIT: u64 = 21_000;

/// Maximum retry attempts before giving up on a transaction.
const MAX_RETRY_ATTEMPTS: u64 = 10;

/// Base delay between retries; grows exponentially (base * 2^attempt).
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// Delay before retrying after a nonce gap (chain is behind).
const NONCE_GAP_DELAY: Duration = Duration::from_secs(1);

/// Interval between periodic progress reports.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

/// HTTP request timeout for RPC calls.
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum idle connections per host in the HTTP connection pool.
const RPC_POOL_MAX_IDLE: usize = 100;

/// Load generator CLI.
#[derive(Parser, Debug)]
#[command(name = "loadgen", about = "Load generator for Kora devnet")]
struct Args {
    /// RPC endpoint URL.
    #[arg(long, default_value = "http://127.0.0.1:8545")]
    rpc_url: String,

    /// Additional RPC endpoint URLs to broadcast each transaction to.
    ///
    /// Kora's current devnet mempools are validator-local, so devnet load tests
    /// should submit to all validator RPCs to ensure the active proposer has the
    /// transaction in its local mempool.
    #[arg(long, value_delimiter = ',')]
    broadcast_rpc_urls: Vec<String>,

    /// Number of accounts to use for sending transactions.
    #[arg(long, default_value = "10")]
    accounts: usize,

    /// Total number of transactions to send.
    #[arg(long, default_value = "1000")]
    total_txs: u64,

    /// Maximum number of concurrent in-flight requests.
    #[arg(long, default_value = "50")]
    concurrency: usize,

    /// Chain ID.
    #[arg(long, default_value = "1337")]
    chain_id: u64,

    /// Dry run (don't actually send transactions).
    #[arg(long)]
    dry_run: bool,

    /// Print each transaction hash.
    #[arg(long)]
    verbose: bool,

    /// Overall timeout in seconds. The load test aborts if it exceeds this duration.
    /// Defaults to 0 (no timeout).
    #[arg(long, default_value = "0")]
    timeout_secs: u64,
}

/// Account with signing key and nonce tracker.
struct Account {
    key: SigningKey,
    address: Address,
    nonce: AtomicU64,
    /// The on-chain nonce when this run started. Used to compute per-run
    /// confirmed counts during post-run verification.
    starting_nonce: AtomicU64,
}

impl Account {
    fn new(seed: u8) -> Self {
        let mut secret = [0u8; 32];
        secret[31] = seed;
        let key = SigningKey::from_bytes((&secret).into()).expect("valid key");
        let address = address_from_key(&key);
        Self { key, nonce: AtomicU64::new(0), starting_nonce: AtomicU64::new(0), address }
    }

    fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::Relaxed)
    }

    fn set_nonce(&self, nonce: u64) {
        self.nonce.store(nonce, Ordering::Relaxed);
    }

    fn set_starting_nonce(&self, nonce: u64) {
        self.starting_nonce.store(nonce, Ordering::Relaxed);
    }

    fn get_starting_nonce(&self) -> u64 {
        self.starting_nonce.load(Ordering::Relaxed)
    }
}

fn loadgen_seeds(accounts: usize) -> Result<Vec<u8>> {
    if !(MIN_LOADGEN_ACCOUNTS..=MAX_LOADGEN_ACCOUNTS).contains(&accounts) {
        eyre::bail!(
            "loadgen accounts must be between {} and {}, got {}",
            MIN_LOADGEN_ACCOUNTS,
            MAX_LOADGEN_ACCOUNTS,
            accounts
        );
    }

    let accounts = u8::try_from(accounts).expect("loadgen account count was validated");
    Ok((1..=accounts).collect())
}

fn address_from_key(key: &SigningKey) -> Address {
    let encoded = key.verifying_key().to_encoded_point(false);
    let pubkey = encoded.as_bytes();
    let hash = keccak256(&pubkey[1..]);
    Address::from_slice(&hash[12..])
}

fn sign_eip1559_transfer(
    key: &SigningKey,
    chain_id: u64,
    to: Address,
    value: U256,
    nonce: u64,
    gas_limit: u64,
) -> Bytes {
    let tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit,
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(to),
        value,
        access_list: Default::default(),
        input: Bytes::new(),
    };

    let digest = Keccak256::new_with_prefix(tx.encoded_for_signing());
    let (sig, recid) = key.sign_digest_recoverable(digest).expect("sign tx");
    let signature = Signature::from((sig, recid));
    let signed = tx.into_signed(signature);
    let envelope = TxEnvelope::from(signed);
    let mut raw_bytes = Vec::new();
    envelope.encode_2718(&mut raw_bytes);
    Bytes::from(raw_bytes)
}

fn parse_json_rpc_quantity(quantity: &str) -> Result<u64> {
    let value = quantity
        .strip_prefix("0x")
        .ok_or_else(|| eyre::eyre!("JSON-RPC quantity missing 0x prefix: {quantity}"))?;
    if value.is_empty() {
        eyre::bail!("JSON-RPC quantity has no digits: {quantity}");
    }

    u64::from_str_radix(value, 16)
        .wrap_err_with(|| format!("invalid JSON-RPC quantity: {quantity}"))
}

/// Returns `true` if the error message indicates a transport-level failure
/// (connection refused, timeout, etc.) rather than a semantic rejection
/// (nonce error, pool error, etc.).
fn is_transport_error(err: &str) -> bool {
    err.contains("error sending request")
        || err.contains("Connection refused")
        || err.contains("connection refused")
        || err.contains("timed out")
        || err.contains("connection closed")
        || err.contains("broken pipe")
        || err.contains("reset by peer")
}

/// HTTP client for RPC calls.
///
/// Multiple `RpcClient`s share a single underlying `reqwest::Client` connection
/// pool, which is more efficient than creating separate pools per endpoint.
#[derive(Clone)]
struct RpcClient {
    client: reqwest::Client,
    url: String,
}

impl RpcClient {
    fn new(url: String, client: reqwest::Client) -> Self {
        Self { client, url }
    }

    async fn send_raw_transaction(&self, raw_tx: &[u8]) -> Result<String> {
        let hex_tx = format!("0x{}", hex::encode(raw_tx));

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [hex_tx],
            "id": 1
        });

        let resp = self.client.post(&self.url).json(&body).send().await?;

        let json: serde_json::Value = resp.json().await?;

        if let Some(error) = json.get("error") {
            eyre::bail!("RPC error: {}", error);
        }

        Ok(json["result"].as_str().unwrap_or("").to_string())
    }

    async fn get_transaction_count(&self, address: Address) -> Result<u64> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionCount",
            "params": [address.to_string(), "latest"],
            "id": 1
        });

        let resp = self.client.post(&self.url).json(&body).send().await?;
        let json: serde_json::Value = resp.json().await?;

        if let Some(error) = json.get("error") {
            eyre::bail!("RPC error: {}", error);
        }

        let nonce_hex =
            json["result"].as_str().ok_or_else(|| eyre::eyre!("missing nonce result"))?;
        parse_json_rpc_quantity(nonce_hex)
    }
}

/// Query `eth_getTransactionCount` from any available RPC client, trying each
/// in order until one succeeds.
async fn get_nonce_from_any(clients: &[RpcClient], address: Address) -> Result<u64> {
    let mut last_err = None;
    for client in clients {
        match client.get_transaction_count(address).await {
            Ok(nonce) => return Ok(nonce),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| eyre::eyre!("no RPC clients configured")))
}

/// Send a transaction to a specific client (by index). Falls back to trying
/// other clients only on transport-level errors (timeouts, connection refused).
/// Semantic rejections (nonce errors, pool errors) are returned immediately
/// since they would fail identically on every validator.
async fn send_raw_transaction_to(
    clients: &[RpcClient],
    raw_tx: Bytes,
    target_idx: usize,
) -> Result<String> {
    let idx = target_idx % clients.len();

    // Try the target client first
    match clients[idx].send_raw_transaction(&raw_tx).await {
        Ok(hash) => Ok(hash),
        Err(e) => {
            let err_str = e.to_string();

            // Semantic rejections (nonce errors, pool errors) will fail on all
            // validators identically. Only fall back for transport errors.
            if !is_transport_error(&err_str) {
                return Err(e);
            }

            // Transport error: try other clients
            let mut errors = vec![err_str];
            for (i, client) in clients.iter().enumerate() {
                if i == idx {
                    continue;
                }
                match client.send_raw_transaction(&raw_tx).await {
                    Ok(hash) => return Ok(hash),
                    Err(e) => errors.push(e.to_string()),
                }
            }
            eyre::bail!("all RPC endpoints failed: {}", errors.join("; "))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let mut rpc_urls = Vec::with_capacity(args.broadcast_rpc_urls.len() + 1);
    rpc_urls.push(args.rpc_url.clone());
    rpc_urls.extend(args.broadcast_rpc_urls.iter().cloned());

    info!(
        rpc_url = %args.rpc_url,
        broadcast_rpc_urls = ?args.broadcast_rpc_urls,
        accounts = args.accounts,
        total_txs = args.total_txs,
        concurrency = args.concurrency,
        chain_id = args.chain_id,
        dry_run = args.dry_run,
        timeout_secs = args.timeout_secs,
        "Starting load generator"
    );

    let account_seeds = loadgen_seeds(args.accounts)?;
    let accounts: Vec<Arc<Account>> =
        account_seeds.into_iter().map(|seed| Arc::new(Account::new(seed))).collect();

    info!("Sender addresses:");
    for acc in &accounts {
        info!("  {}", acc.address);
    }

    let receiver = Address::repeat_byte(0xBB);
    let transfer_amount = U256::from(1u64);

    let http_client = reqwest::Client::builder()
        .timeout(RPC_TIMEOUT)
        .pool_max_idle_per_host(RPC_POOL_MAX_IDLE)
        .build()
        .expect("build http client");
    let clients: Arc<Vec<RpcClient>> = Arc::new(
        rpc_urls.into_iter().map(|url| RpcClient::new(url, http_client.clone())).collect(),
    );

    // Initialize nonces from chain state, with fallback across all RPC endpoints
    if !args.dry_run {
        for account in &accounts {
            let nonce =
                get_nonce_from_any(&clients, account.address).await.wrap_err_with(|| {
                    format!("failed to query nonce for {} from any RPC endpoint", account.address)
                })?;
            account.set_starting_nonce(nonce);
            account.set_nonce(nonce);
        }
    }

    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));
    let nonce_resync_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // Derive optional deadline from --timeout-secs
    let deadline = if args.timeout_secs > 0 {
        Some(start + Duration::from_secs(args.timeout_secs))
    } else {
        None
    };

    if args.dry_run {
        for i in 0..args.total_txs {
            let account = &accounts[i as usize % accounts.len()];
            let nonce = account.next_nonce();
            let _tx = sign_eip1559_transfer(
                &account.key,
                args.chain_id,
                receiver,
                transfer_amount,
                nonce,
                TRANSFER_GAS_LIMIT,
            );
            success_count.fetch_add(1, Ordering::Relaxed);
            if (i + 1) % 1000 == 0 {
                info!(tx = i + 1, "Dry run progress");
            }
        }
    } else {
        // Per-account sequential sends with cross-account parallelism.
        // Each account sends its transactions one at a time (ensuring nonce ordering),
        // but all accounts run in parallel. A semaphore limits total in-flight requests.
        let num_accounts = accounts.len();
        let txs_per_account = args.total_txs / num_accounts as u64;
        let remainder = args.total_txs % num_accounts as u64;

        // Global concurrency limiter -- bounds total in-flight HTTP requests
        if args.concurrency == 0 {
            eyre::bail!("--concurrency must be >= 1");
        }
        let semaphore = Arc::new(Semaphore::new(args.concurrency));

        // Spawn periodic progress reporter
        let progress_success = success_count.clone();
        let progress_failure = failure_count.clone();
        let progress_resyncs = nonce_resync_count.clone();
        let progress_total = args.total_txs;
        let progress_start = start;
        let progress_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(PROGRESS_INTERVAL);
            interval.tick().await; // skip first immediate tick
            loop {
                interval.tick().await;
                let s = progress_success.load(Ordering::Relaxed);
                let f = progress_failure.load(Ordering::Relaxed);
                let r = progress_resyncs.load(Ordering::Relaxed);
                let completed = s + f;
                let elapsed = progress_start.elapsed().as_secs_f64();
                let tps = if elapsed > 0.0 { s as f64 / elapsed } else { 0.0 };
                info!(
                    success = s,
                    failed = f,
                    total = progress_total,
                    nonce_resyncs = r,
                    elapsed_secs = format!("{:.1}", elapsed),
                    tps = format!("{:.1}", tps),
                    pct = format!("{:.1}%", completed as f64 / progress_total as f64 * 100.0),
                    "progress"
                );
                if completed >= progress_total {
                    break;
                }
            }
        });

        let mut handles = Vec::with_capacity(num_accounts);

        for (idx, account) in accounts.iter().enumerate() {
            let account = account.clone();
            let clients = clients.clone();
            let success = success_count.clone();
            let failure = failure_count.clone();
            let resyncs = nonce_resync_count.clone();
            let semaphore = semaphore.clone();
            let verbose = args.verbose;
            let chain_id = args.chain_id;

            // Each account is pinned to one validator (avoids stale copies in other mempools)
            let target_validator = idx;

            // First `remainder` accounts send one extra tx
            let count = txs_per_account + if (idx as u64) < remainder { 1 } else { 0 };

            let handle = tokio::spawn(async move {
                // Use a while loop that tracks transactions completed (sent or
                // permanently failed), not nonces attempted. A nonce resync does
                // not consume a "send slot" -- the outer loop re-acquires a fresh
                // nonce and re-signs a new transaction.
                let mut sent = 0u64;
                while sent < count {
                    // Check deadline before each transaction
                    if let Some(dl) = deadline {
                        if Instant::now() >= dl {
                            warn!(
                                account = %account.address,
                                completed = sent,
                                target = count,
                                "timeout reached, stopping account"
                            );
                            break;
                        }
                    }

                    let nonce = account.next_nonce();
                    let tx = sign_eip1559_transfer(
                        &account.key,
                        chain_id,
                        receiver,
                        transfer_amount,
                        nonce,
                        TRANSFER_GAS_LIMIT,
                    );

                    // Retry with exponential backoff on transient errors. Nonce
                    // errors trigger resync instead of blind retries. The semaphore
                    // permit is acquired per-attempt and dropped after the HTTP call
                    // completes, so backoff sleeps do not consume concurrency slots.
                    let mut attempts = 0u32;
                    let mut needs_resync = false;
                    loop {
                        let _permit = semaphore.acquire().await.expect("semaphore closed");
                        let result =
                            send_raw_transaction_to(&clients, tx.clone(), target_validator).await;
                        drop(_permit);

                        match result {
                            Ok(hash) => {
                                success.fetch_add(1, Ordering::Relaxed);
                                if verbose {
                                    info!(nonce, hash = %hash, account = %account.address, "tx sent");
                                }
                                sent += 1;
                                break;
                            }
                            Err(e) => {
                                let err_msg = e.to_string();
                                attempts += 1;

                                if err_msg.contains("nonce too low") {
                                    // Transaction was already included on-chain
                                    // (e.g. via broadcast copy). Re-query chain
                                    // nonce and advance local counter.
                                    match get_nonce_from_any(&clients, account.address).await {
                                        Ok(chain_nonce) => {
                                            account.set_nonce(chain_nonce);
                                            resyncs.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(resync_err) => {
                                            warn!(
                                                account = %account.address,
                                                error = %resync_err,
                                                "nonce resync failed after nonce-too-low, \
                                                 keeping local nonce"
                                            );
                                        }
                                    }
                                    // The nonce was consumed on-chain; count as success.
                                    success.fetch_add(1, Ordering::Relaxed);
                                    sent += 1;
                                    break;
                                } else if err_msg.contains("already in pool") {
                                    // Transaction with this nonce is already pending
                                    // in the pool. The nonce is covered.
                                    success.fetch_add(1, Ordering::Relaxed);
                                    sent += 1;
                                    break;
                                } else if err_msg.contains("nonce gap") {
                                    // We are ahead of the chain. Wait, resync nonce,
                                    // and restart the outer loop with a fresh nonce
                                    // and re-signed transaction.
                                    warn!(
                                        nonce,
                                        error = %e,
                                        account = %account.address,
                                        "nonce gap detected, resyncing"
                                    );
                                    tokio::time::sleep(NONCE_GAP_DELAY).await;
                                    match get_nonce_from_any(&clients, account.address).await {
                                        Ok(chain_nonce) => {
                                            account.set_nonce(chain_nonce);
                                            resyncs.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(resync_err) => {
                                            warn!(
                                                account = %account.address,
                                                error = %resync_err,
                                                "nonce resync failed during gap recovery, \
                                                 will retry on next iteration"
                                            );
                                            // Brief backoff before the outer loop retries
                                            tokio::time::sleep(NONCE_GAP_DELAY).await;
                                        }
                                    }
                                    // Do NOT increment `sent` -- this nonce was never
                                    // consumed. Break inner loop and let the outer
                                    // while-loop re-acquire a correct nonce.
                                    needs_resync = true;
                                    break;
                                } else {
                                    // Transient error -- exponential backoff
                                    if u64::from(attempts) >= MAX_RETRY_ATTEMPTS {
                                        warn!(
                                            nonce,
                                            error = %e,
                                            account = %account.address,
                                            "tx failed after retries"
                                        );
                                        failure.fetch_add(1, Ordering::Relaxed);
                                        sent += 1;
                                        break;
                                    }
                                    // Exponential backoff: 100ms, 200ms, 400ms, ...
                                    let delay =
                                        RETRY_BASE_DELAY * 2u32.saturating_pow(attempts - 1);
                                    tokio::time::sleep(delay).await;
                                }
                            }
                        }
                    }

                    // After a nonce resync, the pre-signed tx is stale. The outer
                    // while-loop will re-acquire a fresh nonce on the next iteration.
                    // No nonce rewind is needed -- nonce management is handled
                    // exclusively inside the error handlers above.
                    if needs_resync {
                        continue;
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all account tasks to finish
        for handle in handles {
            handle.await?;
        }

        // Stop the progress reporter
        progress_handle.abort();
    }

    let elapsed = start.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let failure = failure_count.load(Ordering::Relaxed);
    let resyncs = nonce_resync_count.load(Ordering::Relaxed);
    let tps =
        if elapsed.as_secs_f64() > 0.0 { success as f64 / elapsed.as_secs_f64() } else { 0.0 };

    info!(
        sent = success + failure,
        success,
        failed = failure,
        nonce_resyncs = resyncs,
        elapsed_secs = format!("{:.2}", elapsed.as_secs_f64()),
        tps = format!("{:.2}", tps),
        "Load generation complete"
    );

    // Post-run inclusion verification: compare expected nonces against on-chain
    // state to detect silently dropped transactions.
    if !args.dry_run {
        info!("Verifying on-chain inclusion...");
        let mut total_confirmed = 0u64;
        let mut total_pending = 0u64;

        for account in &accounts {
            let expected_nonce = account.nonce.load(Ordering::Relaxed);
            let starting_nonce = account.get_starting_nonce();
            match get_nonce_from_any(&clients, account.address).await {
                Ok(chain_nonce) => {
                    let gap = expected_nonce.saturating_sub(chain_nonce);
                    let confirmed_this_run = chain_nonce.saturating_sub(starting_nonce);
                    if gap > 0 {
                        warn!(
                            account = %account.address,
                            expected = expected_nonce,
                            confirmed = chain_nonce,
                            pending = gap,
                            "account has unconfirmed transactions"
                        );
                    }
                    total_confirmed += confirmed_this_run;
                    total_pending += gap;
                }
                Err(e) => {
                    warn!(
                        account = %account.address,
                        error = %e,
                        "failed to verify on-chain nonce"
                    );
                }
            }
        }

        info!(total_confirmed, total_pending, "Inclusion verification complete");
    }

    if failure > 0 {
        error!(failed = failure, "Some transactions failed");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOADGEN_ADDRESS_FIXTURES: &[(u8, &str)] = &[
        (1, "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"),
        (2, "0x2B5AD5c4795c026514f8317c7a215E218DcCD6cF"),
        (3, "0x6813Eb9362372EEF6200f3b1dbC3f819671cBA69"),
    ];

    #[test]
    fn account_addresses_match_seed_fixtures() {
        for &(seed, expected) in LOADGEN_ADDRESS_FIXTURES {
            let account = Account::new(seed);
            assert_eq!(account.address.to_string(), expected);
        }
    }

    #[test]
    fn loadgen_seeds_accepts_supported_range() {
        assert_eq!(loadgen_seeds(1).unwrap(), vec![1]);
        assert_eq!(loadgen_seeds(3).unwrap(), vec![1, 2, 3]);

        let seeds = loadgen_seeds(255).unwrap();
        assert_eq!(seeds.len(), 255);
        assert_eq!(seeds.first(), Some(&1));
        assert_eq!(seeds.last(), Some(&255));
    }

    #[test]
    fn loadgen_seeds_rejects_unsupported_counts() {
        for accounts in [0, 256, usize::MAX] {
            let error = loadgen_seeds(accounts).unwrap_err().to_string();
            assert!(error.contains("between 1 and 255"));
            assert!(error.contains(&accounts.to_string()));
        }
    }

    #[test]
    fn parse_json_rpc_quantity_accepts_hex_quantities() {
        assert_eq!(parse_json_rpc_quantity("0x0").unwrap(), 0);
        assert_eq!(parse_json_rpc_quantity("0xa").unwrap(), 10);
        assert_eq!(parse_json_rpc_quantity("0x10").unwrap(), 16);
        assert_eq!(parse_json_rpc_quantity("0xFF").unwrap(), 255);
    }

    #[test]
    fn parse_json_rpc_quantity_rejects_invalid_quantities() {
        for quantity in ["", "10", "0x", "0xzz"] {
            assert!(parse_json_rpc_quantity(quantity).is_err());
        }
    }

    #[test]
    fn sign_eip1559_transfer_produces_valid_envelope() {
        let account = Account::new(1);
        let to = Address::repeat_byte(0xBB);
        let raw =
            sign_eip1559_transfer(&account.key, 1337, to, U256::from(1), 0, TRANSFER_GAS_LIMIT);
        // EIP-2718 type-2 envelope starts with 0x02
        assert!(!raw.is_empty());
        assert_eq!(raw[0], 0x02, "expected EIP-1559 type prefix");
    }

    #[test]
    fn retry_backoff_is_exponential() {
        let delays: Vec<Duration> =
            (1..=5).map(|attempt| RETRY_BASE_DELAY * 2u32.saturating_pow(attempt - 1)).collect();
        assert_eq!(delays[0], Duration::from_millis(100));
        assert_eq!(delays[1], Duration::from_millis(200));
        assert_eq!(delays[2], Duration::from_millis(400));
        assert_eq!(delays[3], Duration::from_millis(800));
        assert_eq!(delays[4], Duration::from_millis(1600));
    }

    #[test]
    fn nonce_increments_sequentially() {
        let account = Account::new(1);
        assert_eq!(account.next_nonce(), 0);
        assert_eq!(account.next_nonce(), 1);
        assert_eq!(account.next_nonce(), 2);
        account.set_nonce(42);
        assert_eq!(account.next_nonce(), 42);
    }

    #[test]
    fn is_transport_error_classifies_correctly() {
        // Transport errors should return true
        assert!(is_transport_error("error sending request for url"));
        assert!(is_transport_error("Connection refused (os error 111)"));
        assert!(is_transport_error("connection refused"));
        assert!(is_transport_error("request timed out"));
        assert!(is_transport_error("connection closed before message completed"));
        assert!(is_transport_error("broken pipe"));
        assert!(is_transport_error("reset by peer"));

        // Semantic errors should return false
        assert!(!is_transport_error("RPC error: nonce too low"));
        assert!(!is_transport_error("RPC error: nonce gap: got 339, expected 57"));
        assert!(!is_transport_error("nonce 42 already in pool for sender 0x1234"));
        assert!(!is_transport_error("transaction rejected by mempool"));
    }
}
