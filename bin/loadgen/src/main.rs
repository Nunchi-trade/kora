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
use futures::stream::{FuturesUnordered, StreamExt};
use k256::ecdsa::SigningKey;
use sha3::{Digest as _, Keccak256};
use tracing::{error, info, warn};

const MIN_LOADGEN_ACCOUNTS: usize = 1;
const MAX_LOADGEN_ACCOUNTS: usize = u8::MAX as usize;

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
}

/// Account with signing key and nonce tracker.
struct Account {
    key: SigningKey,
    address: Address,
    nonce: AtomicU64,
}

impl Account {
    fn new(seed: u8) -> Self {
        let mut secret = [0u8; 32];
        secret[31] = seed;
        let key = SigningKey::from_bytes((&secret).into()).expect("valid key");
        let address = address_from_key(&key);
        Self { key, nonce: AtomicU64::new(0), address }
    }

    fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::Relaxed)
    }

    fn set_nonce(&self, nonce: u64) {
        self.nonce.store(nonce, Ordering::Relaxed);
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

/// HTTP client for RPC calls.
#[derive(Clone)]
struct RpcClient {
    client: reqwest::Client,
    url: String,
}

impl RpcClient {
    fn new(url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(100)
            .build()
            .expect("build http client");
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

async fn send_raw_transaction_to_any(clients: &[RpcClient], raw_tx: Bytes) -> Result<String> {
    let mut sends = FuturesUnordered::new();

    for client in clients {
        let client = client.clone();
        let tx = raw_tx.clone();
        sends.push(async move { client.send_raw_transaction(&tx).await });
    }

    let mut first_hash = None;
    let mut errors = Vec::new();

    while let Some(result) = sends.next().await {
        match result {
            Ok(hash) => {
                first_hash.get_or_insert(hash);
            }
            Err(error) => errors.push(error.to_string()),
        }
    }

    if let Some(hash) = first_hash {
        Ok(hash)
    } else {
        eyre::bail!("all RPC endpoints rejected transaction: {}", errors.join("; "))
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
    let gas_limit = 21_000u64;

    let clients: Arc<Vec<RpcClient>> = Arc::new(rpc_urls.into_iter().map(RpcClient::new).collect());

    if !args.dry_run {
        for account in &accounts {
            let nonce = clients[0].get_transaction_count(account.address).await?;
            account.set_nonce(nonce);
        }
    }

    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

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
                gas_limit,
            );
            success_count.fetch_add(1, Ordering::Relaxed);
            if (i + 1) % 1000 == 0 {
                info!(tx = i + 1, "Dry run progress");
            }
        }
    } else {
        let mut futures = FuturesUnordered::new();

        for i in 0..args.total_txs {
            let account = accounts[i as usize % accounts.len()].clone();
            let clients = clients.clone();
            let success = success_count.clone();
            let failure = failure_count.clone();
            let verbose = args.verbose;

            let nonce = account.next_nonce();
            let tx = sign_eip1559_transfer(
                &account.key,
                args.chain_id,
                receiver,
                transfer_amount,
                nonce,
                gas_limit,
            );

            let fut = async move {
                match send_raw_transaction_to_any(&clients, tx).await {
                    Ok(hash) => {
                        success.fetch_add(1, Ordering::Relaxed);
                        if verbose {
                            info!(nonce, hash = %hash, "tx sent");
                        }
                    }
                    Err(e) => {
                        failure.fetch_add(1, Ordering::Relaxed);
                        warn!(nonce, error = %e, "tx failed");
                    }
                }
            };

            futures.push(fut);

            // Limit concurrency by waiting when we hit the limit
            if futures.len() >= args.concurrency {
                futures.next().await;
            }
        }

        // Drain remaining futures
        while futures.next().await.is_some() {}
    }

    let elapsed = start.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let failure = failure_count.load(Ordering::Relaxed);
    let tps =
        if elapsed.as_secs_f64() > 0.0 { success as f64 / elapsed.as_secs_f64() } else { 0.0 };

    info!(
        sent = success + failure,
        success,
        failed = failure,
        elapsed_secs = format!("{:.2}", elapsed.as_secs_f64()),
        tps = format!("{:.2}", tps),
        "Load generation complete"
    );

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
}
