//! Boostrap configuration.

use std::{path::Path, str::FromStr};

use alloy_evm::revm::primitives::{Address, U256};
use serde::{Deserialize, Serialize};

use crate::Tx;

/// Bootstrap configuration for genesis state and initial transactions.
#[derive(Clone, Debug)]
pub struct BootstrapConfig {
    /// Initial account allocations (address, balance) for genesis.
    pub genesis_alloc: Vec<(Address, U256)>,
    /// Transactions to execute during bootstrap.
    pub bootstrap_txs: Vec<Tx>,
    /// Genesis block Unix timestamp, in seconds.
    pub genesis_timestamp: u64,
}

#[derive(Serialize, Deserialize)]
struct GenesisJson {
    chain_id: u64,
    timestamp: u64,
    allocations: Vec<AllocationJson>,
}

#[derive(Serialize, Deserialize)]
struct AllocationJson {
    address: String,
    balance: String,
}

impl BootstrapConfig {
    /// Create a new bootstrap configuration.
    #[must_use]
    pub const fn new(genesis_alloc: Vec<(Address, U256)>, bootstrap_txs: Vec<Tx>) -> Self {
        Self { genesis_alloc, bootstrap_txs, genesis_timestamp: 0 }
    }

    /// Set the genesis block timestamp.
    #[must_use]
    pub const fn with_genesis_timestamp(mut self, genesis_timestamp: u64) -> Self {
        self.genesis_timestamp = genesis_timestamp;
        self
    }

    /// Load bootstrap configuration from a genesis JSON file.
    pub fn load(genesis_path: &Path) -> Result<Self, BootstrapError> {
        let content = std::fs::read_to_string(genesis_path)?;
        let genesis: GenesisJson = serde_json::from_str(&content)?;
        let GenesisJson { timestamp, allocations, .. } = genesis;

        let mut genesis_alloc = Vec::with_capacity(allocations.len());
        for alloc in allocations {
            let address = Address::from_str(&alloc.address)
                .map_err(|e| BootstrapError::Parse(format!("invalid address: {}", e)))?;
            let balance = U256::from_str(&alloc.balance)
                .map_err(|e| BootstrapError::Parse(format!("invalid balance: {}", e)))?;
            genesis_alloc.push((address, balance));
        }

        Ok(Self { genesis_alloc, bootstrap_txs: Vec::new(), genesis_timestamp: timestamp })
    }
}

/// Errors that can occur during bootstrap configuration loading.
#[derive(Debug)]
pub enum BootstrapError {
    /// IO error reading the genesis file.
    Io(std::io::Error),
    /// JSON parsing error.
    Json(serde_json::Error),
    /// Error parsing address or balance values.
    Parse(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {}", e),
            Self::Json(e) => write!(f, "json error: {}", e),
            Self::Parse(e) => write!(f, "parse error: {}", e),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<std::io::Error> for BootstrapError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for BootstrapError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::*;

    fn temp_genesis_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "kora-genesis-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ))
    }

    #[test]
    fn new_defaults_genesis_timestamp_to_zero() {
        let bootstrap = BootstrapConfig::new(Vec::new(), Vec::new());
        assert_eq!(bootstrap.genesis_timestamp, 0);
    }

    #[test]
    fn load_preserves_genesis_timestamp() {
        let path = temp_genesis_path();
        let json = r#"{
            "chain_id": 1337,
            "timestamp": 1700000000,
            "allocations": [
                {
                    "address": "0x0000000000000000000000000000000000000001",
                    "balance": "42"
                }
            ]
        }"#;

        fs::write(&path, json).expect("write genesis");
        let bootstrap = BootstrapConfig::load(&path).expect("load genesis");
        fs::remove_file(path).expect("remove genesis");

        assert_eq!(bootstrap.genesis_timestamp, 1_700_000_000);
        assert_eq!(bootstrap.genesis_alloc.len(), 1);
    }
}
