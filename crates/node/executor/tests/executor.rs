//! Integration tests for kora-executor.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use alloy_consensus::{Header, SignableTransaction as _, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, Signature, TxKind, U256, keccak256};
use k256::ecdsa::SigningKey;
use kora_executor::{BlockContext, BlockExecutor, RevmExecutor};
use kora_qmdb::{AccountUpdate, ChangeSet};
use kora_traits::{StateDb, StateDbError, StateDbRead, StateDbWrite};
use rstest::rstest;
use sha3::{Digest as _, Keccak256};

/// Account data stored in the mock state database.
#[derive(Clone, Debug, Default)]
struct MockAccount {
    nonce: u64,
    balance: U256,
    code_hash: B256,
    storage: HashMap<U256, U256>,
}

/// Mock state database for testing.
///
/// Stores account state in memory using a HashMap.
#[derive(Clone, Debug, Default)]
struct MockStateDb {
    /// Accounts indexed by address.
    accounts: Arc<RwLock<HashMap<Address, MockAccount>>>,
    /// Contract code indexed by code hash.
    code: Arc<RwLock<HashMap<B256, Bytes>>>,
    /// Current state root.
    state_root: Arc<RwLock<B256>>,
}

impl MockStateDb {
    /// Create a new empty mock state database.
    fn new() -> Self {
        Self::default()
    }

    /// Insert an account into the database.
    fn insert_account(&self, address: Address, account: MockAccount) {
        self.accounts.write().unwrap().insert(address, account);
    }

    /// Insert code into the database.
    fn insert_code(&self, code_hash: B256, code: Bytes) {
        self.code.write().unwrap().insert(code_hash, code);
    }
}

impl StateDbRead for MockStateDb {
    async fn nonce(&self, address: &Address) -> Result<u64, StateDbError> {
        self.accounts
            .read()
            .unwrap()
            .get(address)
            .map(|acc| acc.nonce)
            .ok_or(StateDbError::AccountNotFound(*address))
    }

    async fn balance(&self, address: &Address) -> Result<U256, StateDbError> {
        self.accounts
            .read()
            .unwrap()
            .get(address)
            .map(|acc| acc.balance)
            .ok_or(StateDbError::AccountNotFound(*address))
    }

    async fn code_hash(&self, address: &Address) -> Result<B256, StateDbError> {
        self.accounts
            .read()
            .unwrap()
            .get(address)
            .map(|acc| acc.code_hash)
            .ok_or(StateDbError::AccountNotFound(*address))
    }

    async fn code(&self, code_hash: &B256) -> Result<Bytes, StateDbError> {
        self.code
            .read()
            .unwrap()
            .get(code_hash)
            .cloned()
            .ok_or(StateDbError::CodeNotFound(*code_hash))
    }

    async fn storage(&self, address: &Address, slot: &U256) -> Result<U256, StateDbError> {
        let accounts = self.accounts.read().unwrap();
        Ok(accounts
            .get(address)
            .and_then(|acc| acc.storage.get(slot).copied())
            .unwrap_or(U256::ZERO))
    }
}

impl StateDbWrite for MockStateDb {
    async fn commit(&self, changes: ChangeSet) -> Result<B256, StateDbError> {
        let mut accounts = self.accounts.write().unwrap();
        let mut code_store = self.code.write().unwrap();

        for (address, update) in changes.accounts {
            if update.selfdestructed {
                accounts.remove(&address);
                continue;
            }

            let account = accounts.entry(address).or_default();
            account.nonce = update.nonce;
            account.balance = update.balance;
            account.code_hash = update.code_hash;

            if let Some(code) = update.code {
                code_store.insert(update.code_hash, Bytes::from(code));
            }

            for (slot, value) in update.storage {
                if value.is_zero() {
                    account.storage.remove(&slot);
                } else {
                    account.storage.insert(slot, value);
                }
            }
        }

        // Generate a simple state root from account count.
        let root = B256::from_slice(&[accounts.len() as u8; 32]);
        *self.state_root.write().unwrap() = root;
        Ok(root)
    }

    async fn compute_root(&self, _changes: &ChangeSet) -> Result<B256, StateDbError> {
        // Simplified: just return current state root.
        Ok(*self.state_root.read().unwrap())
    }

    fn merge_changes(&self, mut older: ChangeSet, newer: ChangeSet) -> ChangeSet {
        older.merge(newer);
        older
    }
}

impl StateDb for MockStateDb {
    async fn state_root(&self) -> Result<B256, StateDbError> {
        Ok(*self.state_root.read().unwrap())
    }
}

// ----------------------------------------------------------------------------
// Tests for RevmExecutor creation with different chain IDs
// ----------------------------------------------------------------------------

#[rstest]
#[case(1, "Ethereum Mainnet")]
#[case(11155111, "Sepolia")]
#[case(42161, "Arbitrum One")]
#[case(10, "Optimism")]
#[case(137, "Polygon")]
#[case(u64::MAX, "Max chain ID")]
fn test_revm_executor_chain_ids(#[case] chain_id: u64, #[case] _name: &str) {
    let executor = RevmExecutor::new(chain_id);
    assert_eq!(executor.chain_id(), chain_id);
}

#[test]
fn test_revm_executor_default_chain_id() {
    let executor = RevmExecutor::default();
    assert_eq!(executor.chain_id(), 1);
}

// ----------------------------------------------------------------------------
// Tests for execute with empty transaction list
// ----------------------------------------------------------------------------

#[test]
fn test_execute_empty_transactions_returns_empty_outcome() {
    let executor = RevmExecutor::new(1);
    let state = MockStateDb::new();
    let context = BlockContext::new(Header::default(), B256::ZERO, B256::ZERO);
    let txs: Vec<Bytes> = vec![];

    let outcome = executor.execute(&state, &context, &txs).expect("execution should succeed");

    assert!(outcome.changes.is_empty());
    assert!(outcome.receipts.is_empty());
    assert_eq!(outcome.gas_used, 0);
}

#[rstest]
#[case(1)]
#[case(137)]
#[case(42161)]
fn test_execute_empty_transactions_different_chains(#[case] chain_id: u64) {
    let executor = RevmExecutor::new(chain_id);
    let state = MockStateDb::new();
    let context = BlockContext::new(Header::default(), B256::ZERO, B256::ZERO);
    let txs: Vec<Bytes> = vec![];

    let outcome = executor.execute(&state, &context, &txs).expect("execution should succeed");

    assert!(outcome.changes.is_empty());
    assert!(outcome.receipts.is_empty());
    assert_eq!(outcome.gas_used, 0);
}

// ----------------------------------------------------------------------------
// Tests for validate_header
// ----------------------------------------------------------------------------

#[test]
fn test_validate_header_succeeds_with_valid_gas_limit() {
    let executor = RevmExecutor::new(1);
    let header = Header { gas_limit: 30_000_000, ..Default::default() };

    let result = <RevmExecutor as BlockExecutor<MockStateDb>>::validate_header(&executor, &header);

    assert!(result.is_ok());
}

#[test]
fn test_validate_header_fails_with_gas_limit_below_minimum() {
    let executor = RevmExecutor::new(1);
    let header = Header { gas_limit: 1000, ..Default::default() };

    let result = <RevmExecutor as BlockExecutor<MockStateDb>>::validate_header(&executor, &header);

    assert!(result.is_err());
}

#[rstest]
#[case(0, 30_000_000)]
#[case(1, 30_000_000)]
#[case(1_000_000, 30_000_000)]
#[case(u64::MAX, 30_000_000)]
fn test_validate_header_succeeds_with_various_block_numbers(
    #[case] number: u64,
    #[case] gas_limit: u64,
) {
    let executor = RevmExecutor::new(1);
    let header = Header { number, gas_limit, ..Default::default() };

    let result = <RevmExecutor as BlockExecutor<MockStateDb>>::validate_header(&executor, &header);

    assert!(result.is_ok());
}

// ----------------------------------------------------------------------------
// Tests for BlockContext creation and field access
// ----------------------------------------------------------------------------

#[test]
fn test_block_context_creation_with_defaults() {
    let header = Header::default();
    let parent_hash = B256::repeat_byte(1);
    let prevrandao = B256::ZERO;

    let context = BlockContext::new(header.clone(), parent_hash, prevrandao);

    assert_eq!(context.prevrandao, B256::ZERO);
    assert_eq!(context.parent_hash, parent_hash);
    assert_eq!(context.header.number, header.number);
}

#[test]
fn test_block_context_creation_with_custom_prevrandao() {
    let header = Header::default();
    let prevrandao = B256::from([0xAB; 32]);

    let context = BlockContext::new(header, B256::ZERO, prevrandao);

    assert_eq!(context.prevrandao, B256::from([0xAB; 32]));
}

#[rstest]
#[case(0, 0)]
#[case(1, 1000)]
#[case(100, 21000)]
#[case(u64::MAX, u64::MAX)]
fn test_block_context_with_various_header_values(#[case] number: u64, #[case] gas_limit: u64) {
    let header = Header { number, gas_limit, ..Default::default() };
    let prevrandao = B256::from([number as u8; 32]);

    let context = BlockContext::new(header, B256::ZERO, prevrandao);

    assert_eq!(context.header.number, number);
    assert_eq!(context.header.gas_limit, gas_limit);
    assert_eq!(context.prevrandao, prevrandao);
}

// ----------------------------------------------------------------------------
// Tests for MockStateDb (validates our test infrastructure)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_mock_state_db_account_not_found() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);

    let result = state.nonce(&address).await;

    assert!(matches!(result, Err(StateDbError::AccountNotFound(_))));
}

#[tokio::test]
async fn test_mock_state_db_insert_and_read_account() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    let account = MockAccount {
        nonce: 5,
        balance: U256::from(1000),
        code_hash: B256::ZERO,
        storage: HashMap::new(),
    };

    state.insert_account(address, account);

    assert_eq!(state.nonce(&address).await.unwrap(), 5);
    assert_eq!(state.balance(&address).await.unwrap(), U256::from(1000));
}

#[tokio::test]
async fn test_mock_state_db_storage_returns_zero_for_missing_slot() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    let account = MockAccount::default();
    state.insert_account(address, account);

    let slot = U256::from(42);
    let value = state.storage(&address, &slot).await.unwrap();

    assert_eq!(value, U256::ZERO);
}

#[tokio::test]
async fn test_mock_state_db_storage_returns_zero_for_missing_account() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    let slot = U256::from(42);

    let value = state.storage(&address, &slot).await.unwrap();

    assert_eq!(value, U256::ZERO);
}

#[tokio::test]
async fn test_mock_state_db_storage_returns_value_for_existing_slot() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    let mut storage = HashMap::new();
    storage.insert(U256::from(42), U256::from(999));
    let account = MockAccount { storage, ..Default::default() };
    state.insert_account(address, account);

    let value = state.storage(&address, &U256::from(42)).await.unwrap();

    assert_eq!(value, U256::from(999));
}

#[tokio::test]
async fn test_mock_state_db_commit_stores_changes() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);

    let mut changes = ChangeSet::new();
    changes.insert(
        address,
        AccountUpdate {
            created: true,
            selfdestructed: false,
            nonce: 10,
            balance: U256::from(5000),
            code_hash: B256::ZERO,
            code: None,
            storage: std::collections::BTreeMap::new(),
        },
    );

    let root = state.commit(changes).await.unwrap();

    assert_ne!(root, B256::ZERO);
    assert_eq!(state.nonce(&address).await.unwrap(), 10);
    assert_eq!(state.balance(&address).await.unwrap(), U256::from(5000));
}

#[tokio::test]
async fn test_mock_state_db_commit_handles_selfdestruct() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);

    // First create the account.
    state.insert_account(
        address,
        MockAccount { nonce: 5, balance: U256::from(1000), ..Default::default() },
    );

    // Then selfdestruct it.
    let mut changes = ChangeSet::new();
    changes.insert(
        address,
        AccountUpdate {
            created: false,
            selfdestructed: true,
            nonce: 0,
            balance: U256::ZERO,
            code_hash: B256::ZERO,
            code: None,
            storage: std::collections::BTreeMap::new(),
        },
    );

    state.commit(changes).await.unwrap();

    assert!(matches!(state.nonce(&address).await, Err(StateDbError::AccountNotFound(_))));
}

#[tokio::test]
async fn test_mock_state_db_commit_stores_code() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    let code_hash = B256::from([0xCC; 32]);
    let code = vec![0x60, 0x00, 0x60, 0x00];

    let mut changes = ChangeSet::new();
    changes.insert(
        address,
        AccountUpdate {
            created: true,
            selfdestructed: false,
            nonce: 0,
            balance: U256::ZERO,
            code_hash,
            code: Some(code.clone()),
            storage: std::collections::BTreeMap::new(),
        },
    );

    state.commit(changes).await.unwrap();

    assert_eq!(state.code(&code_hash).await.unwrap(), Bytes::from(code));
}

#[tokio::test]
async fn test_mock_state_db_code_not_found() {
    let state = MockStateDb::new();
    let code_hash = B256::from([0xCC; 32]);

    let result = state.code(&code_hash).await;

    assert!(matches!(result, Err(StateDbError::CodeNotFound(_))));
}

#[tokio::test]
async fn test_mock_state_db_insert_code() {
    let state = MockStateDb::new();
    let code_hash = B256::from([0xCC; 32]);
    let code = Bytes::from(vec![0x60, 0x00]);

    state.insert_code(code_hash, code.clone());

    assert_eq!(state.code(&code_hash).await.unwrap(), code);
}

#[tokio::test]
async fn test_mock_state_db_state_root() {
    let state = MockStateDb::new();

    let root = state.state_root().await.unwrap();

    assert_eq!(root, B256::ZERO);
}

#[test]
fn test_mock_state_db_merge_changes() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);

    let mut older = ChangeSet::new();
    older.insert(
        address,
        AccountUpdate {
            created: true,
            selfdestructed: false,
            nonce: 1,
            balance: U256::from(100),
            code_hash: B256::ZERO,
            code: None,
            storage: std::collections::BTreeMap::new(),
        },
    );

    let mut newer = ChangeSet::new();
    newer.insert(
        address,
        AccountUpdate {
            created: false,
            selfdestructed: false,
            nonce: 5,
            balance: U256::from(500),
            code_hash: B256::ZERO,
            code: None,
            storage: std::collections::BTreeMap::new(),
        },
    );

    let merged = state.merge_changes(older, newer);

    let update = merged.accounts.get(&address).unwrap();
    assert_eq!(update.nonce, 5);
    assert_eq!(update.balance, U256::from(500));
}

// ----------------------------------------------------------------------------
// Tests for exists() default implementation
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_mock_state_db_exists_returns_false_for_missing_account() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);

    assert!(!state.exists(&address).await.unwrap());
}

#[tokio::test]
async fn test_mock_state_db_exists_returns_true_for_account_with_nonce() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    state.insert_account(address, MockAccount { nonce: 1, ..Default::default() });

    assert!(state.exists(&address).await.unwrap());
}

#[tokio::test]
async fn test_mock_state_db_exists_returns_true_for_account_with_balance() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    state.insert_account(
        address,
        MockAccount { nonce: 0, balance: U256::from(1), ..Default::default() },
    );

    assert!(state.exists(&address).await.unwrap());
}

#[tokio::test]
async fn test_mock_state_db_exists_returns_false_for_empty_account() {
    let state = MockStateDb::new();
    let address = Address::from([0x01; 20]);
    state.insert_account(address, MockAccount::default());

    assert!(!state.exists(&address).await.unwrap());
}

// ----------------------------------------------------------------------------
// Tests for executor with populated state
// ----------------------------------------------------------------------------

#[test]
fn test_execute_with_populated_state() {
    let executor = RevmExecutor::new(1);
    let state = MockStateDb::new();

    // Populate some accounts.
    let alice = Address::from([0x01; 20]);
    let bob = Address::from([0x02; 20]);
    state.insert_account(
        alice,
        MockAccount { nonce: 1, balance: U256::from(1000), ..Default::default() },
    );
    state.insert_account(
        bob,
        MockAccount { nonce: 0, balance: U256::from(500), ..Default::default() },
    );

    let context = BlockContext::new(Header::default(), B256::ZERO, B256::ZERO);
    let txs: Vec<Bytes> = vec![];

    let outcome = executor.execute(&state, &context, &txs).expect("execution should succeed");

    // Empty transactions still produce empty outcome.
    assert!(outcome.changes.is_empty());
    assert!(outcome.receipts.is_empty());
    assert_eq!(outcome.gas_used, 0);
}

// ----------------------------------------------------------------------------
// Helpers for creating signed transactions
// ----------------------------------------------------------------------------

/// Create a signing key from a deterministic seed byte.
fn signing_key_from_seed(seed: u8) -> SigningKey {
    let mut secret = [0u8; 32];
    secret[31] = seed;
    SigningKey::from_bytes((&secret).into()).expect("valid key")
}

/// Derive an Ethereum address from a signing key.
fn address_from_key(key: &SigningKey) -> Address {
    let encoded = key.verifying_key().to_encoded_point(false);
    let pubkey = encoded.as_bytes();
    let hash = keccak256(&pubkey[1..]);
    Address::from_slice(&hash[12..])
}

/// Sign an EIP-1559 transfer and return the raw encoded bytes.
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

// ----------------------------------------------------------------------------
// Tests for block gas limit enforcement
// ----------------------------------------------------------------------------

#[test]
fn test_execute_enforces_block_gas_limit() {
    let chain_id = 1u64;
    let executor = RevmExecutor::new(chain_id);
    let state = MockStateDb::new();

    // Set up a sender with enough balance for transfers.
    let sender_key = signing_key_from_seed(1);
    let sender = address_from_key(&sender_key);
    let receiver = Address::from([0xBB; 20]);

    state.insert_account(
        sender,
        MockAccount { nonce: 0, balance: U256::from(10_000_000_000u64), ..Default::default() },
    );

    // Insert receiver as an existing (empty) account to ensure the 21_000 gas
    // assumption holds regardless of fork rules for new-account creation.
    state.insert_account(receiver, MockAccount::default());

    // Each basic transfer uses 21_000 gas.
    // Create 3 transactions, each requiring 21_000 gas.
    let tx1 = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 0, 21_000);
    let tx2 = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 1, 21_000);
    let tx3 = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 2, 21_000);

    // Set block gas limit to only fit 2 transactions (42_000).
    // The third transaction (cumulative would be 63_000 > 42_000) should be skipped.
    let header = Header { gas_limit: 42_000, number: 1, timestamp: 1000, ..Default::default() };
    let context = BlockContext::new(header, B256::ZERO, B256::ZERO);

    let outcome =
        executor.execute(&state, &context, &[tx1, tx2, tx3]).expect("execution should succeed");

    // Only 2 transactions should have been executed, and both should succeed.
    assert_eq!(
        outcome.receipts.len(),
        2,
        "only 2 of 3 transactions should execute within gas limit"
    );
    assert_eq!(outcome.included_tx_count, 2, "included count must match executed prefix");
    assert!(
        outcome.receipts.iter().all(|r| r.success()),
        "all executed transactions should succeed"
    );
    assert_eq!(outcome.gas_used, 42_000, "cumulative gas should equal 2 * 21_000");
}

#[test]
fn test_execute_within_gas_limit_processes_all_transactions() {
    let chain_id = 1u64;
    let executor = RevmExecutor::new(chain_id);
    let state = MockStateDb::new();

    let sender_key = signing_key_from_seed(1);
    let sender = address_from_key(&sender_key);
    let receiver = Address::from([0xBB; 20]);

    state.insert_account(
        sender,
        MockAccount { nonce: 0, balance: U256::from(10_000_000_000u64), ..Default::default() },
    );

    // Insert receiver as an existing (empty) account to ensure the 21_000 gas
    // assumption holds regardless of fork rules for new-account creation.
    state.insert_account(receiver, MockAccount::default());

    // Create 3 transactions, each requiring 21_000 gas.
    let tx1 = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 0, 21_000);
    let tx2 = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 1, 21_000);
    let tx3 = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 2, 21_000);

    // Set block gas limit high enough for all 3 transactions (63_000).
    let header = Header { gas_limit: 63_000, number: 1, timestamp: 1000, ..Default::default() };
    let context = BlockContext::new(header, B256::ZERO, B256::ZERO);

    let outcome =
        executor.execute(&state, &context, &[tx1, tx2, tx3]).expect("execution should succeed");

    // All 3 transactions should have been executed and all should succeed.
    assert_eq!(outcome.receipts.len(), 3, "all 3 transactions should execute within gas limit");
    assert_eq!(outcome.included_tx_count, 3, "included count must include every transaction");
    assert!(
        outcome.receipts.iter().all(|r| r.success()),
        "all executed transactions should succeed"
    );
    assert_eq!(outcome.gas_used, 63_000, "cumulative gas should equal 3 * 21_000");
}

#[test]
fn test_execute_single_tx_exceeding_block_gas_limit_produces_empty_outcome() {
    let chain_id = 1u64;
    let executor = RevmExecutor::new(chain_id);
    let state = MockStateDb::new();

    let sender_key = signing_key_from_seed(1);
    let sender = address_from_key(&sender_key);
    let receiver = Address::from([0xBB; 20]);

    state.insert_account(
        sender,
        MockAccount { nonce: 0, balance: U256::from(10_000_000_000u64), ..Default::default() },
    );

    // Insert receiver as an existing (empty) account to ensure the 21_000 gas
    // assumption holds regardless of fork rules for new-account creation.
    state.insert_account(receiver, MockAccount::default());

    // Transaction requires 21_000 gas but block limit is only 10_000.
    let tx = sign_eip1559_transfer(&sender_key, chain_id, receiver, U256::from(1), 0, 21_000);

    let header = Header { gas_limit: 10_000, number: 1, timestamp: 1000, ..Default::default() };
    let context = BlockContext::new(header, B256::ZERO, B256::ZERO);

    let outcome = executor.execute(&state, &context, &[tx]).expect("execution should succeed");

    // The transaction should not have been executed.
    assert!(
        outcome.receipts.is_empty(),
        "no transactions should execute when gas limit is too low"
    );
    assert_eq!(outcome.included_tx_count, 0);
    assert_eq!(outcome.gas_used, 0);
}

// ----------------------------------------------------------------------------
// Tests for real signed EIP-1559 transaction execution with state changes
// ----------------------------------------------------------------------------

/// Execute a real signed EIP-1559 transfer and verify that:
/// - The transaction succeeds.
/// - The sender's nonce is incremented.
/// - The receiver's balance increases by the transfer value.
/// - The receipt contains the correct transaction hash.
/// - The total gas used equals the basic transfer cost (21,000).
#[test]
fn test_execute_signed_eip1559_transfer_verifies_state_changes() {
    let chain_id = 1u64;
    let executor = RevmExecutor::new(chain_id);
    let state = MockStateDb::new();

    let sender_key = signing_key_from_seed(1);
    let sender = address_from_key(&sender_key);
    let receiver = Address::from([0xBB; 20]);

    let initial_balance = U256::from(10_000_000_000u64);
    let transfer_value = U256::from(1_000);

    state.insert_account(
        sender,
        MockAccount { nonce: 0, balance: initial_balance, ..Default::default() },
    );
    // Insert receiver as existing (empty) account so the 21,000 gas assumption holds.
    state.insert_account(receiver, MockAccount::default());

    let tx_bytes =
        sign_eip1559_transfer(&sender_key, chain_id, receiver, transfer_value, 0, 21_000);
    let tx_hash = keccak256(&tx_bytes);

    let header = Header { gas_limit: 30_000_000, number: 1, timestamp: 1000, ..Default::default() };
    let context = BlockContext::new(header, B256::ZERO, B256::ZERO);

    let outcome =
        executor.execute(&state, &context, &[tx_bytes]).expect("execution should succeed");

    // Exactly one receipt produced.
    assert_eq!(outcome.receipts.len(), 1, "should produce exactly one receipt");

    // Transaction succeeded.
    assert!(outcome.receipts[0].success(), "transfer should succeed");

    // Receipt hash matches the transaction hash.
    assert_eq!(outcome.receipts[0].tx_hash, tx_hash, "receipt must contain correct tx hash");

    // Gas accounting: a simple transfer costs exactly 21,000 gas.
    assert_eq!(outcome.gas_used, 21_000, "total gas used should be 21,000");
    assert_eq!(outcome.receipts[0].gas_used, 21_000, "per-tx gas should be 21,000");

    // State changes must reflect the transfer.
    let sender_update =
        outcome.changes.accounts.get(&sender).expect("sender must appear in change set");
    assert_eq!(sender_update.nonce, 1, "sender nonce must increment to 1");
    assert_eq!(
        sender_update.balance,
        initial_balance - transfer_value,
        "sender balance must decrease by transfer value (zero base fee means no gas cost)"
    );

    let receiver_update =
        outcome.changes.accounts.get(&receiver).expect("receiver must appear in change set");
    assert_eq!(
        receiver_update.balance, transfer_value,
        "receiver balance must equal the transfer value"
    );
}

/// Execute two sequential signed EIP-1559 transfers from the same sender
/// and verify nonce increments and cumulative balance changes.
#[test]
fn test_execute_multiple_signed_transfers_sequential_nonces() {
    let chain_id = 1u64;
    let executor = RevmExecutor::new(chain_id);
    let state = MockStateDb::new();

    let sender_key = signing_key_from_seed(1);
    let sender = address_from_key(&sender_key);
    let receiver = Address::from([0xCC; 20]);

    let initial_balance = U256::from(10_000_000_000u64);
    let value_1 = U256::from(100);
    let value_2 = U256::from(200);

    state.insert_account(
        sender,
        MockAccount { nonce: 0, balance: initial_balance, ..Default::default() },
    );
    state.insert_account(receiver, MockAccount::default());

    let tx1 = sign_eip1559_transfer(&sender_key, chain_id, receiver, value_1, 0, 21_000);
    let tx2 = sign_eip1559_transfer(&sender_key, chain_id, receiver, value_2, 1, 21_000);

    let header = Header { gas_limit: 30_000_000, number: 1, timestamp: 1000, ..Default::default() };
    let context = BlockContext::new(header, B256::ZERO, B256::ZERO);

    let outcome =
        executor.execute(&state, &context, &[tx1, tx2]).expect("execution should succeed");

    // Both transactions should succeed.
    assert_eq!(outcome.receipts.len(), 2, "should produce two receipts");
    assert!(outcome.receipts[0].success(), "first transfer should succeed");
    assert!(outcome.receipts[1].success(), "second transfer should succeed");

    // Gas accounting.
    assert_eq!(outcome.gas_used, 42_000, "total gas should be 2 * 21,000");

    // Cumulative gas in receipts.
    assert_eq!(outcome.receipts[0].cumulative_gas_used(), 21_000);
    assert_eq!(outcome.receipts[1].cumulative_gas_used(), 42_000);

    // Final state changes reflect both transfers.
    let sender_update = outcome.changes.accounts.get(&sender).expect("sender in changes");
    assert_eq!(sender_update.nonce, 2, "sender nonce must be 2 after two transactions");
    assert_eq!(
        sender_update.balance,
        initial_balance - value_1 - value_2,
        "sender balance must decrease by total transferred (zero base fee)"
    );

    let receiver_update = outcome.changes.accounts.get(&receiver).expect("receiver in changes");
    assert_eq!(
        receiver_update.balance,
        value_1 + value_2,
        "receiver must have sum of both transfers"
    );
}
