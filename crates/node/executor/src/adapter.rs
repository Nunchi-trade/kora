//! State database adapter for REVM.
//!
//! Note: REVM's `DatabaseRef` trait is synchronous, so we bridge async StateDb traits into
//! the sync REVM interface. When executing inside a Tokio runtime, we use `block_in_place`
//! so async storage can continue making progress on runtime workers.

use std::collections::HashMap;

use alloy_primitives::{Address, B256, KECCAK256_EMPTY, U256};
use kora_traits::{StateDbError, StateDbRead};
use revm::{bytecode::Bytecode, database_interface::DatabaseRef, state::AccountInfo};
use tokio::runtime::RuntimeFlavor;

use crate::ExecutionError;

/// Wrapper for blocking async operations in sync contexts.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == RuntimeFlavor::MultiThread
    {
        return tokio::task::block_in_place(|| handle.block_on(f));
    }

    futures::executor::block_on(f)
}

/// Adapts a [`StateDbRead`] to REVM's [`DatabaseRef`] interface.
#[derive(Clone, Debug)]
pub struct StateDbAdapter<S> {
    state: S,
    /// Recent block hashes keyed by block number, used by the BLOCKHASH opcode.
    block_hashes: HashMap<u64, B256>,
}

impl<S> StateDbAdapter<S> {
    /// Create a new adapter wrapping the given state and recent block hashes.
    #[must_use]
    pub const fn new(state: S, block_hashes: HashMap<u64, B256>) -> Self {
        Self { state, block_hashes }
    }

    /// Get the underlying state reference.
    #[must_use]
    pub const fn state(&self) -> &S {
        &self.state
    }
}

impl<S: StateDbRead> DatabaseRef for StateDbAdapter<S> {
    type Error = ExecutionError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // Batch all three reads into a single block_on call to reduce the
        // overhead of the async-to-sync bridge (block_in_place + handle.block_on).
        match block_on(async {
            let nonce = self.state.nonce(&address).await?;
            let balance = self.state.balance(&address).await?;
            let code_hash = self.state.code_hash(&address).await?;
            Ok::<_, StateDbError>((nonce, balance, code_hash))
        }) {
            Ok((nonce, balance, code_hash)) => {
                Ok(Some(AccountInfo { nonce, balance, code_hash, code: None, account_id: None }))
            }
            Err(StateDbError::AccountNotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == KECCAK256_EMPTY || code_hash == B256::ZERO {
            return Ok(Bytecode::default());
        }
        let bytes = block_on(self.state.code(&code_hash))?;
        Ok(Bytecode::new_raw(bytes))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        match block_on(self.state.storage(&address, &index)) {
            Ok(value) => Ok(value),
            Err(StateDbError::AccountNotFound(_)) => Ok(U256::ZERO),
            Err(e) => Err(e.into()),
        }
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.block_hashes.get(&number).copied().unwrap_or(B256::ZERO))
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Bytes;
    use kora_traits::StateDbError;

    use super::*;

    /// Minimal mock that satisfies `StateDbRead` for tests that only exercise
    /// the block-hash lookup path and never actually call the state methods.
    #[derive(Clone)]
    struct NoopState;

    impl StateDbRead for NoopState {
        fn nonce(
            &self,
            _: &Address,
        ) -> impl std::future::Future<Output = Result<u64, StateDbError>> + Send {
            async { Ok(0) }
        }

        fn balance(
            &self,
            _: &Address,
        ) -> impl std::future::Future<Output = Result<U256, StateDbError>> + Send {
            async { Ok(U256::ZERO) }
        }

        fn code_hash(
            &self,
            _: &Address,
        ) -> impl std::future::Future<Output = Result<B256, StateDbError>> + Send {
            async { Ok(B256::ZERO) }
        }

        fn code(
            &self,
            _: &B256,
        ) -> impl std::future::Future<Output = Result<Bytes, StateDbError>> + Send {
            async { Ok(Bytes::new()) }
        }

        fn storage(
            &self,
            _: &Address,
            _: &U256,
        ) -> impl std::future::Future<Output = Result<U256, StateDbError>> + Send {
            async { Ok(U256::ZERO) }
        }
    }

    #[test]
    fn adapter_new() {
        let adapter = StateDbAdapter::new(NoopState, HashMap::new());
        // Verify the adapter is created successfully; state() returns a reference.
        let _ = adapter.state();
    }

    #[test]
    fn block_hash_ref_returns_known_hash() {
        let mut hashes = HashMap::new();
        let expected = B256::repeat_byte(0xab);
        hashes.insert(42, expected);
        let adapter = StateDbAdapter::new(NoopState, hashes);

        let result = DatabaseRef::block_hash_ref(&adapter, 42).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn block_hash_ref_returns_zero_for_unknown() {
        let adapter = StateDbAdapter::new(NoopState, HashMap::new());

        let result = DatabaseRef::block_hash_ref(&adapter, 999).unwrap();
        assert_eq!(result, B256::ZERO);
    }

    #[test]
    fn block_hash_ref_multiple_entries() {
        let mut hashes = HashMap::new();
        let hash_10 = B256::repeat_byte(0x10);
        let hash_11 = B256::repeat_byte(0x11);
        let hash_12 = B256::repeat_byte(0x12);
        hashes.insert(10, hash_10);
        hashes.insert(11, hash_11);
        hashes.insert(12, hash_12);
        let adapter = StateDbAdapter::new(NoopState, hashes);

        assert_eq!(DatabaseRef::block_hash_ref(&adapter, 10).unwrap(), hash_10);
        assert_eq!(DatabaseRef::block_hash_ref(&adapter, 11).unwrap(), hash_11);
        assert_eq!(DatabaseRef::block_hash_ref(&adapter, 12).unwrap(), hash_12);
        assert_eq!(DatabaseRef::block_hash_ref(&adapter, 13).unwrap(), B256::ZERO);
    }
}
