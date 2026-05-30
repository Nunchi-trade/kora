//! State database adapter for REVM.
//!
//! Note: REVM's `DatabaseRef` trait is synchronous, so we bridge async StateDb traits into
//! the sync REVM interface.
//!
//! Callers are expected to run the entire EVM execution inside
//! `tokio::task::spawn_blocking` so that async worker threads remain free for
//! consensus, networking, and RPC.  Inside a `spawn_blocking` thread,
//! `block_in_place` is a no-op (tokio 1.28+) and `Handle::block_on` drives
//! the state DB futures without starving any async workers.

use std::collections::HashMap;

use alloy_primitives::{Address, B256, KECCAK256_EMPTY, U256};
use kora_traits::{StateDbError, StateDbRead};
use revm::{bytecode::Bytecode, database_interface::DatabaseRef, state::AccountInfo};
use tokio::runtime::RuntimeFlavor;

use crate::ExecutionError;

/// Wrapper for blocking async operations in sync contexts.
///
/// When a tokio multi-thread runtime is available (the normal production
/// case -- either from a `spawn_blocking` thread or an async worker),
/// `block_in_place` + `handle.block_on` is used.  On a `spawn_blocking`
/// thread (the expected production path), `block_in_place` is a no-op
/// (tokio >= 1.28) and `handle.block_on` safely drives the future without
/// starving async workers.
///
/// When no tokio runtime is present (e.g. synchronous unit tests), we fall
/// back to `futures::executor::block_on`.
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
        // Issue all three reads concurrently within a single block_on call.
        // The underlying StateDb may serve these from different shards or
        // cache lines, so overlapping the I/O (via tokio::join!) is
        // significantly faster than the previous sequential approach.
        match block_on(async {
            let (nonce, balance, code_hash) = tokio::join!(
                self.state.nonce(&address),
                self.state.balance(&address),
                self.state.code_hash(&address),
            );
            Ok::<_, StateDbError>((nonce?, balance?, code_hash?))
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use alloy_primitives::Bytes;
    use kora_traits::StateDbError;

    use super::*;

    /// Minimal mock that satisfies `StateDbRead` for tests that only exercise
    /// the block-hash lookup path and never actually call the state methods.
    #[derive(Clone)]
    struct NoopState;

    impl StateDbRead for NoopState {
        async fn nonce(&self, _: &Address) -> Result<u64, StateDbError> {
            Ok(0)
        }

        async fn balance(&self, _: &Address) -> Result<U256, StateDbError> {
            Ok(U256::ZERO)
        }

        async fn code_hash(&self, _: &Address) -> Result<B256, StateDbError> {
            Ok(B256::ZERO)
        }

        async fn code(&self, _: &B256) -> Result<Bytes, StateDbError> {
            Ok(Bytes::new())
        }

        async fn storage(&self, _: &Address, _: &U256) -> Result<U256, StateDbError> {
            Ok(U256::ZERO)
        }
    }

    #[derive(Clone, Default)]
    struct ConcurrentState {
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl ConcurrentState {
        async fn observe<T>(&self, value: T) -> Result<T, StateDbError> {
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(current, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(value)
        }
    }

    impl StateDbRead for ConcurrentState {
        async fn nonce(&self, _: &Address) -> Result<u64, StateDbError> {
            self.observe(7).await
        }

        async fn balance(&self, _: &Address) -> Result<U256, StateDbError> {
            self.observe(U256::from(11)).await
        }

        async fn code_hash(&self, _: &Address) -> Result<B256, StateDbError> {
            self.observe(B256::repeat_byte(0x22)).await
        }

        async fn code(&self, _: &B256) -> Result<Bytes, StateDbError> {
            Ok(Bytes::new())
        }

        async fn storage(&self, _: &Address, _: &U256) -> Result<U256, StateDbError> {
            Ok(U256::ZERO)
        }
    }

    #[test]
    fn adapter_new() {
        let adapter = StateDbAdapter::new(NoopState, HashMap::new());
        // Verify the adapter is created successfully; state() returns a reference.
        let _ = adapter.state();
    }

    #[test]
    fn basic_ref_overlaps_account_field_reads_on_multithread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime should build");

        runtime.block_on(async {
            let state = ConcurrentState::default();
            let adapter = StateDbAdapter::new(state.clone(), HashMap::new());

            let account = tokio::task::spawn_blocking(move || {
                DatabaseRef::basic_ref(&adapter, Address::repeat_byte(0x33))
            })
            .await
            .expect("blocking task should complete")
            .expect("state read should succeed")
            .expect("account should exist");

            assert_eq!(account.nonce, 7);
            assert_eq!(account.balance, U256::from(11));
            assert_eq!(account.code_hash, B256::repeat_byte(0x22));
            assert_eq!(state.max_in_flight.load(Ordering::SeqCst), 3);
        });
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
