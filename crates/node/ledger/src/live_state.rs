//! Live state adapter for RPC.
//!
//! Wraps [`LedgerService`] to implement [`StateDbRead`] against the latest
//! in-memory overlay state rather than the persisted QMDB checkpoint.
//!
//! Without this, RPC state queries (balance, nonce, code, storage) read from
//! the QMDB persisted store which can lag up to 256 blocks behind the current
//! head. By delegating every read through [`LedgerService::latest_state()`],
//! queries always reflect the most recently executed block.

use alloy_primitives::{Address, B256, Bytes, U256};
use kora_traits::{StateDbError, StateDbRead};

use crate::LedgerService;

/// A [`StateDbRead`] implementation backed by the live overlay state.
///
/// On every read, this adapter fetches the latest overlay from the ledger
/// (which includes all in-memory changes since the last QMDB checkpoint)
/// and queries it. This ensures RPC responses reflect the most recent
/// executed block rather than a potentially stale persisted snapshot.
#[derive(Clone, Debug)]
pub struct LiveState {
    ledger: LedgerService,
}

impl LiveState {
    /// Create a new live state adapter from a ledger service handle.
    #[must_use]
    pub const fn new(ledger: LedgerService) -> Self {
        Self { ledger }
    }
}

impl StateDbRead for LiveState {
    fn nonce(
        &self,
        address: &Address,
    ) -> impl std::future::Future<Output = Result<u64, StateDbError>> + Send {
        let ledger = self.ledger.clone();
        let address = *address;
        async move {
            let state = ledger.latest_state().await;
            state.nonce(&address).await
        }
    }

    fn balance(
        &self,
        address: &Address,
    ) -> impl std::future::Future<Output = Result<U256, StateDbError>> + Send {
        let ledger = self.ledger.clone();
        let address = *address;
        async move {
            let state = ledger.latest_state().await;
            state.balance(&address).await
        }
    }

    fn code_hash(
        &self,
        address: &Address,
    ) -> impl std::future::Future<Output = Result<B256, StateDbError>> + Send {
        let ledger = self.ledger.clone();
        let address = *address;
        async move {
            let state = ledger.latest_state().await;
            state.code_hash(&address).await
        }
    }

    fn code(
        &self,
        code_hash: &B256,
    ) -> impl std::future::Future<Output = Result<Bytes, StateDbError>> + Send {
        let ledger = self.ledger.clone();
        let code_hash = *code_hash;
        async move {
            let state = ledger.latest_state().await;
            state.code(&code_hash).await
        }
    }

    fn storage(
        &self,
        address: &Address,
        slot: &U256,
    ) -> impl std::future::Future<Output = Result<U256, StateDbError>> + Send {
        let ledger = self.ledger.clone();
        let address = *address;
        let slot = *slot;
        async move {
            let state = ledger.latest_state().await;
            state.storage(&address, &slot).await
        }
    }
}
