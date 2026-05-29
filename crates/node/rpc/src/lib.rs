//! RPC server for Kora nodes.

#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/refcell/kora/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod config;
pub use config::{CorsConfig, RateLimitConfig, RpcServerConfig};

mod error;
pub use error::{RpcError, codes as error_codes};

mod eth;
pub use eth::{
    EthApiImpl, EthApiServer, FeeHistory, GasOracleConfig, NetApiImpl, NetApiServer,
    TxSubmitCallback, TxSubmitFuture, Web3ApiImpl, Web3ApiServer,
};

mod filters;
pub use filters::FilterChanges;

mod kora;
pub use kora::{KoraApiImpl, KoraApiServer};

mod txpool;
pub use txpool::{TxpoolApiImpl, TxpoolApiServer, TxpoolContent, TxpoolInspect, TxpoolStatus};

mod server;
pub use server::{JsonRpcServer, RpcServer, RpcServerHandle, ServerError};

mod subscription;
pub use subscription::{
    MEMPOOL_EVENT_CHANNEL_CAPACITY, MempoolEventSender, PENDING_TX_CHANNEL_CAPACITY,
    PendingTxEvent, PendingTxEventSender, PendingTxInfo, mempool_event_channel, pending_tx_channel,
};

mod state;
pub use state::{NodeState, NodeStatus, PartitionStatus};

mod state_provider;
pub use state_provider::{NoopStateProvider, StateProvider};

mod indexed_provider;
pub use indexed_provider::IndexedStateProvider;

mod types;
pub use types::{
    AddressFilter, BlockNumberOrTag, BlockTag, BlockTransactions, CallRequest, RpcBlock, RpcLog,
    RpcLogFilter, RpcTransaction, RpcTransactionReceipt, TopicFilter,
};
