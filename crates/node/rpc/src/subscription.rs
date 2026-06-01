//! JSON-RPC subscription support.

use alloy_primitives::B256;
use jsonrpsee::{
    RpcModule,
    server::SubscriptionMessage,
    types::{ErrorObjectOwned, Params},
};
use kora_domain::MempoolEvent;
use serde_json::Value;
use tokio::sync::broadcast::{self, error::RecvError};
use tracing::warn;

use crate::{
    error::codes,
    types::{RpcBlock, RpcLog, RpcTransaction},
};

/// Default buffer size for pending transaction notifications.
pub const PENDING_TX_CHANNEL_CAPACITY: usize = 2048;

/// Default buffer size for Kora mempool lifecycle notifications.
pub const MEMPOOL_EVENT_CHANNEL_CAPACITY: usize = 4096;

/// Default buffer size for new head block notifications.
pub const NEW_HEAD_CHANNEL_CAPACITY: usize = 256;

/// Default buffer size for log notifications.
pub const LOG_CHANNEL_CAPACITY: usize = 2048;

/// Broadcast sender for pending transaction events.
pub type PendingTxEventSender = broadcast::Sender<PendingTxEvent>;

/// Broadcast sender for Kora mempool lifecycle events.
pub type MempoolEventSender = broadcast::Sender<MempoolEvent>;

/// Broadcast sender for new head block events.
pub type NewHeadEventSender = broadcast::Sender<RpcBlock>;

/// Broadcast sender for log events.
pub type LogEventSender = broadcast::Sender<RpcLog>;

/// Events broadcast when transactions enter the mempool.
#[derive(Clone, Debug)]
pub enum PendingTxEvent {
    /// A new transaction was accepted into the pool.
    Added(PendingTxInfo),
}

/// Pending transaction data sent to Ethereum subscription clients.
#[derive(Clone, Debug)]
pub struct PendingTxInfo {
    /// Transaction hash.
    pub hash: B256,
    /// Full RPC transaction object when available.
    pub full_tx: Option<RpcTransaction>,
}

/// Create a pending transaction broadcast channel with the default capacity.
pub fn pending_tx_channel() -> (PendingTxEventSender, broadcast::Receiver<PendingTxEvent>) {
    broadcast::channel(PENDING_TX_CHANNEL_CAPACITY)
}

/// Create a mempool lifecycle broadcast channel with the default capacity.
pub fn mempool_event_channel() -> (MempoolEventSender, broadcast::Receiver<MempoolEvent>) {
    broadcast::channel(MEMPOOL_EVENT_CHANNEL_CAPACITY)
}

/// Create a new head block broadcast channel with the default capacity.
pub fn new_head_channel() -> (NewHeadEventSender, broadcast::Receiver<RpcBlock>) {
    broadcast::channel(NEW_HEAD_CHANNEL_CAPACITY)
}

/// Create a log broadcast channel with the default capacity.
pub fn log_channel() -> (LogEventSender, broadcast::Receiver<RpcLog>) {
    broadcast::channel(LOG_CHANNEL_CAPACITY)
}

/// Build the RPC subscription methods.
pub(crate) fn subscription_module(
    pending_tx_broadcast: Option<PendingTxEventSender>,
    mempool_broadcast: Option<MempoolEventSender>,
    new_head_broadcast: Option<NewHeadEventSender>,
    log_broadcast: Option<LogEventSender>,
) -> Result<RpcModule<()>, jsonrpsee::core::RegisterMethodError> {
    let mut module = RpcModule::new(());

    let eth_pending = pending_tx_broadcast;
    let eth_new_heads = new_head_broadcast;
    let eth_logs = log_broadcast;
    module.register_subscription(
        "eth_subscribe",
        "eth_subscription",
        "eth_unsubscribe",
        move |params, pending, _, _| {
            let eth_pending = eth_pending.clone();
            let eth_new_heads = eth_new_heads.clone();
            let eth_logs = eth_logs.clone();
            async move {
                let (kind, options) = match parse_subscription_params(&params) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        pending.reject(err).await;
                        return;
                    }
                };

                match kind.as_str() {
                    "newPendingTransactions" => {
                        let Some(sender) = eth_pending else {
                            pending
                                .reject(ErrorObjectOwned::owned(
                                    codes::METHOD_NOT_SUPPORTED,
                                    "newPendingTransactions subscriptions are not available",
                                    None::<()>,
                                ))
                                .await;
                            return;
                        };

                        let full_tx = wants_full_tx(options.as_ref());
                        let mut receiver = sender.subscribe();
                        let sink = match pending.accept().await {
                            Ok(sink) => sink,
                            Err(err) => {
                                warn!(error = ?err, "failed to accept pending transaction subscription");
                                return;
                            }
                        };

                        while let Some(event) =
                            recv_broadcast(&mut receiver, "eth_newPendingTransactions").await
                        {
                            let PendingTxEvent::Added(info) = event;
                            let message = if full_tx {
                                match &info.full_tx {
                                    Some(tx) => SubscriptionMessage::from_json(tx),
                                    None => SubscriptionMessage::from_json(&info.hash),
                                }
                            } else {
                                SubscriptionMessage::from_json(&info.hash)
                            }
                            .map_err(|err| {
                                warn!(error = %err, "failed to serialize pending transaction notification");
                            });

                            let Ok(message) = message else {
                                break;
                            };

                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                    }
                    "newHeads" => {
                        let Some(sender) = eth_new_heads else {
                            pending
                                .reject(ErrorObjectOwned::owned(
                                    codes::METHOD_NOT_SUPPORTED,
                                    "newHeads subscriptions are not available",
                                    None::<()>,
                                ))
                                .await;
                            return;
                        };

                        let mut receiver = sender.subscribe();
                        let sink = match pending.accept().await {
                            Ok(sink) => sink,
                            Err(err) => {
                                warn!(error = ?err, "failed to accept newHeads subscription");
                                return;
                            }
                        };

                        while let Some(block) =
                            recv_broadcast(&mut receiver, "eth_newHeads").await
                        {
                            let message = SubscriptionMessage::from_json(&block).map_err(|err| {
                                warn!(error = %err, "failed to serialize newHeads notification");
                            });
                            let Ok(message) = message else {
                                break;
                            };
                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                    }
                    "logs" => {
                        let Some(sender) = eth_logs else {
                            pending
                                .reject(ErrorObjectOwned::owned(
                                    codes::METHOD_NOT_SUPPORTED,
                                    "logs subscriptions are not available",
                                    None::<()>,
                                ))
                                .await;
                            return;
                        };

                        let sub_filter = parse_log_subscription_filter(options.as_ref());
                        let mut receiver = sender.subscribe();
                        let sink = match pending.accept().await {
                            Ok(sink) => sink,
                            Err(err) => {
                                warn!(error = ?err, "failed to accept logs subscription");
                                return;
                            }
                        };

                        while let Some(log) =
                            recv_broadcast(&mut receiver, "eth_logs").await
                        {
                            if !matches_log_subscription(&log, &sub_filter) {
                                continue;
                            }
                            let message = SubscriptionMessage::from_json(&log).map_err(|err| {
                                warn!(error = %err, "failed to serialize log notification");
                            });
                            let Ok(message) = message else {
                                break;
                            };
                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                    }
                    _ => {
                        pending.reject(unsupported_subscription("eth", &kind)).await;
                    }
                }
            }
        },
    )?;

    let kora_mempool = mempool_broadcast;
    module.register_subscription(
        "kora_subscribe",
        "kora_subscription",
        "kora_unsubscribe",
        move |params, pending, _, _| {
            let kora_mempool = kora_mempool.clone();
            async move {
                let (kind, _) = match parse_subscription_params(&params) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        pending.reject(err).await;
                        return;
                    }
                };

                if kind != "mempool" {
                    pending.reject(unsupported_subscription("kora", &kind)).await;
                    return;
                }

                let Some(sender) = kora_mempool else {
                    pending
                        .reject(ErrorObjectOwned::owned(
                            codes::METHOD_NOT_SUPPORTED,
                            "mempool subscriptions are not available",
                            None::<()>,
                        ))
                        .await;
                    return;
                };

                let mut receiver = sender.subscribe();
                let sink = match pending.accept().await {
                    Ok(sink) => sink,
                    Err(err) => {
                        warn!(error = ?err, "failed to accept mempool subscription");
                        return;
                    }
                };

                while let Some(event) = recv_broadcast(&mut receiver, "kora_mempool").await {
                    let message = SubscriptionMessage::from_json(&event).map_err(|err| {
                        warn!(error = %err, "failed to serialize mempool notification");
                    });

                    let Ok(message) = message else {
                        break;
                    };

                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
            }
        },
    )?;

    Ok(module)
}

fn parse_subscription_params(
    params: &Params<'_>,
) -> Result<(String, Option<Value>), ErrorObjectOwned> {
    let mut params = params.sequence();
    let kind = params.next()?;
    let options = params.optional_next()?;
    Ok((kind, options))
}

fn wants_full_tx(options: Option<&Value>) -> bool {
    match options {
        Some(Value::Bool(full_tx)) => *full_tx,
        Some(Value::Object(map)) => map.get("fullTx").and_then(Value::as_bool).unwrap_or_default(),
        _ => false,
    }
}

async fn recv_broadcast<T>(receiver: &mut broadcast::Receiver<T>, subscription: &str) -> Option<T>
where
    T: Clone,
{
    loop {
        match receiver.recv().await {
            Ok(event) => return Some(event),
            Err(RecvError::Lagged(skipped)) => {
                warn!(subscription, skipped, "subscription receiver lagged; skipping events");
            }
            Err(RecvError::Closed) => return None,
        }
    }
}

fn unsupported_subscription(namespace: &str, kind: &str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        codes::METHOD_NOT_SUPPORTED,
        format!("{namespace}_subscribe does not support {kind:?}"),
        None::<()>,
    )
}

/// Parsed log subscription filter (address + topic constraints).
#[derive(Debug, Default)]
struct LogSubscriptionFilter {
    addresses: Vec<alloy_primitives::Address>,
    topics: Vec<Option<Vec<B256>>>,
}

/// Parse the optional filter object from a `logs` subscription request.
fn parse_log_subscription_filter(options: Option<&Value>) -> LogSubscriptionFilter {
    let Some(Value::Object(map)) = options else {
        return LogSubscriptionFilter::default();
    };

    let addresses = match map.get("address") {
        Some(Value::String(s)) => s.parse().ok().into_iter().collect(),
        Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str()?.parse().ok()).collect(),
        _ => Vec::new(),
    };

    let topics: Vec<Option<Vec<B256>>> = match map.get("topics") {
        Some(Value::Array(arr)) => arr
            .iter()
            .map(|t| match t {
                Value::Null => None,
                Value::String(s) => s.parse().ok().map(|h| vec![h]),
                Value::Array(inner) => {
                    let v: Vec<B256> =
                        inner.iter().filter_map(|v| v.as_str()?.parse().ok()).collect();
                    if v.is_empty() { None } else { Some(v) }
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    LogSubscriptionFilter { addresses, topics }
}

/// Check whether a log matches the subscription filter.
fn matches_log_subscription(log: &RpcLog, filter: &LogSubscriptionFilter) -> bool {
    if !filter.addresses.is_empty() && !filter.addresses.contains(&log.address) {
        return false;
    }
    for (i, topic_filter) in filter.topics.iter().enumerate() {
        if let Some(allowed) = topic_filter {
            match log.topics.get(i) {
                Some(t) if allowed.contains(t) => {}
                _ => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::{Address, B256, U64, U256};
    use serde_json::json;

    use super::*;

    async fn next_value<T: serde::de::DeserializeOwned>(
        sub: &mut jsonrpsee::server::Subscription,
    ) -> T {
        let next = tokio::time::timeout(Duration::from_secs(1), sub.next::<T>())
            .await
            .expect("subscription response timed out")
            .expect("subscription closed")
            .expect("subscription response should decode");
        next.0
    }

    #[tokio::test]
    async fn eth_pending_subscription_receives_hash() {
        let (pending_tx, _) = broadcast::channel(16);
        let module = subscription_module(Some(pending_tx.clone()), None, None, None).unwrap();
        let mut sub =
            module.subscribe_unbounded("eth_subscribe", ("newPendingTransactions",)).await.unwrap();
        let hash = B256::repeat_byte(0xaa);

        pending_tx.send(PendingTxEvent::Added(PendingTxInfo { hash, full_tx: None })).unwrap();

        let value: Value = next_value(&mut sub).await;
        assert_eq!(value, json!(hash));
    }

    #[tokio::test]
    async fn eth_pending_subscription_receives_full_tx() {
        let (pending_tx, _) = broadcast::channel(16);
        let module = subscription_module(Some(pending_tx.clone()), None, None, None).unwrap();
        let mut sub = module
            .subscribe_unbounded(
                "eth_subscribe",
                ("newPendingTransactions", json!({ "fullTx": true })),
            )
            .await
            .unwrap();
        let tx = RpcTransaction {
            hash: B256::repeat_byte(0xbb),
            nonce: U64::from(7),
            from: Address::repeat_byte(0x11),
            to: Some(Address::repeat_byte(0x22)),
            value: U256::from(123),
            gas_price: U256::from(1_000_000_000u64),
            ..Default::default()
        };

        pending_tx
            .send(PendingTxEvent::Added(PendingTxInfo { hash: tx.hash, full_tx: Some(tx.clone()) }))
            .unwrap();

        let value: Value = next_value(&mut sub).await;
        assert_eq!(value, serde_json::to_value(tx).unwrap());
    }

    #[tokio::test]
    async fn kora_mempool_subscription_receives_event() {
        let (mempool, _) = broadcast::channel(16);
        let module = subscription_module(None, Some(mempool.clone()), None, None).unwrap();
        let mut sub = module.subscribe_unbounded("kora_subscribe", ("mempool",)).await.unwrap();
        let event = MempoolEvent::TxIncluded {
            hash: B256::repeat_byte(0xcc),
            block_number: 9,
            block_hash: B256::repeat_byte(0xdd),
        };

        mempool.send(event.clone()).unwrap();

        let received: MempoolEvent = next_value(&mut sub).await;
        assert_eq!(received, event);
    }

    #[tokio::test]
    async fn broadcast_receiver_skips_lagged_events() {
        let (sender, mut receiver) = broadcast::channel(1);
        sender.send(1_u64).unwrap();
        sender.send(2_u64).unwrap();

        let received = recv_broadcast(&mut receiver, "test").await;
        assert_eq!(received, Some(2));
    }
}
