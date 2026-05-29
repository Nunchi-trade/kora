//! HTTP and JSON-RPC server implementation.

use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use jsonrpsee::{
    core::server::MethodResponse,
    server::{
        BatchRequestConfig, ConnectionId, Server, ServerHandle,
        middleware::rpc::{RpcServiceBuilder, RpcServiceT},
    },
    types::{ErrorObjectOwned, Id, Request as RpcRequest},
};
use kora_txpool::TransactionPool;
use parking_lot::Mutex;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, info, warn};

use crate::{
    config::{CorsConfig, RateLimitConfig, RpcServerConfig},
    error::codes,
    eth::{
        EthApiImpl, EthApiServer, NetApiImpl, NetApiServer, TxSubmitCallback, Web3ApiImpl,
        Web3ApiServer,
    },
    kora::{KoraApiImpl, KoraApiServer},
    state::NodeState,
    state_provider::{NoopStateProvider, StateProvider},
    subscription::{MempoolEventSender, PendingTxEventSender, subscription_module},
    txpool::{TxpoolApiImpl, TxpoolApiServer},
};

/// Error type for RPC server operations.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Failed to bind server.
    #[error("failed to bind server: {0}")]
    Bind(std::io::Error),
    /// Failed to build server.
    #[error("failed to build server: {0}")]
    Build(String),
    /// Failed to register RPC methods.
    #[error("failed to register RPC methods: {0}")]
    RegisterMethod(#[from] jsonrpsee::core::RegisterMethodError),
}

/// Build a CORS layer from configuration.
fn build_cors_layer(config: &CorsConfig) -> CorsLayer {
    if config.allowed_origins.is_empty() {
        return CorsLayer::new();
    }

    let mut layer = CorsLayer::new();

    if config.allowed_origins.len() == 1 && config.allowed_origins[0] == "*" {
        layer = layer.allow_origin(Any);
    } else {
        let origins: Vec<_> =
            config.allowed_origins.iter().filter_map(|o| o.parse().ok()).collect();
        layer = layer.allow_origin(AllowOrigin::list(origins));
    }

    if config.allowed_methods.iter().any(|m| m == "*") {
        layer = layer.allow_methods(Any);
    } else {
        let methods: Vec<_> =
            config.allowed_methods.iter().filter_map(|m| m.parse().ok()).collect();
        layer = layer.allow_methods(methods);
    }

    if config.allowed_headers.iter().any(|h| h == "*") {
        layer = layer.allow_headers(Any);
    } else {
        let headers: Vec<_> =
            config.allowed_headers.iter().filter_map(|h| h.parse().ok()).collect();
        layer = layer.allow_headers(headers);
    }

    layer.max_age(Duration::from_secs(config.max_age))
}

/// Global (server-wide) rate limiter used as a backstop to cap total
/// throughput across all connections.  This is the original single-bucket
/// limiter, now renamed to clarify its role.
#[derive(Debug, Clone)]
struct SharedRateLimiter {
    bucket: Arc<Mutex<TokenBucket>>,
}

impl SharedRateLimiter {
    fn new(config: RateLimitConfig) -> Option<Self> {
        if config.is_disabled() {
            return None;
        }

        Some(Self { bucket: Arc::new(Mutex::new(TokenBucket::new(config, Instant::now()))) })
    }

    fn try_acquire(&self) -> bool {
        self.bucket.lock().try_acquire_at(Instant::now())
    }
}

/// Per-connection rate limiter that maintains a separate [`TokenBucket`] for
/// each jsonrpsee [`ConnectionId`].
///
/// Ideally this would key by client IP, but jsonrpsee 0.24 only injects
/// [`ConnectionId`] (not the peer address) into request extensions.  Since
/// each TCP connection gets a unique ID, this still isolates independent
/// clients.  A single client opening many connections will get a separate
/// budget per connection, which is acceptable -- the global limiter caps
/// aggregate throughput.
///
/// Stale entries are pruned lazily: every [`CLEANUP_INTERVAL`] seconds the
/// map is scanned and buckets that have been idle longer than the interval
/// are removed.
#[derive(Debug, Clone)]
struct PerConnectionRateLimiter {
    inner: Arc<Mutex<PerConnectionInner>>,
    config: RateLimitConfig,
}

/// Duration of inactivity after which a connection bucket is considered stale
/// and eligible for eviction.
const STALE_BUCKET_SECS: u64 = 300;

/// Minimum wall-clock interval between cleanup sweeps.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct PerConnectionInner {
    buckets: HashMap<usize, TokenBucket>,
    last_cleanup: Instant,
}

impl PerConnectionRateLimiter {
    fn new(config: RateLimitConfig) -> Option<Self> {
        if config.is_disabled() {
            return None;
        }
        Some(Self {
            inner: Arc::new(Mutex::new(PerConnectionInner {
                buckets: HashMap::new(),
                last_cleanup: Instant::now(),
            })),
            config,
        })
    }

    /// Try to acquire a token for the given connection.  Creates a new bucket
    /// lazily if this is the first request on `conn_id`.
    fn try_acquire(&self, conn_id: usize) -> bool {
        let now = Instant::now();
        let mut inner = self.inner.lock();

        // Lazy cleanup: periodically prune idle buckets to bound memory.
        if now.saturating_duration_since(inner.last_cleanup) >= CLEANUP_INTERVAL {
            let stale_cutoff = Duration::from_secs(STALE_BUCKET_SECS);
            inner.buckets.retain(|_, bucket| {
                now.saturating_duration_since(bucket.last_refill) < stale_cutoff
            });
            inner.last_cleanup = now;
        }

        let bucket = inner
            .buckets
            .entry(conn_id)
            .or_insert_with(|| TokenBucket::new(self.config.clone(), now));
        bucket.try_acquire_at(now)
    }
}

#[derive(Debug)]
struct TokenBucket {
    requests_per_second: f64,
    burst_size: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    const fn new(config: RateLimitConfig, now: Instant) -> Self {
        let requests_per_second = config.requests_per_second as f64;
        let burst_size = if config.requests_per_second == 0 {
            0.0
        } else {
            // Clamp burst_size to at least 1 so that an enabled limiter can
            // always admit at least one request.  Without this, burst_size==0
            // would start with 0 tokens and refill() would never add more,
            // permanently rejecting all requests.
            let bs = config.burst_size as f64;
            if bs < 1.0 { 1.0 } else { bs }
        };

        Self { requests_per_second, burst_size, tokens: burst_size, last_refill: now }
    }

    fn try_acquire_at(&mut self, now: Instant) -> bool {
        self.refill(now);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        if elapsed.is_zero() {
            return;
        }

        self.last_refill = now;
        if self.requests_per_second == 0.0 || self.tokens >= self.burst_size {
            return;
        }

        let replenished = elapsed.as_secs_f64() * self.requests_per_second;
        self.tokens = (self.tokens + replenished).min(self.burst_size);
    }
}

fn global_rate_limit_allows(rate_limiter: &Option<SharedRateLimiter>) -> bool {
    rate_limiter.as_ref().is_none_or(SharedRateLimiter::try_acquire)
}

fn rate_limited_rpc_response(id: Id<'static>) -> MethodResponse {
    MethodResponse::error(
        id,
        ErrorObjectOwned::owned(codes::LIMIT_EXCEEDED, "rate limit exceeded", None::<()>),
    )
}

async fn enforce_http_rate_limit(
    State(rate_limiter): State<Option<SharedRateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    if !global_rate_limit_allows(&rate_limiter) {
        return (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response();
    }

    next.run(request).await
}

/// Maximum number of JSON-RPC calls allowed in a single batch request.
/// Prevents a single HTTP POST from draining the entire rate limit budget.
const MAX_BATCH_SIZE: u32 = 50;

#[derive(Debug, Clone)]
struct RateLimitedRpcService<S> {
    service: S,
    /// Per-connection rate limiter (primary defense).
    per_conn_limiter: Option<PerConnectionRateLimiter>,
    /// Global rate limiter (backstop for aggregate throughput).
    global_limiter: Option<SharedRateLimiter>,
}

/// Subscription method names that require WebSocket transport.
const SUBSCRIPTION_METHODS: &[&str] =
    &["eth_subscribe", "eth_unsubscribe", "kora_subscribe", "kora_unsubscribe"];

/// Check whether `method` is a subscription method that requires WebSocket.
fn is_subscription_method(method: &str) -> bool {
    SUBSCRIPTION_METHODS.contains(&method)
}

/// Build a [`MethodResponse`] with error code `-32004` (method not supported)
/// when a subscription method is called over HTTP.
fn subscription_not_available_response(id: Id<'static>) -> MethodResponse {
    MethodResponse::error(
        id,
        ErrorObjectOwned::owned(
            codes::METHOD_NOT_SUPPORTED,
            "Subscriptions are not available over HTTP. Use WebSocket instead.",
            None::<()>,
        ),
    )
}

impl<'a, S> RpcServiceT<'a> for RateLimitedRpcService<S>
where
    S: RpcServiceT<'a> + Clone + Send + Sync + 'static,
    S::Future: Send,
{
    type Future = Pin<Box<dyn Future<Output = MethodResponse> + Send + 'a>>;

    fn call(&self, request: RpcRequest<'a>) -> Self::Future {
        // --- Per-connection rate limit (primary) ---
        if let Some(ref limiter) = self.per_conn_limiter {
            let conn_id = request.extensions().get::<ConnectionId>().map(|id| id.0);

            match conn_id {
                Some(id) => {
                    if !limiter.try_acquire(id) {
                        return Box::pin(std::future::ready(rate_limited_rpc_response(
                            request.id().into_owned(),
                        )));
                    }
                }
                None => {
                    // ConnectionId is normally always present.  If missing,
                    // log once and fall through to the global limiter.
                    warn!(
                        "RPC request missing ConnectionId in extensions; falling back to global limiter"
                    );
                }
            }
        }

        // --- Global rate limit (backstop) ---
        if !global_rate_limit_allows(&self.global_limiter) {
            return Box::pin(std::future::ready(rate_limited_rpc_response(
                request.id().into_owned(),
            )));
        }

        let is_sub = is_subscription_method(request.method_name());
        let id = request.id().into_owned();
        let fut = self.service.call(request);

        Box::pin(async move {
            let response = fut.await;

            // When jsonrpsee receives a subscription call over HTTP it returns
            // ErrorCode::InternalError (-32603) because subscriptions require a
            // persistent connection.  Replace that with -32004 and a message
            // that tells the caller to use WebSocket instead.
            if is_sub && response.as_error_code() == Some(codes::INTERNAL_ERROR) {
                return subscription_not_available_response(id);
            }

            response
        })
    }
}

fn build_http_router(
    node_state: Arc<NodeState>,
    cors_layer: CorsLayer,
    max_connections: u32,
    rate_limiter: Option<SharedRateLimiter>,
) -> Router {
    Router::new()
        .route("/status", get(status_handler))
        .route("/health", get(health_handler))
        .layer(middleware::from_fn_with_state(rate_limiter, enforce_http_rate_limit))
        .layer(cors_layer)
        .layer(ConcurrencyLimitLayer::new(max_connections as usize))
        .with_state(node_state)
}

/// RPC server for exposing node status via HTTP and Ethereum JSON-RPC.
pub struct RpcServer<S: StateProvider = NoopStateProvider> {
    state: NodeState,
    http_addr: SocketAddr,
    jsonrpc_addr: SocketAddr,
    chain_id: u64,
    tx_submit: Option<TxSubmitCallback>,
    txpool: Option<TransactionPool>,
    state_provider: S,
    cors_config: CorsConfig,
    rate_limit_config: RateLimitConfig,
    max_connections: u32,
    max_subscriptions_per_connection: u32,
    peer_count: u64,
    pending_tx_broadcast: Option<PendingTxEventSender>,
    mempool_broadcast: Option<MempoolEventSender>,
}

impl<S: StateProvider> std::fmt::Debug for RpcServer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcServer")
            .field("state", &self.state)
            .field("http_addr", &self.http_addr)
            .field("jsonrpc_addr", &self.jsonrpc_addr)
            .field("chain_id", &self.chain_id)
            .field("tx_submit", &self.tx_submit.is_some())
            .field("txpool", &self.txpool.is_some())
            .field("pending_tx_broadcast", &self.pending_tx_broadcast.is_some())
            .field("mempool_broadcast", &self.mempool_broadcast.is_some())
            .field("rate_limit_config", &self.rate_limit_config)
            .field("max_connections", &self.max_connections)
            .field("max_subscriptions_per_connection", &self.max_subscriptions_per_connection)
            .finish()
    }
}

/// Compute a default HTTP address by incrementing the port of the given address.
const fn default_http_addr(jsonrpc_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(jsonrpc_addr.ip(), jsonrpc_addr.port() + 1)
}

impl RpcServer<NoopStateProvider> {
    /// Create a new RPC server with default (noop) state provider.
    ///
    /// The JSON-RPC server binds to `addr`; the HTTP status server binds to `addr.port() + 1`.
    pub fn new(state: NodeState, addr: SocketAddr) -> Self {
        Self {
            state,
            http_addr: default_http_addr(addr),
            jsonrpc_addr: addr,
            chain_id: 1,
            tx_submit: None,
            txpool: None,
            state_provider: NoopStateProvider,
            cors_config: CorsConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
            max_connections: 100,
            max_subscriptions_per_connection: 32,
            peer_count: 0,
            pending_tx_broadcast: None,
            mempool_broadcast: None,
        }
    }

    /// Create a new RPC server with chain ID.
    pub fn with_chain_id(state: NodeState, addr: SocketAddr, chain_id: u64) -> Self {
        Self {
            state,
            http_addr: default_http_addr(addr),
            jsonrpc_addr: addr,
            chain_id,
            tx_submit: None,
            txpool: None,
            state_provider: NoopStateProvider,
            cors_config: CorsConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
            max_connections: 100,
            max_subscriptions_per_connection: 32,
            peer_count: 0,
            pending_tx_broadcast: None,
            mempool_broadcast: None,
        }
    }
}

impl<S: StateProvider + Clone + 'static> RpcServer<S> {
    /// Create a new RPC server with a custom state provider.
    pub fn with_state_provider(
        state: NodeState,
        addr: SocketAddr,
        chain_id: u64,
        state_provider: S,
    ) -> Self {
        Self {
            state,
            http_addr: default_http_addr(addr),
            jsonrpc_addr: addr,
            chain_id,
            tx_submit: None,
            txpool: None,
            state_provider,
            cors_config: CorsConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
            max_connections: 100,
            max_subscriptions_per_connection: 32,
            peer_count: 0,
            pending_tx_broadcast: None,
            mempool_broadcast: None,
        }
    }

    /// Set the transaction submission callback.
    #[must_use]
    pub fn with_tx_submit(mut self, tx_submit: TxSubmitCallback) -> Self {
        self.tx_submit = Some(tx_submit);
        self
    }

    /// Set the transaction pool exposed by the `txpool_*` namespace.
    #[must_use]
    pub fn with_txpool(mut self, txpool: TransactionPool) -> Self {
        self.txpool = Some(txpool);
        self
    }

    /// Set the pending transaction broadcast channel used by subscriptions.
    #[must_use]
    pub fn with_pending_tx_broadcast(mut self, pending_tx_broadcast: PendingTxEventSender) -> Self {
        self.pending_tx_broadcast = Some(pending_tx_broadcast);
        self
    }

    /// Set the Kora mempool lifecycle broadcast channel used by subscriptions.
    #[must_use]
    pub fn with_mempool_broadcast(mut self, mempool_broadcast: MempoolEventSender) -> Self {
        self.mempool_broadcast = Some(mempool_broadcast);
        self
    }

    /// Set CORS configuration.
    #[must_use]
    pub fn with_cors(mut self, cors_config: CorsConfig) -> Self {
        self.cors_config = cors_config;
        self
    }

    /// Set rate limiting configuration.
    #[must_use]
    pub const fn with_rate_limit_config(mut self, rate_limit_config: RateLimitConfig) -> Self {
        self.rate_limit_config = rate_limit_config;
        self
    }

    /// Set maximum concurrent connections.
    #[must_use]
    pub const fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set the maximum number of WebSocket subscriptions per connection.
    #[must_use]
    pub const fn with_max_subscriptions_per_connection(
        mut self,
        max_subscriptions_per_connection: u32,
    ) -> Self {
        self.max_subscriptions_per_connection = max_subscriptions_per_connection;
        self
    }

    /// Set the initially reported peer count for `net_peerCount`.
    #[must_use]
    pub const fn with_peer_count(mut self, peer_count: u64) -> Self {
        self.peer_count = peer_count;
        self
    }

    /// Create from configuration.
    pub fn from_config(state: NodeState, config: RpcServerConfig, state_provider: S) -> Self {
        Self {
            state,
            http_addr: config.http_addr,
            jsonrpc_addr: config.jsonrpc_addr,
            chain_id: config.chain_id,
            tx_submit: None,
            txpool: None,
            state_provider,
            cors_config: config.cors,
            rate_limit_config: config.rate_limit,
            max_connections: config.max_connections,
            max_subscriptions_per_connection: config.max_subscriptions_per_connection,
            peer_count: 0,
            pending_tx_broadcast: None,
            mempool_broadcast: None,
        }
    }

    /// Start the RPC server.
    ///
    /// This spawns background tasks for both HTTP and JSON-RPC servers and returns immediately.
    pub fn start(self) -> RpcServerHandle {
        let http_addr = self.http_addr;
        let jsonrpc_addr = self.jsonrpc_addr;
        let node_state = Arc::new(self.state);
        let node_state_for_jsonrpc = Arc::clone(&node_state);
        let chain_id = self.chain_id;
        let tx_submit = self.tx_submit;
        let txpool = self.txpool;
        let cors_layer = build_cors_layer(&self.cors_config);
        let http_rate_limiter = SharedRateLimiter::new(self.rate_limit_config.clone());
        let rpc_global_limiter = SharedRateLimiter::new(self.rate_limit_config.clone());
        let rpc_per_conn_limiter = PerConnectionRateLimiter::new(self.rate_limit_config);
        let max_connections = self.max_connections;
        let max_subscriptions_per_connection = self.max_subscriptions_per_connection;
        let state_provider = self.state_provider;
        let peer_count = self.peer_count;
        let pending_tx_broadcast = self.pending_tx_broadcast;
        let mempool_broadcast = self.mempool_broadcast;

        let http_handle = tokio::spawn(async move {
            let app = build_http_router(node_state, cors_layer, max_connections, http_rate_limiter);

            info!(addr = %http_addr, "Starting HTTP server");

            let listener = match tokio::net::TcpListener::bind(http_addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(error = %e, "Failed to bind HTTP server");
                    return;
                }
            };

            if let Err(e) = axum::serve(listener, app).await {
                error!(error = %e, "HTTP server error");
            }
        });

        let jsonrpc_handle = tokio::spawn(async move {
            let rpc_middleware =
                RpcServiceBuilder::new().layer_fn(move |service| RateLimitedRpcService {
                    service,
                    per_conn_limiter: rpc_per_conn_limiter.clone(),
                    global_limiter: rpc_global_limiter.clone(),
                });

            let server = match Server::builder()
                .max_connections(max_connections)
                .max_subscriptions_per_connection(max_subscriptions_per_connection)
                .set_batch_request_config(BatchRequestConfig::Limit(MAX_BATCH_SIZE))
                .set_rpc_middleware(rpc_middleware)
                .build(jsonrpc_addr)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "Failed to build JSON-RPC server");
                    return None;
                }
            };

            let mut eth_api = tx_submit.map_or_else(
                || EthApiImpl::new(chain_id, state_provider.clone()),
                |submit| EthApiImpl::with_tx_submit(chain_id, state_provider.clone(), submit),
            );
            if let Some(sender) = pending_tx_broadcast.clone() {
                eth_api = eth_api.with_pending_tx_broadcast(sender);
            }
            if let Some(sender) = mempool_broadcast.clone() {
                eth_api = eth_api.with_mempool_broadcast(sender);
            }
            let net_api = NetApiImpl::new(chain_id);
            net_api.set_peer_count(peer_count);
            let web3_api = Web3ApiImpl::new();
            let kora_api = KoraApiImpl::new(node_state_for_jsonrpc);
            let subscription_api = match subscription_module(
                pending_tx_broadcast.clone(),
                mempool_broadcast.clone(),
            ) {
                Ok(api) => api,
                Err(e) => {
                    error!(error = %e, "Failed to build subscription API");
                    return None;
                }
            };

            let mut module = jsonrpsee::RpcModule::new(());
            if let Err(e) = module.merge(eth_api.into_rpc()) {
                error!(error = %e, "Failed to merge eth API");
                return None;
            }
            if let Err(e) = module.merge(net_api.into_rpc()) {
                error!(error = %e, "Failed to merge net API");
                return None;
            }
            if let Err(e) = module.merge(web3_api.into_rpc()) {
                error!(error = %e, "Failed to merge web3 API");
                return None;
            }
            if let Err(e) = module.merge(kora_api.into_rpc()) {
                error!(error = %e, "Failed to merge kora API");
                return None;
            }
            if let Some(txpool) = txpool
                && let Err(e) = module.merge(TxpoolApiImpl::new(txpool).into_rpc())
            {
                error!(error = %e, "Failed to merge txpool API");
                return None;
            }
            if let Err(e) = module.merge(subscription_api) {
                error!(error = %e, "Failed to merge subscription API");
                return None;
            }

            info!(addr = %jsonrpc_addr, "Starting JSON-RPC server");

            let handle = server.start(module);
            handle.stopped().await;
            Some(())
        });

        RpcServerHandle { http_handle, jsonrpc_handle }
    }
}

/// Handle for managing the RPC server lifecycle.
pub struct RpcServerHandle {
    http_handle: tokio::task::JoinHandle<()>,
    jsonrpc_handle: tokio::task::JoinHandle<Option<()>>,
}

impl std::fmt::Debug for RpcServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcServerHandle").finish_non_exhaustive()
    }
}

impl RpcServerHandle {
    /// Wait for both servers to complete.
    pub async fn stopped(self) {
        let _ = tokio::join!(self.http_handle, self.jsonrpc_handle);
    }

    /// Abort both servers.
    pub fn abort(self) {
        self.http_handle.abort();
        self.jsonrpc_handle.abort();
    }
}

async fn status_handler(State(state): State<Arc<NodeState>>) -> impl IntoResponse {
    let status = state.status();
    (StatusCode::OK, axum::Json(status))
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Standalone JSON-RPC server without HTTP status endpoints.
pub struct JsonRpcServer<S: StateProvider = NoopStateProvider> {
    addr: SocketAddr,
    chain_id: u64,
    tx_submit: Option<TxSubmitCallback>,
    txpool: Option<TransactionPool>,
    state_provider: S,
    rate_limit_config: RateLimitConfig,
    max_connections: u32,
    max_subscriptions_per_connection: u32,
    peer_count: u64,
    pending_tx_broadcast: Option<PendingTxEventSender>,
    mempool_broadcast: Option<MempoolEventSender>,
}

impl<S: StateProvider> std::fmt::Debug for JsonRpcServer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonRpcServer")
            .field("addr", &self.addr)
            .field("chain_id", &self.chain_id)
            .field("tx_submit", &self.tx_submit.is_some())
            .field("txpool", &self.txpool.is_some())
            .field("pending_tx_broadcast", &self.pending_tx_broadcast.is_some())
            .field("mempool_broadcast", &self.mempool_broadcast.is_some())
            .field("rate_limit_config", &self.rate_limit_config)
            .field("max_connections", &self.max_connections)
            .field("max_subscriptions_per_connection", &self.max_subscriptions_per_connection)
            .finish()
    }
}

impl JsonRpcServer<NoopStateProvider> {
    /// Create a new JSON-RPC server with default (noop) state provider.
    pub fn new(addr: SocketAddr, chain_id: u64) -> Self {
        Self {
            addr,
            chain_id,
            tx_submit: None,
            txpool: None,
            state_provider: NoopStateProvider,
            rate_limit_config: RateLimitConfig::default(),
            max_connections: 100,
            max_subscriptions_per_connection: 32,
            peer_count: 0,
            pending_tx_broadcast: None,
            mempool_broadcast: None,
        }
    }
}

impl<S: StateProvider + Clone + 'static> JsonRpcServer<S> {
    /// Create a new JSON-RPC server with a custom state provider.
    pub fn with_state_provider(addr: SocketAddr, chain_id: u64, state_provider: S) -> Self {
        Self {
            addr,
            chain_id,
            tx_submit: None,
            txpool: None,
            state_provider,
            rate_limit_config: RateLimitConfig::default(),
            max_connections: 100,
            max_subscriptions_per_connection: 32,
            peer_count: 0,
            pending_tx_broadcast: None,
            mempool_broadcast: None,
        }
    }

    /// Set the transaction submission callback.
    #[must_use]
    pub fn with_tx_submit(mut self, tx_submit: TxSubmitCallback) -> Self {
        self.tx_submit = Some(tx_submit);
        self
    }

    /// Set the transaction pool exposed by the `txpool_*` namespace.
    #[must_use]
    pub fn with_txpool(mut self, txpool: TransactionPool) -> Self {
        self.txpool = Some(txpool);
        self
    }

    /// Set the pending transaction broadcast channel used by subscriptions.
    #[must_use]
    pub fn with_pending_tx_broadcast(mut self, pending_tx_broadcast: PendingTxEventSender) -> Self {
        self.pending_tx_broadcast = Some(pending_tx_broadcast);
        self
    }

    /// Set the Kora mempool lifecycle broadcast channel used by subscriptions.
    #[must_use]
    pub fn with_mempool_broadcast(mut self, mempool_broadcast: MempoolEventSender) -> Self {
        self.mempool_broadcast = Some(mempool_broadcast);
        self
    }

    /// Set rate limiting configuration.
    #[must_use]
    pub const fn with_rate_limit_config(mut self, rate_limit_config: RateLimitConfig) -> Self {
        self.rate_limit_config = rate_limit_config;
        self
    }

    /// Set maximum concurrent connections.
    #[must_use]
    pub const fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set the maximum number of WebSocket subscriptions per connection.
    #[must_use]
    pub const fn with_max_subscriptions_per_connection(
        mut self,
        max_subscriptions_per_connection: u32,
    ) -> Self {
        self.max_subscriptions_per_connection = max_subscriptions_per_connection;
        self
    }

    /// Set the initially reported peer count for `net_peerCount`.
    #[must_use]
    pub const fn with_peer_count(mut self, peer_count: u64) -> Self {
        self.peer_count = peer_count;
        self
    }

    /// Start the JSON-RPC server.
    pub async fn start(self) -> Result<ServerHandle, ServerError> {
        let rpc_global_limiter = SharedRateLimiter::new(self.rate_limit_config.clone());
        let rpc_per_conn_limiter = PerConnectionRateLimiter::new(self.rate_limit_config);
        let rpc_middleware =
            RpcServiceBuilder::new().layer_fn(move |service| RateLimitedRpcService {
                service,
                per_conn_limiter: rpc_per_conn_limiter.clone(),
                global_limiter: rpc_global_limiter.clone(),
            });

        let server = Server::builder()
            .max_connections(self.max_connections)
            .max_subscriptions_per_connection(self.max_subscriptions_per_connection)
            .set_batch_request_config(BatchRequestConfig::Limit(MAX_BATCH_SIZE))
            .set_rpc_middleware(rpc_middleware)
            .build(self.addr)
            .await
            .map_err(|e| ServerError::Build(e.to_string()))?;

        let mut eth_api = self.tx_submit.map_or_else(
            || EthApiImpl::new(self.chain_id, self.state_provider.clone()),
            |submit| EthApiImpl::with_tx_submit(self.chain_id, self.state_provider.clone(), submit),
        );
        if let Some(sender) = self.pending_tx_broadcast.clone() {
            eth_api = eth_api.with_pending_tx_broadcast(sender);
        }
        if let Some(sender) = self.mempool_broadcast.clone() {
            eth_api = eth_api.with_mempool_broadcast(sender);
        }
        let net_api = NetApiImpl::new(self.chain_id);
        net_api.set_peer_count(self.peer_count);
        let web3_api = Web3ApiImpl::new();
        let subscription_api =
            subscription_module(self.pending_tx_broadcast, self.mempool_broadcast)?;

        let mut module = jsonrpsee::RpcModule::new(());
        module.merge(eth_api.into_rpc())?;
        module.merge(net_api.into_rpc())?;
        module.merge(web3_api.into_rpc())?;
        if let Some(txpool) = self.txpool {
            module.merge(TxpoolApiImpl::new(txpool).into_rpc())?;
        }
        module.merge(subscription_api)?;

        info!(addr = %self.addr, "Starting JSON-RPC server");

        Ok(server.start(module))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use axum::{body::Body, http::Request as HttpRequest};
    use jsonrpsee::core::server::ResponsePayload;
    use tower::ServiceExt;

    use super::*;

    #[derive(Debug, Clone)]
    struct AlwaysOkRpcService;

    impl<'a> RpcServiceT<'a> for AlwaysOkRpcService {
        type Future = std::future::Ready<MethodResponse>;

        fn call(&self, request: RpcRequest<'a>) -> Self::Future {
            std::future::ready(MethodResponse::response(
                request.id().into_owned(),
                ResponsePayload::success("ok"),
                usize::MAX,
            ))
        }
    }

    fn rpc_request(id: u64) -> RpcRequest<'static> {
        RpcRequest::new(Cow::Borrowed("web3_clientVersion"), None, Id::Number(id))
    }

    /// Build an [`RpcRequest`] with a [`ConnectionId`] injected in extensions.
    fn rpc_request_with_conn(id: u64, conn_id: usize) -> RpcRequest<'static> {
        let mut req = rpc_request(id);
        req.extensions_mut().insert(ConnectionId(conn_id));
        req
    }

    #[test]
    fn cors_layer_empty_origins() {
        let config = CorsConfig::none();
        let _layer = build_cors_layer(&config);
    }

    #[test]
    fn cors_layer_specific_origins() {
        let config = CorsConfig {
            allowed_origins: vec!["http://localhost:3000".to_string()],
            allowed_methods: vec!["GET".to_string(), "POST".to_string()],
            allowed_headers: vec!["Content-Type".to_string()],
            max_age: 3600,
        };
        let _layer = build_cors_layer(&config);
    }

    #[test]
    fn cors_layer_wildcard() {
        let config = CorsConfig::permissive();
        let _layer = build_cors_layer(&config);
    }

    #[test]
    fn token_bucket_honors_burst_and_refill() {
        let start = Instant::now();
        let mut bucket =
            TokenBucket::new(RateLimitConfig { requests_per_second: 2, burst_size: 2 }, start);

        assert!(bucket.try_acquire_at(start));
        assert!(bucket.try_acquire_at(start));
        assert!(!bucket.try_acquire_at(start));

        let half_second_later = start + Duration::from_millis(500);
        assert!(bucket.try_acquire_at(half_second_later));
        assert!(!bucket.try_acquire_at(half_second_later));
    }

    #[test]
    fn token_bucket_clamps_zero_burst_to_one() {
        let start = Instant::now();
        let mut bucket =
            TokenBucket::new(RateLimitConfig { requests_per_second: 10, burst_size: 0 }, start);

        // burst_size=0 is clamped to 1, so the first request succeeds.
        assert!(bucket.try_acquire_at(start));
        // Second request at the same instant is rejected (burst of 1).
        assert!(!bucket.try_acquire_at(start));

        // After enough time, a new token is replenished.
        let later = start + Duration::from_millis(200);
        assert!(bucket.try_acquire_at(later));
    }

    #[test]
    fn token_bucket_zero_rps_rejects_all() {
        let start = Instant::now();
        let mut bucket =
            TokenBucket::new(RateLimitConfig { requests_per_second: 0, burst_size: 100 }, start);

        // With requests_per_second=0, burst_size is forced to 0 and no tokens are ever added.
        assert!(!bucket.try_acquire_at(start));
        assert!(!bucket.try_acquire_at(start + Duration::from_secs(10)));
    }

    #[test]
    fn token_bucket_does_not_exceed_burst() {
        let start = Instant::now();
        let mut bucket =
            TokenBucket::new(RateLimitConfig { requests_per_second: 100, burst_size: 3 }, start);

        // Drain all tokens.
        assert!(bucket.try_acquire_at(start));
        assert!(bucket.try_acquire_at(start));
        assert!(bucket.try_acquire_at(start));
        assert!(!bucket.try_acquire_at(start));

        // Wait long enough for many tokens to accumulate, but cap at burst_size.
        let much_later = start + Duration::from_secs(60);
        assert!(bucket.try_acquire_at(much_later));
        assert!(bucket.try_acquire_at(much_later));
        assert!(bucket.try_acquire_at(much_later));
        assert!(!bucket.try_acquire_at(much_later));
    }

    #[test]
    fn disabled_rate_limit_does_not_build_limiter() {
        assert!(SharedRateLimiter::new(RateLimitConfig::disabled()).is_none());
        assert!(PerConnectionRateLimiter::new(RateLimitConfig::disabled()).is_none());
    }

    #[test]
    fn rate_limit_allows_with_no_limiter() {
        assert!(global_rate_limit_allows(&None));
    }

    #[test]
    fn rpc_server_from_config_threads_limits() {
        let config = RpcServerConfig::default()
            .with_rate_limit_burst(7, 11)
            .with_max_connections(13)
            .with_max_subscriptions_per_connection(17);

        let server = RpcServer::from_config(NodeState::new(1, 0), config, NoopStateProvider);

        assert_eq!(server.rate_limit_config.requests_per_second, 7);
        assert_eq!(server.rate_limit_config.burst_size, 11);
        assert_eq!(server.max_connections, 13);
        assert_eq!(server.max_subscriptions_per_connection, 17);
    }

    #[test]
    fn json_rpc_server_builders_thread_limits() {
        let server = JsonRpcServer::new("127.0.0.1:0".parse().unwrap(), 1)
            .with_rate_limit_config(RateLimitConfig { requests_per_second: 3, burst_size: 5 })
            .with_max_connections(7)
            .with_max_subscriptions_per_connection(9);

        assert_eq!(server.rate_limit_config.requests_per_second, 3);
        assert_eq!(server.rate_limit_config.burst_size, 5);
        assert_eq!(server.max_connections, 7);
        assert_eq!(server.max_subscriptions_per_connection, 9);
    }

    #[tokio::test]
    async fn rpc_rate_limiter_rejects_after_burst() {
        let per_conn = PerConnectionRateLimiter::new(RateLimitConfig {
            requests_per_second: 1,
            burst_size: 1,
        });
        let service = RateLimitedRpcService {
            service: AlwaysOkRpcService,
            per_conn_limiter: per_conn,
            global_limiter: None,
        };

        let first = service.call(rpc_request_with_conn(1, 42)).await;
        assert!(first.is_success());

        let second = service.call(rpc_request_with_conn(2, 42)).await;
        assert_eq!(second.as_error_code(), Some(crate::error::codes::LIMIT_EXCEEDED));
        assert!(second.as_result().contains("rate limit exceeded"));
    }

    #[tokio::test]
    async fn per_connection_limiter_isolates_connections() {
        // Two connections each get their own bucket.
        let per_conn = PerConnectionRateLimiter::new(RateLimitConfig {
            requests_per_second: 1,
            burst_size: 1,
        });
        let service = RateLimitedRpcService {
            service: AlwaysOkRpcService,
            per_conn_limiter: per_conn,
            global_limiter: None,
        };

        // Connection 1: exhaust its bucket.
        let resp = service.call(rpc_request_with_conn(1, 1)).await;
        assert!(resp.is_success());
        let resp = service.call(rpc_request_with_conn(2, 1)).await;
        assert_eq!(resp.as_error_code(), Some(crate::error::codes::LIMIT_EXCEEDED));

        // Connection 2: should still be allowed (separate bucket).
        let resp = service.call(rpc_request_with_conn(3, 2)).await;
        assert!(resp.is_success());
    }

    #[tokio::test]
    async fn global_limiter_caps_aggregate_throughput() {
        // Even though per-connection allows the request, the global limiter
        // can reject it.
        let global =
            SharedRateLimiter::new(RateLimitConfig { requests_per_second: 1, burst_size: 1 });
        let service = RateLimitedRpcService {
            service: AlwaysOkRpcService,
            per_conn_limiter: None,
            global_limiter: global,
        };

        let first = service.call(rpc_request_with_conn(1, 1)).await;
        assert!(first.is_success());

        // Second request from a different connection is still blocked by global.
        let second = service.call(rpc_request_with_conn(2, 2)).await;
        assert_eq!(second.as_error_code(), Some(crate::error::codes::LIMIT_EXCEEDED));
    }

    /// A mock service that returns InternalError (-32603) for subscription
    /// methods, mimicking jsonrpsee's behaviour when subscriptions are called
    /// over HTTP.
    #[derive(Debug, Clone)]
    struct InternalErrorOnSubscriptionService;

    impl<'a> RpcServiceT<'a> for InternalErrorOnSubscriptionService {
        type Future = std::future::Ready<MethodResponse>;

        fn call(&self, request: RpcRequest<'a>) -> Self::Future {
            let id = request.id().into_owned();
            if is_subscription_method(request.method_name()) {
                std::future::ready(MethodResponse::error(
                    id,
                    ErrorObjectOwned::owned(codes::INTERNAL_ERROR, "Internal error", None::<()>),
                ))
            } else {
                std::future::ready(MethodResponse::response(
                    id,
                    ResponsePayload::success("ok"),
                    usize::MAX,
                ))
            }
        }
    }

    #[tokio::test]
    async fn subscription_over_http_returns_method_not_supported() {
        let service = RateLimitedRpcService {
            service: InternalErrorOnSubscriptionService,
            per_conn_limiter: None,
            global_limiter: None,
        };

        // eth_subscribe should be rewritten from -32603 to -32004.
        let sub_req = RpcRequest::new(Cow::Borrowed("eth_subscribe"), None, Id::Number(1));
        let response = service.call(sub_req).await;
        assert_eq!(response.as_error_code(), Some(codes::METHOD_NOT_SUPPORTED));
        assert!(response.as_result().contains("Subscriptions are not available over HTTP"));
    }

    #[tokio::test]
    async fn subscription_over_ws_passes_through() {
        // When the inner service returns success (WebSocket case), the
        // middleware must not interfere.
        let service = RateLimitedRpcService {
            service: AlwaysOkRpcService,
            per_conn_limiter: None,
            global_limiter: None,
        };

        let sub_req = RpcRequest::new(Cow::Borrowed("eth_subscribe"), None, Id::Number(1));
        let response = service.call(sub_req).await;
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn non_subscription_internal_error_not_rewritten() {
        // An InternalError on a regular method must NOT be rewritten.
        let service = RateLimitedRpcService {
            service: InternalErrorOnSubscriptionService,
            per_conn_limiter: None,
            global_limiter: None,
        };

        let req = rpc_request(1);
        let response = service.call(req).await;
        assert!(response.is_success());
    }

    #[tokio::test]
    async fn http_status_rate_limiter_returns_too_many_requests() {
        let rate_limiter =
            SharedRateLimiter::new(RateLimitConfig { requests_per_second: 1, burst_size: 1 });
        let app = build_http_router(
            Arc::new(NodeState::new(1, 0)),
            build_cors_layer(&CorsConfig::none()),
            10,
            rate_limiter,
        );

        let first = app
            .clone()
            .oneshot(HttpRequest::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(HttpRequest::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
