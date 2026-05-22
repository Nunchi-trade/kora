//! HTTP and JSON-RPC server implementation.

use std::{
    net::SocketAddr,
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
        Server, ServerHandle,
        middleware::rpc::{ResponseFuture, RpcServiceBuilder, RpcServiceT},
    },
    types::{ErrorObjectOwned, Id, Request as RpcRequest},
};
use kora_txpool::TransactionPool;
use parking_lot::Mutex;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, info};

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

fn rate_limit_allows(rate_limiter: &Option<SharedRateLimiter>) -> bool {
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
    if !rate_limit_allows(&rate_limiter) {
        return (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response();
    }

    next.run(request).await
}

#[derive(Debug, Clone)]
struct RateLimitedRpcService<S> {
    service: S,
    rate_limiter: Option<SharedRateLimiter>,
}

impl<'a, S> RpcServiceT<'a> for RateLimitedRpcService<S>
where
    S: RpcServiceT<'a> + Clone + Send + Sync + 'static,
{
    type Future = ResponseFuture<S::Future>;

    fn call(&self, request: RpcRequest<'a>) -> Self::Future {
        if rate_limit_allows(&self.rate_limiter) {
            ResponseFuture::future(self.service.call(request))
        } else {
            ResponseFuture::ready(rate_limited_rpc_response(request.id().into_owned()))
        }
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
        let rpc_rate_limiter = SharedRateLimiter::new(self.rate_limit_config);
        let max_connections = self.max_connections;
        let max_subscriptions_per_connection = self.max_subscriptions_per_connection;
        let state_provider = self.state_provider;
        let peer_count = self.peer_count;

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
            let rpc_middleware = RpcServiceBuilder::new().layer_fn(move |service| {
                RateLimitedRpcService { service, rate_limiter: rpc_rate_limiter.clone() }
            });

            let server = match Server::builder()
                .max_connections(max_connections)
                .max_subscriptions_per_connection(max_subscriptions_per_connection)
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

            let eth_api = tx_submit.map_or_else(
                || EthApiImpl::new(chain_id, state_provider.clone()),
                |submit| EthApiImpl::with_tx_submit(chain_id, state_provider.clone(), submit),
            );
            let net_api = NetApiImpl::new(chain_id);
            net_api.set_peer_count(peer_count);
            let web3_api = Web3ApiImpl::new();
            let kora_api = KoraApiImpl::new(node_state_for_jsonrpc);

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
}

impl<S: StateProvider> std::fmt::Debug for JsonRpcServer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonRpcServer")
            .field("addr", &self.addr)
            .field("chain_id", &self.chain_id)
            .field("tx_submit", &self.tx_submit.is_some())
            .field("txpool", &self.txpool.is_some())
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
        let rpc_rate_limiter = SharedRateLimiter::new(self.rate_limit_config);
        let rpc_middleware = RpcServiceBuilder::new().layer_fn(move |service| {
            RateLimitedRpcService { service, rate_limiter: rpc_rate_limiter.clone() }
        });

        let server = Server::builder()
            .max_connections(self.max_connections)
            .max_subscriptions_per_connection(self.max_subscriptions_per_connection)
            .set_rpc_middleware(rpc_middleware)
            .build(self.addr)
            .await
            .map_err(|e| ServerError::Build(e.to_string()))?;

        let eth_api = self.tx_submit.map_or_else(
            || EthApiImpl::new(self.chain_id, self.state_provider.clone()),
            |submit| EthApiImpl::with_tx_submit(self.chain_id, self.state_provider.clone(), submit),
        );
        let net_api = NetApiImpl::new(self.chain_id);
        net_api.set_peer_count(self.peer_count);
        let web3_api = Web3ApiImpl::new();

        let mut module = jsonrpsee::RpcModule::new(());
        module.merge(eth_api.into_rpc())?;
        module.merge(net_api.into_rpc())?;
        module.merge(web3_api.into_rpc())?;
        if let Some(txpool) = self.txpool {
            module.merge(TxpoolApiImpl::new(txpool).into_rpc())?;
        }

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
    }

    #[test]
    fn rate_limit_allows_with_no_limiter() {
        assert!(rate_limit_allows(&None));
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
        let rate_limiter =
            SharedRateLimiter::new(RateLimitConfig { requests_per_second: 1, burst_size: 1 });
        let service = RateLimitedRpcService { service: AlwaysOkRpcService, rate_limiter };

        let first = service.call(rpc_request(1)).await;
        assert!(first.is_success());

        let second = service.call(rpc_request(2)).await;
        assert_eq!(second.as_error_code(), Some(crate::error::codes::LIMIT_EXCEEDED));
        assert!(second.as_result().contains("rate limit exceeded"));
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
