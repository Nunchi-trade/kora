# PR #74: RPC Rate Limiting

## Problem

The RPC server had no mechanism to limit the rate of incoming requests. A single
client (or a bot) could send an unlimited number of requests per second, which
risked overwhelming the node, exhausting resources, and degrading service for all
other clients. This applied to both the HTTP status endpoints (`/status`,
`/health`) and the JSON-RPC transport layer (Ethereum API calls over WebSocket or
HTTP).

## Solution

This PR introduces a global token-bucket rate limiter that enforces a
configurable requests-per-second cap with burst tolerance. The limiter is applied
at two layers:

1. **HTTP middleware** -- an Axum middleware intercepts every request to the HTTP
   status endpoints. If the bucket is exhausted, the server returns
   `429 Too Many Requests` before the handler runs.

2. **JSON-RPC middleware** -- a `jsonrpsee` RPC service wrapper intercepts every
   JSON-RPC call. If the bucket is exhausted, the server returns a standard
   Ethereum JSON-RPC error with code `-32005` (`LIMIT_EXCEEDED`) and the message
   `"rate limit exceeded"`.

Each layer maintains its own independent token bucket so that HTTP and RPC
traffic are rate-limited separately.

### Token Bucket Algorithm

- The bucket starts full at `burst_size` tokens.
- Each request consumes one token.
- Tokens are replenished at `requests_per_second` per second, up to the
  `burst_size` cap.
- If no tokens are available, the request is rejected immediately (no queuing).
- A `burst_size` of 0 is automatically clamped to 1 so that an enabled limiter
  can always admit at least one request. Without this, a burst of 0 would start
  with 0 tokens and never refill, permanently blocking all traffic.
- A `requests_per_second` of 0 means "reject everything" -- the bucket is
  initialized empty and never refills.

### Configuration

Rate limiting is configured through `RateLimitConfig`:

- **Default**: 100 requests/second, burst size of 200.
- **Disabled**: `RateLimitConfig::disabled()` sets both values to `u64::MAX`,
  which causes `SharedRateLimiter::new` to return `None`, bypassing the limiter
  entirely with zero overhead.

Additionally, this PR surfaces the `max_subscriptions_per_connection` setting
(defaulting to 32) through `RpcServerConfig`, `RpcServer`, and `JsonRpcServer`
builder APIs, and passes it to the `jsonrpsee` server builder.

## Files Modified

### `crates/node/rpc/Cargo.toml`
- Added `"util"` to the `tower` dependency features. This is needed for the
  `ServiceExt::oneshot` method used in the HTTP rate-limiting tests.

### `crates/node/rpc/src/config.rs`
- Added `max_subscriptions_per_connection` field to `RpcServerConfig` (default
  32).
- Added `with_rate_limit_burst(requests_per_second, burst_size)` builder method
  to `RpcServerConfig` for configuring both rate and burst together.
- Added `with_max_subscriptions_per_connection` builder method.
- Updated `RateLimitConfig` doc comment to clarify the rate limit is
  server-wide, not per-client.
- Added `RateLimitConfig::is_disabled()` helper.
- Added tests for all new builder methods, the chained builder, and the
  `is_disabled` predicate.

### `crates/node/rpc/src/server.rs`
- Added `SharedRateLimiter` -- a thread-safe wrapper around an
  `Arc<Mutex<TokenBucket>>` that returns `None` when rate limiting is disabled.
- Added `TokenBucket` -- the core rate-limiting state machine with `const fn`
  construction, deterministic `try_acquire_at(Instant)` for testability, and
  internal refill logic.
- Added `rate_limit_allows()` helper that treats `None` as "always allow."
- Added `rate_limited_rpc_response()` to build the standard JSON-RPC error.
- Added `enforce_http_rate_limit` Axum middleware function.
- Added `RateLimitedRpcService<S>` implementing `jsonrpsee::RpcServiceT`.
- Extracted `build_http_router()` to a standalone function (makes both
  production code and tests cleaner).
- Threaded `rate_limit_config` and `max_subscriptions_per_connection` through
  `RpcServer` and `JsonRpcServer` constructors, builders, `from_config`, and
  `start` methods.
- Added `Debug` fields for the new config values.
- Added comprehensive unit tests:
  - Token bucket burst and refill behavior
  - `burst_size=0` clamping
  - `requests_per_second=0` rejection
  - Burst cap enforcement
  - Disabled limiter produces `None`
  - `rate_limit_allows` with no limiter
  - Config threading through `RpcServer::from_config`
  - Config threading through `JsonRpcServer` builders
  - RPC-layer rate limiting with mock service
  - HTTP-layer rate limiting with `tower::ServiceExt::oneshot`

## Breaking Changes

- `RpcServerConfig` has a new public field `max_subscriptions_per_connection`.
  Code that constructs `RpcServerConfig` with struct literal syntax (rather than
  the builder methods) will need to add this field.
- `RpcServer` and `JsonRpcServer` now carry `rate_limit_config` and
  `max_subscriptions_per_connection` fields internally. This does not affect
  public API since these structs are constructed via methods, not struct
  literals.

## Migration

- No changes required for existing callers that use the builder API or
  `Default` -- the defaults (100 rps, burst 200, 32 subscriptions) are applied
  automatically.
- To opt out of rate limiting, call `.with_rate_limit_config(RateLimitConfig::disabled())`.
- To tune the limits, use `.with_rate_limit_burst(rps, burst)` on the config
  or `.with_rate_limit_config(...)` on the server.

## Testing

The following test cases cover the rate-limiting implementation:

| Test | What it verifies |
|------|-----------------|
| `token_bucket_honors_burst_and_refill` | Burst consumption and time-based refill |
| `token_bucket_clamps_zero_burst_to_one` | `burst_size=0` is clamped to 1; first request succeeds |
| `token_bucket_zero_rps_rejects_all` | `requests_per_second=0` rejects everything, even after time passes |
| `token_bucket_does_not_exceed_burst` | Tokens never accumulate beyond `burst_size` |
| `disabled_rate_limit_does_not_build_limiter` | `RateLimitConfig::disabled()` produces `None` |
| `rate_limit_allows_with_no_limiter` | `rate_limit_allows(&None)` returns `true` |
| `rate_limit_config_default_is_not_disabled` | Default config is not considered disabled |
| `rpc_server_from_config_threads_limits` | `RpcServer::from_config` propagates all limit fields |
| `json_rpc_server_builders_thread_limits` | `JsonRpcServer` builder methods propagate all limit fields |
| `rpc_rate_limiter_rejects_after_burst` | JSON-RPC middleware returns `-32005` after burst is exhausted |
| `http_status_rate_limiter_returns_too_many_requests` | HTTP middleware returns `429` after burst is exhausted |
| `rpc_server_config_with_rate_limit_burst` | Config builder sets both rps and burst |
| `rpc_server_config_with_max_subscriptions_per_connection` | Config builder sets subscription limit |
| `rpc_server_config_chained_builder` | Full builder chain applies all settings |
