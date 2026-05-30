# `kora-config`

<a href="https://github.com/refcell/kora/actions/workflows/ci.yml"><img src="https://github.com/refcell/kora/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
<a href="https://github.com/refcell/kora/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-MIT-d1d1f6.svg" alt="License"></a>

Configuration types for Kora node.

This crate provides serializable configuration structures for all node components,
supporting both TOML (default) and JSON formats.

## Configuration Schema

```toml
[node]
chain_id = 1
data_dir = "/var/lib/kora"

[consensus]
validator_key = "path/to/key"
threshold = 2
participants = ["pk1", "pk2", "pk3"]

[consensus.block_codec]
max_txs = 10000
max_tx_bytes = 8388608

[consensus.simplex]
replay_buffer_bytes = 16777216
write_buffer_bytes = 16777216
leader_timeout_secs = 1
certification_timeout_secs = 2
timeout_retry_secs = 2
fetch_timeout_secs = 5
activity_timeout_views = 20
skip_timeout_views = 10
fetch_concurrent = 8

[network]
listen_addr = "0.0.0.0:30303"
bootstrap_peers = ["peer1:30303", "peer2:30303"]

[execution]
gas_limit = 250000000

[rpc]
http_addr = "0.0.0.0:8545"
ws_addr = "0.0.0.0:8546"
```

## Usage

```rust,ignore
use kora_config::NodeConfig;
use std::path::Path;

// Load from TOML file
let config = NodeConfig::from_toml_file(Path::new("config.toml"))?;

// Or use defaults
let config = NodeConfig::default();

// Serialize back to TOML
let toml_str = config.to_toml()?;
```

## License

[MIT License](https://github.com/refcell/kora/blob/main/LICENSE)
