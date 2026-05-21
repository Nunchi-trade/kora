# Loadgen

[![MIT License](https://img.shields.io/badge/License-MIT-a78bfa.svg?style=flat&labelColor=1C2C2E)](../../LICENSE)

Load generator for Kora devnet. Generates and submits signed EIP-1559 transactions at high throughput using concurrent execution.

## Usage

```sh
# Start the devnet first
just trusted-devnet

# Run load generator with 1000 transactions
cargo run --release --bin loadgen -- --total-txs 1000

# High concurrency stress test
cargo run --release --bin loadgen -- --total-txs 10000 --concurrency 100 --accounts 50

# Target specific RPC endpoint
cargo run --release --bin loadgen -- --rpc-url http://localhost:8546 --total-txs 5000

# Broadcast each transaction to all validator RPCs in a multi-validator devnet
cargo run --release --bin loadgen -- \
  --rpc-url http://localhost:8545 \
  --broadcast-rpc-urls http://localhost:8546,http://localhost:8547,http://localhost:8548 \
  --total-txs 10000 --accounts 50

# Dry run (test tx signing performance only)
cargo run --release --bin loadgen -- --total-txs 10000 --dry-run
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc-url` | `http://127.0.0.1:8545` | RPC endpoint URL |
| `--broadcast-rpc-urls` | none | Additional comma-separated RPC endpoint URLs to broadcast each transaction to |
| `--accounts` | `10` | Number of sender accounts, from 1 to 255 |
| `--total-txs` | `1000` | Total transactions to send |
| `--concurrency` | `50` | Maximum concurrent in-flight requests |
| `--chain-id` | `1337` | Chain ID for transactions |
| `--dry-run` | `false` | Sign transactions without sending |
| `--verbose` | `false` | Print each transaction hash |

## Notes

Standard `keygen setup` devnet genesis output funds the default loadgen seed range, currently accounts 1 through 50. The default `--accounts 10` and the common `--accounts 50` stress-test configuration work against a fresh trusted devnet without manually funding sender accounts.

If you run with non-standard accounts above the funded default range, such as `--accounts 75`, the additional seed accounts need to be included in genesis with sufficient balance or funded manually before loadgen transactions can execute successfully.

In multi-validator devnets, pass every validator RPC endpoint through `--rpc-url` and `--broadcast-rpc-urls`. Devnet mempools are validator-local, so broadcasting gives the active proposer a copy of each transaction.

Sender addresses are deterministically generated from seed bytes:
- Account 1: seed `[0,0,...,0,1]`
- Account 2: seed `[0,0,...,0,2]`
- etc.

The loadgen outputs the sender addresses at startup so you can verify which genesis allocations or manual transfers are needed for custom account ranges.

## Performance

The loadgen uses:
- `FuturesUnordered` for concurrent request handling
- Connection pooling via `reqwest`
- Atomic nonce tracking for parallel account access
- Arc-wrapped accounts for thread-safe sharing
