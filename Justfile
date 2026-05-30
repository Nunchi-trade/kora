# List available recipes
default:
    @just --list

# Run the full CI suite
ci: fmt build-all-locked clippy test test-e2e test-doc deny

# Run all checks
check: fmt build-all-locked clippy test test-e2e test-doc

# Run non-e2e tests
test:
    cargo nextest run --workspace --all-features --exclude kora-e2e --no-tests=pass

# Run e2e tests serially
test-e2e:
    cargo nextest run -p kora-e2e --all-features --run-ignored all -j1 --no-tests=fail

# Run doc tests
test-doc:
    cargo test --workspace --all-features --doc

# Build in release mode
build:
    cargo build --release

# Build all targets
build-all:
    cargo build --workspace --all-targets

# Build all targets with the checked-in lockfile
build-all-locked:
    cargo build --workspace --all-targets --locked

# Check formatting
fmt:
    cargo +nightly fmt --all -- --check

# Fix formatting
fmt-fix:
    cargo +nightly fmt --all

# Run clippy
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# Run cargo deny
deny:
    .github/ensure-cargo-deny.sh
    cargo deny check

# Clean build artifacts
clean:
    cargo clean

# Start the devnet with interactive DKG (production-like)
devnet:
    cd docker && just devnet

# Start the devnet with trusted dealer DKG (fast, insecure, for local dev)
trusted-devnet:
    cd docker && just trusted-devnet

# Stop the devnet
devnet-down:
    cd docker && just down

# Reset devnet (clears all state, requires fresh DKG)
devnet-reset:
    cd docker && just reset

# View devnet logs
devnet-logs:
    cd docker && just logs

# View devnet status
devnet-status:
    cd docker && just status

# Live devnet monitoring dashboard
devnet-stats:
    cd docker && just stats

# Devnet health diagnostics report
devnet-health:
    cd docker && just health

# Build docker images
docker-build:
    cd docker && just build

# Run load generator against devnet
loadgen *args:
    cargo run --release -p loadgen --bin loadgen -- {{args}}

# Quick load test (1000 txs)
loadtest:
    cargo run --release -p loadgen --bin loadgen -- --total-txs 1000 --broadcast-rpc-urls http://127.0.0.1:8546,http://127.0.0.1:8547,http://127.0.0.1:8548

# Stress test (10000 txs with 50 accounts)
stresstest:
    cargo run --release -p loadgen --bin loadgen -- --total-txs 10000 --accounts 50 --broadcast-rpc-urls http://127.0.0.1:8546,http://127.0.0.1:8547,http://127.0.0.1:8548

# Provision the remote server (one-time)
remote-provision:
    cd ansible && ansible-playbook playbooks/provision.yml

# Deploy to remote server
remote-deploy *args:
    cd ansible && ansible-playbook playbooks/deploy.yml {{args}}

# Reset remote devnet (clean slate)
remote-reset:
    cd ansible && ansible-playbook playbooks/reset.yml

# Start observability on remote
remote-observe:
    cd ansible && ansible-playbook playbooks/observe.yml
