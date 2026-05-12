# Public Testnet Standup

This runbook describes how to stand up a public Kora testnet from the same
building blocks used by the Docker devnet. It assumes the first public testnet
uses validators with stable public IP addresses or public DNS names.

Kora is pre-alpha software. Treat this as a testnet procedure, not a production
mainnet security guide.

## Overview

The local devnet starts four validators in one Docker Compose network. That
works because the generated peer file can use Docker hostnames such as
`node0:30303`. A public testnet needs the same artifacts, but the peer file must
use addresses that every validator can dial from the public internet.

For the first public testnet:

- Every validator operator provides a stable P2P endpoint, preferably DNS such
  as `validator-0.testnet.kora.network:30303`, or a static public IP such as
  `203.0.113.10:30303`.
- Every validator opens the P2P port on that endpoint.
- All validators use the same finalized `peers.json`.
- Validators run the interactive DKG ceremony before starting consensus.

Future iterations can document a private validator mesh using ZeroTier or a
similar VPN. A later P2P design could also explore iroh. Those are follow-up
designs and should not block the public-IP standup path described here.

## Current Devnet Primitives

The public testnet flow reuses the existing commands and files:

- `keygen setup` generates `genesis.json`, `peers.json`, per-validator
  `validator.key` files, and optional secondary identities.
- `kora dkg --peers <peers.json>` runs the interactive DKG ceremony and writes
  `share.key` and `output.json` into each validator data directory.
- `kora validator --peers <peers.json>` starts a validator after DKG has
  completed.
- `genesis.json` contains chain state only. It does not contain P2P endpoints.
- `peers.json` contains `participants`, `secondary_participants`, `threshold`,
  and `bootstrappers`.

The important difference from the Docker devnet is the `bootstrappers` section
in `peers.json`. `keygen setup` currently writes Docker-local addresses like
`node0:30303`; for a public testnet, replace them with public DNS names or
public IP addresses before running DKG.

## Prerequisites

Choose and record these values before generating artifacts:

- Validator count, for example `4`.
- Threshold, for example `3` for a 4-validator testnet.
- Chain ID, for example a testnet-specific value distinct from local devnets.
- Public P2P endpoint for each validator.
- Optional secondary peer count.
- A shared release artifact or Docker image version that all validators will
  run.

Each validator host should have:

- A static public IP address or stable DNS record.
- Inbound TCP open for the Kora P2P port, default `30303`.
- Outbound TCP allowed to every other validator P2P endpoint.
- NTP or another reliable clock sync service.
- Persistent disk for the Kora data directory.
- Log collection and a restart supervisor such as systemd, Docker restart
  policy, or equivalent.

RPC is currently started by the validator command on `0.0.0.0:8545`. Do not
leave RPC open to the internet unless that is an intentional testnet policy.
Prefer firewalling RPC to operator IPs, a bastion, or a public load balancer
that you explicitly control.

## Address Handling

Use public DNS names when possible:

```text
validator-0.testnet.kora.network:30303
validator-1.testnet.kora.network:30303
validator-2.testnet.kora.network:30303
validator-3.testnet.kora.network:30303
```

Static public IPs are also valid:

```text
203.0.113.10:30303
203.0.113.11:30303
203.0.113.12:30303
203.0.113.13:30303
```

The address in `peers.json` must be the address other validators can dial. Do
not use `0.0.0.0`, `127.0.0.1`, Docker service names, or private cloud
addresses unless every validator is intentionally on the same private network.

The node listen address can remain the default `0.0.0.0:30303`, which means
"listen on all local interfaces." The public endpoint belongs in `peers.json`.
If a host is behind NAT, the public `host:port` must forward to the local Kora
P2P listener.

## Artifact Layout

Use one coordinator directory while preparing the network:

```text
testnet-artifacts/
  genesis.json
  peers.json
  node0/
    validator.key
    setup.json
  node1/
    validator.key
    setup.json
  node2/
    validator.key
    setup.json
  node3/
    validator.key
    setup.json
  secondary0/
    validator.key
    setup.json
```

After DKG, each validator directory also contains:

```text
share.key
output.json
```

Artifact ownership:

- Share `genesis.json` with every validator and secondary operator.
- Share the finalized `peers.json` with every validator and secondary operator.
- Give each validator operator only its own `nodeN/validator.key`.
- After DKG, keep each `nodeN/share.key` private to that validator.
- `output.json` is required for the validator to start and should be kept with
  that validator's data directory.
- Do not publish `validator.key` or `share.key`.

The current `keygen setup` command generates validator identity keys centrally.
For this first testnet, that means the coordinator must distribute each private
key securely. A later tooling improvement should support operator-generated
identity keys or an endpoint/participant manifest so operators do not need to
receive private identity material from a coordinator.

## Generate Initial Artifacts

From a trusted coordinator machine:

```sh
cargo run --release -p keygen -- setup \
  --validators 4 \
  --secondary-peers 1 \
  --threshold 3 \
  --chain-id 424242 \
  --output-dir ./testnet-artifacts
```

Then edit `testnet-artifacts/peers.json` and replace the generated
Docker-local bootstrapper addresses with public endpoints.

Example shape:

```json
{
  "validators": 4,
  "threshold": 3,
  "participants": [
    "<validator-0-public-key>",
    "<validator-1-public-key>",
    "<validator-2-public-key>",
    "<validator-3-public-key>"
  ],
  "secondary_participants": [
    "<secondary-0-public-key>"
  ],
  "bootstrappers": {
    "<validator-0-public-key>": "validator-0.testnet.kora.network:30303",
    "<validator-1-public-key>": "validator-1.testnet.kora.network:30303",
    "<validator-2-public-key>": "validator-2.testnet.kora.network:30303",
    "<validator-3-public-key>": "validator-3.testnet.kora.network:30303"
  }
}
```


## Run Interactive DKG

Interactive DKG is the preferred testnet ceremony because no single party
generates all BLS threshold shares. The trusted dealer command is only for local
development and should not be used for public testnet keys.

Before the ceremony, each validator host should have:

```text
/var/lib/kora/
  validator.key
  genesis.json
  peers.json
```

Start DKG on every validator using the same chain ID and finalized peer file:

```sh
kora \
  --data-dir /var/lib/kora \
  --chain-id 424242 \
  dkg \
  --peers /var/lib/kora/peers.json
```

All validators need to be online and reachable for the ceremony. A successful
ceremony writes:

```text
/var/lib/kora/share.key
/var/lib/kora/output.json
```

If the ceremony fails, inspect validator logs, confirm every public endpoint is
reachable, confirm every operator has the same `peers.json`, and rerun DKG only
after deciding whether to preserve or clear partial DKG state. Use
`--force-restart` only when every operator agrees to restart the ceremony.

## Start Validators

After DKG, every validator data directory should contain:

```text
/var/lib/kora/
  genesis.json
  peers.json
  validator.key
  share.key
  output.json
```

Start each validator:

```sh
kora \
  --data-dir /var/lib/kora \
  --chain-id 424242 \
  validator \
  --peers /var/lib/kora/peers.json
```

For a systemd deployment, use the same command in a unit file and set a restart
policy appropriate for a testnet. Keep the data directory on persistent storage.

The existing single-host Docker Compose file is not a public testnet deployment
template. It is useful as a reference for local devnet behavior, but public
testnet operators should use a per-host service definition or a future per-host
compose template.

## Start Secondary Peers

Secondary peers are authenticated P2P participants that follow validator traffic
without participating in consensus. Their public keys must already be listed in
`secondary_participants`.

Prepare the secondary data directory with its own `validator.key` plus the
shared `peers.json`:

```text
/var/lib/kora-secondary/
  validator.key
  peers.json
```

Start the secondary:

```sh
kora \
  --data-dir /var/lib/kora-secondary \
  --chain-id 424242 \
  secondary \
  --peers /var/lib/kora-secondary/peers.json
```

## Validation Checklist

Before announcing the testnet:

- Every validator can resolve every validator DNS name, if DNS is used.
- Every validator can open TCP connections to every other validator P2P
  endpoint.
- Every validator has the same `genesis.json` and finalized `peers.json`.
- Every validator has its own `validator.key`, `share.key`, and `output.json`.
- Validators start without DKG-output errors.
- Logs show peer connections and consensus progress.
- At least one controlled RPC endpoint responds on the expected chain ID.
- Metrics and logs are visible to the testnet operators.
- RPC and metrics exposure match the intended firewall policy.

## Operations

Recommended minimum operating practices:

- Keep `validator.key` and `share.key` backed up securely.
- Keep `genesis.json` and the finalized `peers.json` in versioned release
  artifacts so operators can verify they are running the intended network.
- Use DNS records with low enough TTLs to recover from host replacement.
- Monitor process restarts, disk usage, peer connectivity, block production,
  RPC health, and host clock drift.
- Restrict SSH, RPC, metrics, and dashboards. Only the P2P port needs to be
  broadly reachable by other validators.

## Reset Or Re-DKG

Changing validator identities, validator count, threshold, or DKG output creates
a new network ceremony. Coordinate resets explicitly:

1. Stop validators.
2. Decide whether the existing chain data is being discarded.
3. Generate or agree on the new `peers.json`.
4. Clear old `share.key`, `output.json`, and partial DKG state from each
   validator data directory if the ceremony is being restarted.
5. Run interactive DKG again.
6. Restart validators with the new artifacts.

Do not mix old and new `peers.json`, `share.key`, or `output.json` files across
validators.

## Future Improvements

The current flow can stand up a public-IP testnet, but the rough edges are worth
tracking:

- Add a `keygen setup` option that accepts an endpoint manifest and writes public
  bootstrappers directly, avoiding manual `peers.json` edits.
- Add a flow for operator-generated validator identity public keys so the
  coordinator does not create or distribute `validator.key` files.
- Add a per-host systemd or Docker Compose template for validators and
  secondaries.
- Document a ZeroTier-based private validator mesh for closed rehearsals.
- Evaluate whether iroh can simplify future P2P connectivity and NAT traversal.
- Make RPC bind address configurable for the validator command, or document the
  exact firewall/reverse-proxy pattern used by the public testnet.
