# PR #76: Docker Build and Runtime Healthcheck Configuration

## Summary

This PR adds a `HEALTHCHECK` instruction to the Dockerfile and extends the
root `.dockerignore` to exclude documentation files that inflate the Docker
build context. Together these changes ensure the Docker image carries its own
healthcheck contract and that builds transfer only the files needed to compile
the Rust binaries and package the runtime scripts.

## Problem

Before this change, health checking was defined only in the Compose file
(`docker/compose/devnet.yaml`). The Dockerfile itself had no `HEALTHCHECK`
instruction. This meant:

- Running the image with plain `docker run` (outside Compose) produced a
  container with no healthcheck -- orchestrators could not tell whether the
  node was ready.
- The healthcheck contract (which script to call, intervals, retries) was
  scattered across one file instead of being declared at the image level and
  optionally overridden by Compose.
- The root `.dockerignore` did not exclude markdown documentation, so every
  `*.md` file in the repository was copied into the build context, wasting
  time and bandwidth on files the build never uses.

## Solution

### Dockerfile HEALTHCHECK

A `HEALTHCHECK` instruction was added to the runtime stage of the Dockerfile:

```dockerfile
HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=30s \
    CMD /scripts/healthcheck.sh
```

- The timing parameters (`interval`, `timeout`, `retries`, `start_period`)
  match the values already used in the Compose `x-validator-common` anchor,
  so the two configurations stay in sync.
- The default healthcheck mode is `p2p` (set inside `healthcheck.sh` via
  `HEALTHCHECK_MODE:-p2p`), which checks that TCP port 30303 is listening.
- Compose services override this to `HEALTHCHECK_MODE=ready`, which checks
  both the `.ready` sentinel file and the P2P port.
- The `HEALTHCHECK_MODE` environment variable supports three modes:
  - `dkg` -- succeeds when `/data/share.key` and `/data/output.json` both
    exist (DKG ceremony completed).
  - `p2p` -- succeeds when port 30303 accepts TCP connections.
  - `ready` -- succeeds when `/data/.ready` exists AND port 30303 is up.

### .dockerignore extensions

The root `.dockerignore` now excludes markdown files while preserving the
`README.md` files that Rust crates embed via `include_str!("../README.md")`:

```
*.md
!README.md
!bin/**/README.md
!crates/**/README.md
!docker/README.md
```

This keeps the build context small without breaking `cargo doc` or crate-level
documentation.

## Files Modified

| File | Change |
|------|--------|
| `docker/Dockerfile` | Added `HEALTHCHECK` instruction with timing parameters and a comment documenting all three healthcheck modes. |
| `.dockerignore` | Added rules to exclude `*.md` files from the Docker build context while keeping `README.md` files needed by crate documentation. |

## Breaking Changes

None. Existing Compose-based workflows are unaffected because the Compose
healthcheck definition takes precedence over the Dockerfile `HEALTHCHECK`.
Users running the image directly with `docker run` will now get automatic
healthchecks (previously there were none), which is additive, not breaking.

## Migration

No migration steps are required. The change is fully backward-compatible.

## Testing

1. **Validate Compose configuration:**
   ```bash
   docker compose -f docker/compose/devnet.yaml config --quiet
   ```
   Should exit 0 with no output.

2. **Check Dockerfile syntax:**
   ```bash
   docker build --check -f docker/Dockerfile .
   ```
   Should report no warnings.

3. **Build the image locally:**
   ```bash
   cd docker && just build
   ```
   Confirm the build succeeds and does not copy unnecessary markdown files
   into the context (watch the "transferring context" line for size).

4. **Verify the healthcheck is embedded in the image:**
   ```bash
   docker inspect kora:local | jq '.[0].Config.Healthcheck'
   ```
   Should show the `HEALTHCHECK` configuration with the correct interval,
   timeout, retries, and start period.

5. **Run a standalone container and observe health status:**
   ```bash
   docker run -d --name kora-test kora:local
   # Wait ~40 seconds for start_period + interval
   docker inspect --format='{{.State.Health.Status}}' kora-test
   ```
   The status should transition from `starting` to `healthy` or `unhealthy`
   depending on whether the node is actually running.

6. **Run the devnet and verify validators become healthy:**
   ```bash
   cd docker && just trusted-devnet
   docker compose -f compose/devnet.yaml ps
   ```
   All validator and secondary nodes should show `healthy` status.
