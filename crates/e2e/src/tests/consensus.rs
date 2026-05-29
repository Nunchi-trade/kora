//! Consensus-related e2e tests.
//!
//! These tests verify that validators can reach consensus, finalize blocks,
//! and maintain consistent state across the network.

use crate::{TestConfig, TestHarness, TestSetup};

/// Test that a 4-validator network can finalize blocks.
#[test]
fn test_four_validators_reach_consensus() {
    let config = TestConfig::default().with_validators(4).with_max_blocks(3);
    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    assert_eq!(outcome.blocks_finalized, 3);
    assert!(outcome.node_finalization_counts.iter().all(|&c| c >= 3));
}

/// Test that a 7-validator network can finalize blocks (larger quorum).
#[test]
fn test_seven_validators_reach_consensus() {
    let config = TestConfig::default()
        .with_validators(7)
        .with_max_blocks(3)
        .with_seed(123)
        .with_timeout(std::time::Duration::from_secs(120));
    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    assert_eq!(outcome.blocks_finalized, 3);
}

/// Test that all nodes agree on the same state root.
#[test]
fn test_state_root_convergence() {
    let config = TestConfig::default().with_validators(4).with_max_blocks(5);
    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    // The harness already verifies state convergence internally
    // If we got here, all nodes have the same state root
    assert_eq!(outcome.blocks_finalized, 5);
}

/// Test that all nodes agree on the same seed (prevrandao).
#[test]
fn test_seed_convergence() {
    let config = TestConfig::default().with_validators(4).with_max_blocks(3);
    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    // Seeds are verified during state convergence check
    // A non-zero seed indicates threshold signature aggregation worked
    assert_ne!(outcome.seed, alloy_primitives::B256::ZERO);
}

/// Test that blocks are produced in sequence without gaps.
#[test]
fn test_sequential_block_production() {
    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(10)
        .with_timeout(std::time::Duration::from_secs(60));
    let setup = TestSetup::empty();

    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    assert_eq!(outcome.blocks_finalized, 10);
}

/// Test with different random seeds for reproducibility.
#[test]
fn test_deterministic_with_seed() {
    let config = TestConfig::default().with_validators(4).with_max_blocks(3).with_seed(42);
    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome1 = TestHarness::run(config.clone(), setup.clone()).expect("first run");
    let outcome2 = TestHarness::run(config, setup).expect("second run");

    // With the same seed, we should get the same state root
    assert_eq!(outcome1.state_root, outcome2.state_root);
}

/// Test that empty blocks (no transactions) can be finalized.
#[test]
fn test_empty_blocks() {
    let config = TestConfig::default().with_validators(4).with_max_blocks(5);
    let setup = TestSetup::empty();

    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    assert_eq!(outcome.blocks_finalized, 5);
}

/// Test minimum viable network (4 validators, threshold 3).
#[test]
fn test_minimum_quorum() {
    // 4 validators with threshold 3 is the minimum for BFT
    let config = TestConfig::default().with_validators(4).with_max_blocks(3);
    assert_eq!(config.threshold, 3);

    let setup = TestSetup::simple_transfer(config.chain_id);
    let outcome = TestHarness::run(config, setup).expect("consensus should succeed");

    assert_eq!(outcome.blocks_finalized, 3);
}

/// Test that transactions affect balances correctly after finalization.
#[test]
fn test_balance_updates_after_finalization() {
    let config = TestConfig::default().with_validators(4).with_max_blocks(3);
    let setup = TestSetup::simple_transfer(config.chain_id);

    // The harness verifies expected balances internally
    let outcome = TestHarness::run(config, setup).expect("balances should match");

    assert_eq!(outcome.blocks_finalized, 3);
}
