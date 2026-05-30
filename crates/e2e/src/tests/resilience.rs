//! Resilience and edge case tests.
//!
//! These tests verify network behavior under adverse conditions,
//! stress testing, and edge cases.

use std::time::Duration;

use kora_transport_sim::SimLinkConfig;

use crate::{TestConfig, TestHarness, TestSetup};

/// Test with high network latency.
#[test]
fn test_high_latency_network() {
    let high_latency_link = SimLinkConfig {
        latency: Duration::from_millis(100),
        jitter: Duration::from_millis(20),
        success_rate: 1.0,
    };

    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(3)
        .with_link(high_latency_link)
        .with_timeout(Duration::from_secs(60));

    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("high latency should work");

    assert_eq!(outcome.blocks_finalized, 3);
}

/// Test with network jitter.
#[test]
fn test_network_jitter() {
    let jittery_link = SimLinkConfig {
        latency: Duration::from_millis(20),
        jitter: Duration::from_millis(15),
        success_rate: 1.0,
    };

    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(5)
        .with_link(jittery_link)
        .with_timeout(Duration::from_secs(60));

    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("jitter should be tolerated");

    assert_eq!(outcome.blocks_finalized, 5);
}

fn run_validator_count(n: usize) {
    let config = TestConfig::default().with_validators(n).with_max_blocks(3).with_seed(n as u64);

    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome =
        TestHarness::run(config, setup).unwrap_or_else(|e| panic!("{n} validators failed: {e}"));

    assert_eq!(outcome.blocks_finalized, 3, "Failed with {n} validators");
}

/// Test that consensus works with four validators.
#[test]
fn test_four_validator_count() {
    run_validator_count(4);
}

/// Test that consensus works with five validators.
#[test]
fn test_five_validator_count() {
    run_validator_count(5);
}

/// Test that consensus works with six validators.
#[test]
fn test_six_validator_count() {
    run_validator_count(6);
}

/// Test that consensus works with seven validators.
#[test]
fn test_seven_validator_count() {
    run_validator_count(7);
}

/// Test longer chains to detect state accumulation issues.
#[test]
fn test_longer_chain() {
    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(20)
        .with_timeout(Duration::from_secs(120));

    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("longer chain should work");

    assert_eq!(outcome.blocks_finalized, 20);
}

/// Test with more transactions over more blocks.
#[test]
fn test_sustained_throughput() {
    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(10)
        .with_timeout(Duration::from_secs(90));

    // Multiple transfers to stress the mempool
    let setup = TestSetup::multi_transfer(config.chain_id, 15);

    let outcome = TestHarness::run(config, setup).expect("sustained load should work");

    assert_eq!(outcome.blocks_finalized, 10);
}

/// Test that different seeds produce different (but valid) outcomes.
#[test]
fn test_different_seeds_different_paths() {
    let setup = TestSetup::simple_transfer(1337);
    let timeout = std::time::Duration::from_secs(45);

    let config1 = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(5)
        .with_seed(1)
        .with_timeout(timeout);
    let config2 = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(5)
        .with_seed(2)
        .with_timeout(timeout);

    let outcome1 = TestHarness::run(config1, setup.clone()).expect("seed 1");
    let outcome2 = TestHarness::run(config2, setup).expect("seed 2");

    // Both should finalize the same number of blocks
    assert_eq!(outcome1.blocks_finalized, outcome2.blocks_finalized);

    // But state roots should be the same because same genesis + txs
    // (seeds affect consensus path, not execution determinism)
    assert_eq!(outcome1.state_root, outcome2.state_root);
}

/// Stress test with maximum transactions.
#[test]
fn test_stress_max_transactions() {
    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(5)
        .with_timeout(Duration::from_secs(120));

    // Near maximum tx count (block codec allows 64)
    let setup = TestSetup::multi_transfer(config.chain_id, 50);

    let outcome = TestHarness::run(config, setup).expect("stress test should pass");

    assert_eq!(outcome.blocks_finalized, 5);
}

/// Stress test with many blocks.
#[test]
fn test_stress_many_blocks() {
    let config = TestConfig::default()
        .with_validators(4)
        .with_max_blocks(50)
        .with_timeout(Duration::from_secs(300));

    let setup = TestSetup::simple_transfer(config.chain_id);

    let outcome = TestHarness::run(config, setup).expect("many blocks should work");

    assert_eq!(outcome.blocks_finalized, 50);
}
