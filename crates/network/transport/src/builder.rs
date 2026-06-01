//! Transport builder.

use std::num::NonZeroU32;

use commonware_cryptography::Signer;
use commonware_p2p::authenticated::discovery;
use commonware_runtime::{
    BufferPooler, Clock, Metrics, Network as RNetwork, Quota, Resolver, Spawner,
};
use rand_core::CryptoRngCore;

use crate::{
    channels::{
        CHANNEL_BACKFILL, CHANNEL_BLOCKS, CHANNEL_CERTS, CHANNEL_RESOLVER, CHANNEL_TX_GOSSIP,
        CHANNEL_VOTES, MarshalChannels, SimplexChannels, TxGossipChannel,
    },
    config::TransportConfig,
    transport::NetworkTransport,
};

/// Default rate quota for consensus channels (3000 messages per second).
/// Consensus channels (votes, certs) carry high-frequency trusted traffic
/// and need generous headroom for view changes and recovery bursts.
const fn default_consensus_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(3000).expect("3000 is non-zero"))
}

/// Default rate quota for block channels (100 messages per second).
/// Block/backfill channels carry large, infrequent messages -- a low
/// message-count limit prevents bandwidth abuse.
const fn default_block_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(100).expect("100 is non-zero"))
}

/// Default rate quota for resolver/sync channels (500 messages per second).
const fn default_sync_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(500).expect("500 is non-zero"))
}

/// Default rate quota for transaction gossip channel (200 messages per second).
/// Gossip carries untrusted external content and is the most abuse-prone
/// channel, so it gets the most restrictive quota.
const fn default_gossip_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(200).expect("200 is non-zero"))
}

impl<C: Signer> TransportConfig<C> {
    /// Build the network transport.
    ///
    /// This creates the authenticated discovery network, registers all channels,
    /// and starts the network. Returns a [`NetworkTransport`] containing
    /// everything needed for consensus and block dissemination.
    ///
    /// # Parameters
    ///
    /// * `context` - Runtime context for spawning network tasks.
    ///
    /// # Returns
    ///
    /// A [`NetworkTransport`] containing:
    /// - Oracle for peer management
    /// - All channel pairs grouped by consumer
    /// - Network handle
    ///
    /// # Example
    ///
    /// ```ignore
    /// let transport = config.build(context)?;
    ///
    /// // Register validators with oracle
    /// transport.oracle.track(0, validators);
    ///
    /// // Pass channels to consumers
    /// engine.start(
    ///     transport.simplex.votes,
    ///     transport.simplex.certs,
    ///     transport.simplex.resolver,
    /// );
    /// ```
    pub fn build<E>(self, context: E) -> NetworkTransport<C::PublicKey, E>
    where
        E: Spawner + BufferPooler + Clock + CryptoRngCore + RNetwork + Resolver + Metrics,
    {
        self.build_with_quotas(
            context,
            default_consensus_quota(),
            default_block_quota(),
            default_sync_quota(),
            default_gossip_quota(),
        )
    }

    /// Build the network transport with a single uniform rate quota for all channels.
    ///
    /// Prefer [`build`](Self::build) which uses per-channel quotas calibrated
    /// to each channel's traffic pattern.
    pub fn build_with_quota<E>(self, context: E, quota: Quota) -> NetworkTransport<C::PublicKey, E>
    where
        E: Spawner + BufferPooler + Clock + CryptoRngCore + RNetwork + Resolver + Metrics,
    {
        self.build_with_quotas(context, quota, quota, quota, quota)
    }

    /// Build the network transport with per-channel rate quotas.
    ///
    /// # Parameters
    ///
    /// * `consensus_quota` - Quota for votes and certs channels.
    /// * `block_quota` - Quota for block and backfill channels.
    /// * `sync_quota` - Quota for the resolver channel.
    /// * `gossip_quota` - Quota for the transaction gossip channel.
    pub fn build_with_quotas<E>(
        self,
        context: E,
        consensus_quota: Quota,
        block_quota: Quota,
        sync_quota: Quota,
        gossip_quota: Quota,
    ) -> NetworkTransport<C::PublicKey, E>
    where
        E: Spawner + BufferPooler + Clock + CryptoRngCore + RNetwork + Resolver + Metrics,
    {
        let consensus_backlog = self.consensus_backlog;
        let block_backlog = self.block_backlog;
        let resolver_backlog = self.resolver_backlog;
        let gossip_backlog = self.gossip_backlog;

        // Create network and oracle
        let (mut network, oracle) = discovery::Network::new(context.child("network"), self.inner);

        // Register simplex channels (consensus: high frequency, small messages)
        let votes = network.register(CHANNEL_VOTES, consensus_quota, consensus_backlog);
        let certs = network.register(CHANNEL_CERTS, consensus_quota, consensus_backlog);
        let resolver = network.register(CHANNEL_RESOLVER, sync_quota, resolver_backlog);

        // Register marshal channels (blocks: large messages, backfill: burst-heavy)
        let blocks = network.register(CHANNEL_BLOCKS, block_quota, block_backlog);
        let backfill = network.register(CHANNEL_BACKFILL, block_quota, resolver_backlog);

        // Register transaction gossip channel (most restrictive: untrusted content)
        let tx_gossip_channel = network.register(CHANNEL_TX_GOSSIP, gossip_quota, gossip_backlog);

        // Start the network
        let handle = network.start();

        tracing::info!("network transport started with 6 channels (per-channel rate limits)");

        NetworkTransport {
            oracle,
            handle,
            simplex: SimplexChannels { votes, certs, resolver },
            marshal: MarshalChannels { blocks, backfill },
            tx_gossip: TxGossipChannel { channel: tx_gossip_channel },
        }
    }
}
