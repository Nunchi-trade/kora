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

/// Default rate quota for channels (1000 messages per second).
const fn default_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(1000).expect("1000 is non-zero"))
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
    /// transport.oracle.track(0, validators).await;
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
        self.build_with_quota(context, default_quota())
    }

    /// Build the network transport with a custom rate quota.
    ///
    /// Same as [`build`](Self::build) but allows specifying a custom
    /// rate limit for all channels.
    pub fn build_with_quota<E>(self, context: E, quota: Quota) -> NetworkTransport<C::PublicKey, E>
    where
        E: Spawner + BufferPooler + Clock + CryptoRngCore + RNetwork + Resolver + Metrics,
    {
        let consensus_backlog = self.consensus_backlog;
        let block_backlog = self.block_backlog;
        let resolver_backlog = self.resolver_backlog;
        let gossip_backlog = self.gossip_backlog;

        // Create network and oracle
        let (mut network, oracle) =
            discovery::Network::new(context.with_label("network"), self.inner);

        // Register simplex channels (consensus: high frequency, small messages)
        let votes = network.register(CHANNEL_VOTES, quota, consensus_backlog);
        let certs = network.register(CHANNEL_CERTS, quota, consensus_backlog);
        let resolver = network.register(CHANNEL_RESOLVER, quota, resolver_backlog);

        // Register marshal channels (blocks: large messages, backfill: burst-heavy)
        let blocks = network.register(CHANNEL_BLOCKS, quota, block_backlog);
        let backfill = network.register(CHANNEL_BACKFILL, quota, resolver_backlog);

        // Register transaction gossip channel
        let tx_gossip_channel = network.register(CHANNEL_TX_GOSSIP, quota, gossip_backlog);

        // Start the network
        let handle = network.start();

        tracing::info!("network transport started with 6 channels");

        NetworkTransport {
            oracle,
            handle,
            simplex: SimplexChannels { votes, certs, resolver },
            marshal: MarshalChannels { blocks, backfill },
            tx_gossip: TxGossipChannel { channel: tx_gossip_channel },
        }
    }
}
