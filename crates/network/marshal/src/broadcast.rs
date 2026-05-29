//! Contains the [`BroadcastInitializer`] which initializes the buffered broadcast engine.

use std::num::NonZeroUsize;

use commonware_broadcast::buffered::{Config, Engine, Mailbox};
use commonware_codec::Codec;
use commonware_cryptography::{Digestible, PublicKey};
use commonware_p2p::Provider;
use commonware_runtime::{BufferPooler, Clock, Metrics, Spawner};
use commonware_utils::NZUsize;

/// Initializes the buffered broadcast engine with sensible defaults.
#[derive(Debug, Clone, Copy)]
pub struct BroadcastInitializer;

impl BroadcastInitializer {
    /// The default mailbox size.
    pub const DEFAULT_MAILBOX_SIZE: NonZeroUsize = NZUsize!(1024);

    /// The default deque size for message buffering.
    pub const DEFAULT_DEQUE_SIZE: usize = 256;

    /// Whether messages are sent with priority by default.
    pub const DEFAULT_PRIORITY: bool = true;
}

impl BroadcastInitializer {
    /// Initializes the buffered broadcast engine.
    ///
    /// Returns the engine and a mailbox for sending messages.
    pub fn init<E, P, M, D>(
        ctx: E,
        public_key: P,
        peer_provider: D,
        codec_config: M::Cfg,
    ) -> (Engine<E, P, M, D>, Mailbox<P, M>)
    where
        E: BufferPooler + Clock + Spawner + Metrics,
        P: PublicKey,
        M: Digestible + Codec,
        D: Provider<PublicKey = P>,
    {
        let config = Config {
            public_key,
            mailbox_size: Self::DEFAULT_MAILBOX_SIZE,
            deque_size: Self::DEFAULT_DEQUE_SIZE,
            priority: Self::DEFAULT_PRIORITY,
            codec_config,
            peer_provider,
        };
        Engine::new(ctx, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        assert_eq!(BroadcastInitializer::DEFAULT_MAILBOX_SIZE.get(), 1024);
        assert_eq!(BroadcastInitializer::DEFAULT_DEQUE_SIZE, 256);
        assert!(BroadcastInitializer::DEFAULT_PRIORITY);
    }
}
