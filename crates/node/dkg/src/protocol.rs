//! Interactive DKG protocol implementation using commonware's Joint-Feldman DKG.
//!
//! This module implements the full interactive DKG protocol where each participant
//! acts as both a dealer (generating shares for others) and a player (receiving shares).

use std::collections::{BTreeMap, HashSet};

use commonware_codec::{Read as _, ReadExt, Write};
use commonware_cryptography::{
    Hasher as _, Sha256,
    bls12381::{
        dkg::feldman_desmedt::{
            Dealer, DealerLog, DealerPrivMsg, DealerPubMsg, Info, Logs, Player, PlayerAck,
            SignedDealerLog, observe,
        },
        primitives::{sharing::Mode, variant::MinSig},
    },
    ed25519,
};
use commonware_parallel::Sequential;
use commonware_utils::{Faults, N3f1, TryCollect, ordered::Set};
use tracing::{debug, info, warn};

/// Session metadata for a DKG ceremony, providing anti-replay protection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CeremonySession {
    /// Unique identifier for this ceremony instance (random bytes generated at start).
    pub ceremony_id: [u8; 32],
    /// Chain ID from configuration, binding the ceremony to a specific network.
    pub chain_id: u64,
    /// DKG round number (0 for initial DKG).
    pub round: u32,
}

impl CeremonySession {
    /// Generate a new ceremony session with a deterministic ceremony_id.
    ///
    /// The ceremony_id is derived from chain_id + sorted participant keys + timestamp,
    /// ensuring all participants can independently compute the same ID.
    pub fn new(chain_id: u64, participants: &[ed25519::PublicKey], timestamp_nanos: u64) -> Self {
        let mut hasher = Sha256::default();
        hasher.update(b"kora-dkg-ceremony-v1");
        hasher.update(&chain_id.to_le_bytes());
        hasher.update(&(participants.len() as u64).to_le_bytes());

        let mut sorted_participants = participants.to_vec();
        sorted_participants.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        for pk in &sorted_participants {
            hasher.update(pk.as_ref());
        }

        hasher.update(&timestamp_nanos.to_le_bytes());
        let digest = hasher.finalize();
        let mut ceremony_id = [0u8; 32];
        ceremony_id.copy_from_slice(digest.as_ref());

        Self { ceremony_id, chain_id, round: 0 }
    }

    /// Serialize the session to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(44);
        buf.extend_from_slice(&self.ceremony_id);
        buf.extend_from_slice(&self.chain_id.to_le_bytes());
        buf.extend_from_slice(&self.round.to_le_bytes());
        buf
    }

    fn info_namespace(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + 44);
        buf.extend_from_slice(b"kora-dkg-session");
        buf.extend_from_slice(&self.to_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, commonware_codec::Error> {
        if bytes.len() < 44 {
            return Err(commonware_codec::Error::EndOfBuffer);
        }
        let mut ceremony_id = [0u8; 32];
        ceremony_id.copy_from_slice(&bytes[0..32]);
        let chain_id = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let round = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
        Ok(Self { ceremony_id, chain_id, round })
    }
}

use crate::{DkgConfig, DkgError, DkgOutput, DkgPhase, PersistedDkgState};

/// Maximum accepted DKG protocol payload size.
pub(crate) const MAX_DKG_MESSAGE_BYTES: usize = 256 * 1024;

const WIRE_V2_MARKER: u8 = 0xFF;
const WIRE_V2_VERSION: u8 = 2;
const SESSION_ID_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtocolFrameHeader {
    session_id: Option<[u8; 32]>,
    tag: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SeenMessageKey {
    DealerPublic(ed25519::PublicKey),
    DealerPrivate(ed25519::PublicKey),
    PlayerAck(ed25519::PublicKey),
    DealerLog(ed25519::PublicKey),
    RequestLogs(ed25519::PublicKey),
    AllLogs(ed25519::PublicKey),
    Ready(ed25519::PublicKey),
}

/// Inner message types for the DKG protocol (without session binding).
#[derive(Debug, Clone)]
pub enum ProtocolMessageKind {
    /// Public commitment from a dealer to all players.
    DealerPublic {
        /// The dealer's public key.
        dealer: ed25519::PublicKey,
        /// The public commitment message.
        msg: DealerPubMsg<MinSig>,
    },
    /// Private share from a dealer to a specific player.
    DealerPrivate {
        /// The dealer's public key.
        dealer: ed25519::PublicKey,
        /// The private share message.
        msg: DealerPrivMsg,
    },
    /// Acknowledgement from a player to a dealer.
    PlayerAck {
        /// The player's public key.
        player: ed25519::PublicKey,
        /// The dealer's public key.
        dealer: ed25519::PublicKey,
        /// The acknowledgement.
        ack: PlayerAck<ed25519::PublicKey>,
    },
    /// Signed dealer log for finalization.
    DealerLog {
        /// The signed dealer log.
        log: SignedDealerLog<MinSig, ed25519::PrivateKey>,
    },
    /// Request for all dealer logs (sent by non-leaders to leader).
    RequestLogs,
    /// All collected dealer logs (sent by leader to all).
    AllLogs {
        /// The collected dealer logs.
        logs: Vec<(ed25519::PublicKey, SignedDealerLog<MinSig, ed25519::PrivateKey>)>,
    },
    /// Ready signal indicating a node has sent all acks and is waiting to finalize.
    Ready {
        /// The player's public key.
        player: ed25519::PublicKey,
    },
}

/// Message envelope that wraps protocol messages with session binding.
///
/// All messages must include a session_id for anti-replay protection.
#[derive(Debug, Clone)]
pub struct ProtocolMessage {
    /// Session ID binding this message to a specific ceremony.
    /// `None` indicates a legacy message without session binding (rejected on receipt).
    pub session_id: Option<[u8; 32]>,
    /// The actual protocol message content.
    pub kind: ProtocolMessageKind,
}

impl ProtocolMessage {
    /// Create a new message with session binding.
    pub const fn new(session_id: [u8; 32], kind: ProtocolMessageKind) -> Self {
        Self { session_id: Some(session_id), kind }
    }

    /// Serialize the message to bytes.
    ///
    /// Format v2 (with session): `[0xFF][version=2]`[session_id: 32 bytes][inner message]
    /// Format v1 (legacy): `[tag < 0xFF]`[inner message data]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        if let Some(session_id) = &self.session_id {
            buf.push(WIRE_V2_MARKER);
            buf.push(WIRE_V2_VERSION);
            buf.extend_from_slice(session_id);
        }

        match &self.kind {
            ProtocolMessageKind::DealerPublic { dealer, msg } => {
                buf.push(0u8);
                dealer.write(&mut buf);
                msg.write(&mut buf);
            }
            ProtocolMessageKind::DealerPrivate { dealer, msg } => {
                buf.push(1u8);
                dealer.write(&mut buf);
                msg.write(&mut buf);
            }
            ProtocolMessageKind::PlayerAck { player, dealer, ack } => {
                buf.push(2u8);
                player.write(&mut buf);
                dealer.write(&mut buf);
                ack.write(&mut buf);
            }
            ProtocolMessageKind::DealerLog { log } => {
                buf.push(3u8);
                log.write(&mut buf);
            }
            ProtocolMessageKind::RequestLogs => {
                buf.push(4u8);
            }
            ProtocolMessageKind::AllLogs { logs } => {
                buf.push(5u8);
                (logs.len() as u32).write(&mut buf);
                for (pk, log) in logs {
                    pk.write(&mut buf);
                    log.write(&mut buf);
                }
            }
            ProtocolMessageKind::Ready { player } => {
                buf.push(6u8);
                player.write(&mut buf);
            }
        }
        buf
    }

    fn frame_header(bytes: &[u8]) -> Result<ProtocolFrameHeader, commonware_codec::Error> {
        if bytes.len() > MAX_DKG_MESSAGE_BYTES {
            return Err(commonware_codec::Error::InvalidLength(bytes.len()));
        }

        let Some((&first_byte, rest)) = bytes.split_first() else {
            return Err(commonware_codec::Error::EndOfBuffer);
        };

        if first_byte != WIRE_V2_MARKER {
            return Ok(ProtocolFrameHeader { session_id: None, tag: first_byte });
        }

        if rest.len() < 1 + SESSION_ID_LEN + 1 {
            return Err(commonware_codec::Error::EndOfBuffer);
        }

        let version = rest[0];
        if version != WIRE_V2_VERSION {
            return Err(commonware_codec::Error::InvalidEnum(version));
        }

        let mut session_id = [0u8; 32];
        session_id.copy_from_slice(&rest[1..1 + SESSION_ID_LEN]);
        let tag = rest[1 + SESSION_ID_LEN];

        Ok(ProtocolFrameHeader { session_id: Some(session_id), tag })
    }

    /// Deserialize from bytes.
    ///
    /// Supports both v2 (session-bound) and v1 (legacy) message formats.
    pub fn from_bytes(bytes: &[u8], max_degree: u32) -> Result<Self, commonware_codec::Error> {
        let max_log_entries = max_entries_for_degree(max_degree);
        Self::from_bytes_with_limits(bytes, max_degree, max_log_entries)
    }

    fn from_bytes_with_limits(
        bytes: &[u8],
        max_degree: u32,
        max_log_entries: usize,
    ) -> Result<Self, commonware_codec::Error> {
        Self::frame_header(bytes)?;
        let mut reader = bytes;

        let first_byte = u8::read(&mut reader)?;

        let (session_id, tag) = if first_byte == WIRE_V2_MARKER {
            let version = u8::read(&mut reader)?;
            if version != WIRE_V2_VERSION {
                return Err(commonware_codec::Error::InvalidEnum(version));
            }
            let mut session_id = [0u8; 32];
            if reader.len() < SESSION_ID_LEN {
                return Err(commonware_codec::Error::EndOfBuffer);
            }
            session_id.copy_from_slice(&reader[..SESSION_ID_LEN]);
            reader = &reader[SESSION_ID_LEN..];
            let tag = u8::read(&mut reader)?;
            (Some(session_id), tag)
        } else {
            (None, first_byte)
        };

        let max_degree_nz = core::num::NonZeroU32::new(max_degree)
            .ok_or(commonware_codec::Error::InvalidLength(0))?;

        let kind = match tag {
            0 => {
                let dealer = ed25519::PublicKey::read(&mut reader)?;
                let msg = DealerPubMsg::<MinSig>::read_cfg(&mut reader, &max_degree_nz)?;
                ProtocolMessageKind::DealerPublic { dealer, msg }
            }
            1 => {
                let dealer = ed25519::PublicKey::read(&mut reader)?;
                let msg = DealerPrivMsg::read(&mut reader)?;
                ProtocolMessageKind::DealerPrivate { dealer, msg }
            }
            2 => {
                let player = ed25519::PublicKey::read(&mut reader)?;
                let dealer = ed25519::PublicKey::read(&mut reader)?;
                let ack = PlayerAck::<ed25519::PublicKey>::read(&mut reader)?;
                ProtocolMessageKind::PlayerAck { player, dealer, ack }
            }
            3 => {
                let log = SignedDealerLog::<MinSig, ed25519::PrivateKey>::read_cfg(
                    &mut reader,
                    &max_degree_nz,
                )?;
                ProtocolMessageKind::DealerLog { log }
            }
            4 => ProtocolMessageKind::RequestLogs,
            5 => {
                let count = u32::read(&mut reader)? as usize;
                if count > max_log_entries {
                    return Err(commonware_codec::Error::InvalidLength(count));
                }
                let mut logs = Vec::with_capacity(count);
                for _ in 0..count {
                    let pk = ed25519::PublicKey::read(&mut reader)?;
                    let log = SignedDealerLog::<MinSig, ed25519::PrivateKey>::read_cfg(
                        &mut reader,
                        &max_degree_nz,
                    )?;
                    logs.push((pk, log));
                }
                ProtocolMessageKind::AllLogs { logs }
            }
            6 => {
                let player = ed25519::PublicKey::read(&mut reader)?;
                ProtocolMessageKind::Ready { player }
            }
            _ => return Err(commonware_codec::Error::InvalidEnum(tag)),
        };

        if !reader.is_empty() {
            return Err(commonware_codec::Error::ExtraData(reader.len()));
        }

        Ok(Self { session_id, kind })
    }
}

const fn max_entries_for_degree(max_degree: u32) -> usize {
    let threshold = max_degree as usize;
    threshold.saturating_add(threshold.saturating_sub(1) / 2)
}

fn dealer_log_matches_public_message(
    dealer_log: &DealerLog<MinSig, ed25519::PublicKey>,
    pub_msg: &DealerPubMsg<MinSig>,
) -> bool {
    let mut log_bytes = Vec::new();
    dealer_log.write(&mut log_bytes);

    let mut pub_msg_bytes = Vec::new();
    pub_msg.write(&mut pub_msg_bytes);

    log_bytes.starts_with(&pub_msg_bytes)
}

/// State of a participant in the DKG protocol.
pub struct DkgParticipant {
    // Note: Manual Debug impl below due to complex inner types.
    config: DkgConfig,
    info: Info<MinSig, ed25519::PublicKey>,
    player: Option<Player<MinSig, ed25519::PrivateKey>>,
    dealer: Option<Dealer<MinSig, ed25519::PrivateKey>>,

    /// Session metadata for this ceremony (anti-replay protection).
    session: CeremonySession,
    /// Set of semantic message keys we've already accepted.
    seen_messages: HashSet<SeenMessageKey>,

    /// Messages to send (accumulated during protocol execution).
    outgoing: Vec<(Option<ed25519::PublicKey>, ProtocolMessage)>,

    /// Received dealer public messages.
    dealer_pub_msgs: BTreeMap<ed25519::PublicKey, DealerPubMsg<MinSig>>,
    /// Received dealer private messages.
    dealer_priv_msgs: BTreeMap<ed25519::PublicKey, DealerPrivMsg>,

    /// Signed dealer logs we've collected.
    dealer_logs: BTreeMap<ed25519::PublicKey, DealerLog<MinSig, ed25519::PublicKey>>,
    /// Signed logs (for sending to leader).
    signed_logs: BTreeMap<ed25519::PublicKey, SignedDealerLog<MinSig, ed25519::PrivateKey>>,

    /// Our own signed log.
    our_signed_log: Option<SignedDealerLog<MinSig, ed25519::PrivateKey>>,

    /// Whether we've finalized.
    finalized: bool,

    /// Count of acks we've sent to dealers.
    acks_sent: HashSet<ed25519::PublicKey>,

    /// Players who have signaled they are ready to finalize (received all dealer messages).
    ready_players: HashSet<ed25519::PublicKey>,

    /// Whether we have broadcast our ready signal.
    sent_ready: bool,

    /// Current phase of the DKG protocol.
    current_phase: DkgPhase,

    /// Timestamp used for this ceremony session.
    timestamp_nanos: u64,
}

impl std::fmt::Debug for DkgParticipant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DkgParticipant")
            .field("validator_index", &self.config.validator_index)
            .field("phase", &self.current_phase)
            .field("finalized", &self.finalized)
            .field("dealer_logs_count", &self.dealer_logs.len())
            .finish_non_exhaustive()
    }
}

impl DkgParticipant {
    /// Create a new DKG participant.
    ///
    /// The `timestamp_nanos` is used along with chain_id and participants to generate
    /// a deterministic ceremony_id. All participants must use the same timestamp
    /// (typically coordinated via the leader or a shared clock).
    pub fn new(config: DkgConfig, timestamp_nanos: u64) -> Result<Self, DkgError> {
        let participants_set: Set<ed25519::PublicKey> = config
            .participants
            .iter()
            .cloned()
            .try_collect()
            .map_err(|_| DkgError::CeremonyFailed("duplicate participants".into()))?;

        let session = CeremonySession::new(config.chain_id, &config.participants, timestamp_nanos);
        let info_namespace = session.info_namespace();

        // Create round info - all participants are both dealers and players
        let info = Info::<MinSig, ed25519::PublicKey>::new::<N3f1>(
            &info_namespace,
            u64::from(session.round),
            None, // no previous output
            Mode::default(),
            participants_set.clone(), // dealers
            participants_set,         // players
        )
        .map_err(|e| DkgError::Crypto(format!("Failed to create DKG info: {:?}", e)))?;

        // Create our player instance
        let player =
            Player::<MinSig, ed25519::PrivateKey>::new(info.clone(), config.identity_key.clone())
                .map_err(|e| DkgError::Crypto(format!("Failed to create player: {:?}", e)))?;

        info!(
            ceremony_id = hex::encode(session.ceremony_id),
            chain_id = session.chain_id,
            round = session.round,
            "Created DKG ceremony session"
        );

        Ok(Self {
            config,
            info,
            player: Some(player),
            dealer: None,
            session,
            seen_messages: HashSet::new(),
            outgoing: Vec::new(),
            dealer_pub_msgs: BTreeMap::new(),
            dealer_priv_msgs: BTreeMap::new(),
            dealer_logs: BTreeMap::new(),
            signed_logs: BTreeMap::new(),
            our_signed_log: None,
            finalized: false,
            acks_sent: HashSet::new(),
            ready_players: HashSet::new(),
            sent_ready: false,
            current_phase: DkgPhase::AwaitingStart,
            timestamp_nanos,
        })
    }

    /// Get the ceremony session.
    pub const fn session(&self) -> &CeremonySession {
        &self.session
    }

    /// Get the ceremony ID for message creation.
    pub const fn ceremony_id(&self) -> [u8; 32] {
        self.session.ceremony_id
    }

    fn logs_for_verification(&self) -> Logs<MinSig, ed25519::PublicKey, N3f1> {
        let mut logs = Logs::new(self.info.clone());
        for (dealer, log) in &self.dealer_logs {
            logs.record(dealer.clone(), log.clone());
        }
        logs
    }

    /// Start the dealer phase - generate and return messages to send.
    pub fn start_dealer(&mut self) -> Result<(), DkgError> {
        let mut rng = rand::rngs::OsRng;

        let (dealer, pub_msg, priv_msgs) = Dealer::<MinSig, ed25519::PrivateKey>::start::<N3f1>(
            &mut rng,
            self.info.clone(),
            self.config.identity_key.clone(),
            None, // no previous share for initial DKG
        )
        .map_err(|e| DkgError::Crypto(format!("Failed to start dealer: {:?}", e)))?;

        let my_pk = self.config.my_public_key();
        let ceremony_id = self.ceremony_id();

        info!(
            validator_index = self.config.validator_index,
            "Generated dealer messages for {} players",
            priv_msgs.len()
        );

        // Queue public message for broadcast
        self.outgoing.push((
            None, // broadcast
            ProtocolMessage::new(
                ceremony_id,
                ProtocolMessageKind::DealerPublic { dealer: my_pk.clone(), msg: pub_msg.clone() },
            ),
        ));

        // Queue private messages for each player, storing our own
        for (player_pk, priv_msg) in priv_msgs {
            if player_pk == my_pk {
                // Store our own private message so we can process ourselves as a dealer
                self.dealer_priv_msgs.insert(my_pk.clone(), priv_msg);
            } else {
                self.outgoing.push((
                    Some(player_pk.clone()),
                    ProtocolMessage::new(
                        ceremony_id,
                        ProtocolMessageKind::DealerPrivate { dealer: my_pk.clone(), msg: priv_msg },
                    ),
                ));
            }
        }

        // Store our own public message so we can process it
        self.dealer_pub_msgs.insert(my_pk.clone(), pub_msg);
        self.dealer = Some(dealer);

        // Process our own dealer messages immediately to generate self-ack
        self.try_process_dealer_messages(&my_pk)?;

        Ok(())
    }

    /// Process an incoming message from raw bytes.
    ///
    /// This method handles session verification and message deduplication before
    /// processing the actual message content.
    pub fn handle_message_bytes(
        &mut self,
        from: &ed25519::PublicKey,
        bytes: &[u8],
    ) -> Result<(), DkgError> {
        // Validate sender is a known participant before inserting into
        // seen_messages to prevent non-participants from exhausting memory.
        if !self.is_participant(from) {
            warn!(?from, "Rejecting message from non-participant (pre-dedup)");
            return Err(DkgError::UnknownSender { sender: format!("{:?}", from) });
        }

        let header = ProtocolMessage::frame_header(bytes)
            .map_err(|e| DkgError::InvalidMessage(format!("Invalid frame: {:?}", e)))?;

        match &header.session_id {
            Some(session_id) => {
                if *session_id != self.session.ceremony_id {
                    warn!(
                        ?from,
                        expected = hex::encode(self.session.ceremony_id),
                        received = hex::encode(session_id),
                        "Rejecting message with mismatched session ID"
                    );
                    return Err(DkgError::SessionMismatch {
                        expected: hex::encode(self.session.ceremony_id),
                        received: hex::encode(session_id),
                    });
                }
            }
            None => {
                warn!(?from, "Rejecting message without session ID");
                return Err(DkgError::SessionMismatch {
                    expected: hex::encode(self.session.ceremony_id),
                    received: String::from("<none>"),
                });
            }
        }

        self.validate_message_tag_for_phase(header.tag)?;

        let msg =
            ProtocolMessage::from_bytes_with_limits(bytes, self.config.n() as u32, self.config.n())
                .map_err(|e| DkgError::InvalidMessage(format!("Failed to decode: {:?}", e)))?;

        let message_key = self.message_key(from, &msg)?;
        if self.seen_messages.contains(&message_key) {
            debug!(?from, ?message_key, "Rejecting duplicate message");
            return Ok(());
        }

        if self.seen_messages.len() >= self.max_seen_messages() {
            warn!("seen_messages capacity reached, rejecting message");
            return Err(DkgError::InvalidMessage("deduplication set full".into()));
        }

        self.handle_message(from, msg)?;
        self.seen_messages.insert(message_key);
        Ok(())
    }

    const fn max_seen_messages(&self) -> usize {
        self.config.n().saturating_mul(6).saturating_add(8)
    }

    fn validate_message_tag_for_phase(&self, tag: u8) -> Result<(), DkgError> {
        let allowed = match tag {
            // Dealer public/private messages and acks may be in flight while a
            // participant is starting or collecting phase-2 messages.
            0..=2 => matches!(
                self.current_phase,
                DkgPhase::AwaitingStart | DkgPhase::DealerStarted | DkgPhase::CollectingMessages
            ),
            // Dealer logs and log sync messages belong to phase 4.
            3..=5 => {
                matches!(self.current_phase, DkgPhase::DealerFinalized | DkgPhase::CollectingLogs)
            }
            // Ready can arrive while local phase-2 completion is racing.
            6 => matches!(
                self.current_phase,
                DkgPhase::AwaitingStart | DkgPhase::DealerStarted | DkgPhase::CollectingMessages
            ),
            _ => false,
        };

        if allowed {
            Ok(())
        } else {
            Err(DkgError::InvalidMessage(format!(
                "message tag {} not valid in phase {}",
                tag, self.current_phase
            )))
        }
    }

    fn message_key(
        &self,
        from: &ed25519::PublicKey,
        msg: &ProtocolMessage,
    ) -> Result<SeenMessageKey, DkgError> {
        match &msg.kind {
            ProtocolMessageKind::DealerPublic { dealer, .. } => {
                if from != dealer {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", dealer),
                        actual: format!("{:?}", from),
                    });
                }
                Ok(SeenMessageKey::DealerPublic(dealer.clone()))
            }
            ProtocolMessageKind::DealerPrivate { dealer, .. } => {
                if from != dealer {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", dealer),
                        actual: format!("{:?}", from),
                    });
                }
                Ok(SeenMessageKey::DealerPrivate(dealer.clone()))
            }
            ProtocolMessageKind::PlayerAck { player, dealer, .. } => {
                if from != player {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", player),
                        actual: format!("{:?}", from),
                    });
                }
                if dealer != &self.config.my_public_key() {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", self.config.my_public_key()),
                        actual: format!("{:?}", dealer),
                    });
                }
                Ok(SeenMessageKey::PlayerAck(player.clone()))
            }
            ProtocolMessageKind::DealerLog { .. } => Ok(SeenMessageKey::DealerLog(from.clone())),
            ProtocolMessageKind::RequestLogs => Ok(SeenMessageKey::RequestLogs(from.clone())),
            ProtocolMessageKind::AllLogs { logs } => {
                if from != self.leader() {
                    return Err(DkgError::UnauthorizedSender);
                }
                if logs.len() > self.config.n() {
                    return Err(DkgError::InvalidMessage("too many dealer logs".into()));
                }
                Ok(SeenMessageKey::AllLogs(from.clone()))
            }
            ProtocolMessageKind::Ready { player } => {
                if from != player {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", player),
                        actual: format!("{:?}", from),
                    });
                }
                Ok(SeenMessageKey::Ready(player.clone()))
            }
        }
    }

    /// Process an incoming message (after session/dedup validation).
    pub fn handle_message(
        &mut self,
        from: &ed25519::PublicKey,
        msg: ProtocolMessage,
    ) -> Result<(), DkgError> {
        let max_entries = self.config.n();

        if !self.is_participant(from) {
            warn!(?from, "Received message from unknown sender");
            return Err(DkgError::UnknownSender { sender: format!("{:?}", from) });
        }

        match msg.kind {
            ProtocolMessageKind::DealerPublic { dealer, msg } => {
                if from != &dealer {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", dealer),
                        actual: format!("{:?}", from),
                    });
                }
                if self.dealer_pub_msgs.len() >= max_entries {
                    return Err(DkgError::TooManyDealers {
                        count: self.dealer_pub_msgs.len() + 1,
                        max: max_entries,
                    });
                }
                if self.dealer_pub_msgs.contains_key(&dealer) {
                    return Err(DkgError::DuplicateDealer { dealer: format!("{:?}", dealer) });
                }
                debug!(?dealer, "Received dealer public message");
                self.dealer_pub_msgs.insert(dealer.clone(), msg);
                self.try_process_dealer_messages(&dealer)?;
            }
            ProtocolMessageKind::DealerPrivate { dealer, msg } => {
                if from != &dealer {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", dealer),
                        actual: format!("{:?}", from),
                    });
                }
                if self.dealer_priv_msgs.len() >= max_entries {
                    return Err(DkgError::TooManyDealers {
                        count: self.dealer_priv_msgs.len() + 1,
                        max: max_entries,
                    });
                }
                if self.dealer_priv_msgs.contains_key(&dealer) {
                    return Err(DkgError::DuplicateDealer { dealer: format!("{:?}", dealer) });
                }
                debug!(?dealer, "Received dealer private message");
                self.dealer_priv_msgs.insert(dealer.clone(), msg);
                self.try_process_dealer_messages(&dealer)?;
            }
            ProtocolMessageKind::PlayerAck { player, dealer, ack } => {
                if from != &player {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", player),
                        actual: format!("{:?}", from),
                    });
                }
                if dealer != self.config.my_public_key() {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", self.config.my_public_key()),
                        actual: format!("{:?}", dealer),
                    });
                }
                if let Some(ref mut our_dealer) = self.dealer
                    && dealer == self.config.my_public_key()
                {
                    debug!(?player, "Received player ack");
                    if let Err(e) = our_dealer.receive_player_ack(player, ack) {
                        warn!(?e, "Failed to process player ack");
                    }
                }
            }
            ProtocolMessageKind::DealerLog { log } => {
                let log_clone = log.clone();
                if let Some((dealer_pk, dealer_log)) = log.check(&self.info) {
                    if from != &dealer_pk {
                        return Err(DkgError::SenderMismatch {
                            expected: format!("{:?}", dealer_pk),
                            actual: format!("{:?}", from),
                        });
                    }
                    if !self.is_participant(&dealer_pk) {
                        return Err(DkgError::UnknownSender { sender: format!("{:?}", dealer_pk) });
                    }
                    self.validate_dealer_log_for_current_messages(&dealer_pk, &dealer_log)?;
                    if self.dealer_logs.len() >= max_entries {
                        return Err(DkgError::TooManyDealers {
                            count: self.dealer_logs.len() + 1,
                            max: max_entries,
                        });
                    }
                    if self.dealer_logs.contains_key(&dealer_pk) {
                        return Err(DkgError::DuplicateDealer {
                            dealer: format!("{:?}", dealer_pk),
                        });
                    }
                    debug!(?dealer_pk, "Received valid dealer log");
                    self.dealer_logs.insert(dealer_pk.clone(), dealer_log);
                    self.signed_logs.insert(dealer_pk, log_clone);
                } else {
                    return Err(DkgError::InvalidDealerLog { dealer: format!("{:?}", from) });
                }
            }
            ProtocolMessageKind::RequestLogs => {
                let logs: Vec<_> =
                    self.signed_logs.iter().map(|(pk, log)| (pk.clone(), log.clone())).collect();
                self.outgoing.push((
                    Some(from.clone()),
                    ProtocolMessage::new(
                        self.session.ceremony_id,
                        ProtocolMessageKind::AllLogs { logs },
                    ),
                ));
            }
            ProtocolMessageKind::AllLogs { logs } => {
                if from != self.leader() {
                    return Err(DkgError::UnauthorizedSender);
                }
                info!(count = logs.len(), "Received all dealer logs from leader");
                let mut accepted_logs = 0usize;
                for (_pk, log) in logs {
                    let log_clone = log.clone();
                    if let Some((dealer_pk, dealer_log)) = log.check(&self.info) {
                        if !self.is_participant(&dealer_pk) {
                            continue;
                        }
                        if self
                            .validate_dealer_log_for_current_messages(&dealer_pk, &dealer_log)
                            .is_err()
                        {
                            continue;
                        }
                        if self.dealer_logs.len() >= max_entries {
                            break;
                        }
                        if self.dealer_logs.contains_key(&dealer_pk) {
                            continue;
                        }
                        self.dealer_logs.insert(dealer_pk.clone(), dealer_log);
                        self.signed_logs.insert(dealer_pk, log_clone);
                        accepted_logs += 1;
                    }
                }
                if accepted_logs == 0 {
                    return Err(DkgError::InvalidMessage("all logs contained no new logs".into()));
                }
            }
            ProtocolMessageKind::Ready { player } => {
                if from != &player {
                    return Err(DkgError::SenderMismatch {
                        expected: format!("{:?}", player),
                        actual: format!("{:?}", from),
                    });
                }
                debug!(?player, "Received ready signal");
                self.ready_players.insert(player);
            }
        }
        Ok(())
    }

    fn validate_dealer_log_for_current_messages(
        &self,
        dealer: &ed25519::PublicKey,
        dealer_log: &DealerLog<MinSig, ed25519::PublicKey>,
    ) -> Result<(), DkgError> {
        let has_current_transcript = !self.dealer_pub_msgs.is_empty() || !self.acks_sent.is_empty();
        if !has_current_transcript {
            // Phase-4 restore persists signed logs but not the phase-2 transcript.
            return Ok(());
        }

        if !self.acks_sent.contains(dealer) {
            return Err(DkgError::InvalidDealerLog { dealer: format!("{:?}", dealer) });
        }

        let Some(pub_msg) = self.dealer_pub_msgs.get(dealer) else {
            return Err(DkgError::InvalidDealerLog { dealer: format!("{:?}", dealer) });
        };

        if !dealer_log_matches_public_message(dealer_log, pub_msg) {
            return Err(DkgError::InvalidDealerLog { dealer: format!("{:?}", dealer) });
        }

        Ok(())
    }

    /// Try to process dealer messages if we have both pub and priv.
    fn try_process_dealer_messages(&mut self, dealer: &ed25519::PublicKey) -> Result<(), DkgError> {
        let pub_msg = match self.dealer_pub_msgs.get(dealer) {
            Some(m) => m.clone(),
            None => return Ok(()),
        };
        let priv_msg = match self.dealer_priv_msgs.get(dealer) {
            Some(m) => m.clone(),
            None => return Ok(()),
        };

        // Process the dealer message and potentially generate an ack
        if let Some(ref mut player) = self.player {
            if let Some(ack) = player.dealer_message::<N3f1>(dealer.clone(), pub_msg, priv_msg) {
                debug!(?dealer, "Sending ack to dealer");
                let ceremony_id = self.ceremony_id();
                self.outgoing.push((
                    Some(dealer.clone()),
                    ProtocolMessage::new(
                        ceremony_id,
                        ProtocolMessageKind::PlayerAck {
                            player: self.config.my_public_key(),
                            dealer: dealer.clone(),
                            ack,
                        },
                    ),
                ));
                self.acks_sent.insert(dealer.clone());
            } else {
                warn!(?dealer, "Failed to verify dealer message");
            }
        }

        Ok(())
    }

    /// Finalize our dealer and create signed log.
    pub fn finalize_dealer(&mut self) -> Result<(), DkgError> {
        if let Some(dealer) = self.dealer.take() {
            let signed_log = dealer.finalize::<N3f1>();
            let signed_log_clone = signed_log.clone();

            // Verify our own log
            if let Some((dealer_pk, dealer_log)) = signed_log.check(&self.info) {
                info!(?dealer_pk, "Created valid dealer log");
                self.dealer_logs.insert(dealer_pk.clone(), dealer_log);
                self.signed_logs.insert(dealer_pk, signed_log_clone.clone());
                self.our_signed_log = Some(signed_log_clone.clone());

                // Send to all participants (leader will collect)
                let ceremony_id = self.ceremony_id();
                self.outgoing.push((
                    None, // broadcast
                    ProtocolMessage::new(
                        ceremony_id,
                        ProtocolMessageKind::DealerLog { log: signed_log_clone },
                    ),
                ));
            } else {
                return Err(DkgError::CeremonyFailed("Our own dealer log is invalid".into()));
            }
        }
        Ok(())
    }

    /// Broadcast a ready signal indicating we've sent all our acks.
    ///
    /// This should be called after receiving all dealer messages and sending acks.
    pub fn broadcast_ready(&mut self) {
        if self.sent_ready {
            return;
        }
        let my_pk = self.config.my_public_key();
        let ceremony_id = self.ceremony_id();
        info!("Broadcasting ready signal");
        self.outgoing.push((
            None, // broadcast
            ProtocolMessage::new(ceremony_id, ProtocolMessageKind::Ready { player: my_pk.clone() }),
        ));
        self.ready_players.insert(my_pk);
        self.sent_ready = true;
    }

    /// Check if all participants have signaled ready.
    pub fn all_ready(&self) -> bool {
        self.ready_players.len() >= self.config.n()
    }

    /// Get the count of ready players.
    pub fn ready_count(&self) -> usize {
        self.ready_players.len()
    }

    /// Check if we have all dealer logs needed to finalize.
    pub fn can_finalize(&self) -> bool {
        self.dealer_logs.len() >= self.required_dealer_logs()
    }

    /// Finalize the DKG and produce output.
    pub fn finalize(&mut self) -> Result<DkgOutput, DkgError> {
        if self.finalized {
            return Err(DkgError::CeremonyFailed("Already finalized".into()));
        }

        if !self.can_finalize() {
            return Err(DkgError::CeremonyFailed(format!(
                "Not enough dealer logs: {} < {}",
                self.dealer_logs.len(),
                self.required_dealer_logs()
            )));
        }

        info!(
            logs = self.dealer_logs.len(),
            dealer_pub_msgs = self.dealer_pub_msgs.len(),
            dealer_priv_msgs = self.dealer_priv_msgs.len(),
            acks_sent = self.acks_sent.len(),
            "Finalizing DKG with collected logs"
        );

        // Debug: log which dealers we have logs for vs which we processed
        for dealer_pk in self.dealer_logs.keys() {
            let has_pub = self.dealer_pub_msgs.contains_key(dealer_pk);
            let has_priv = self.dealer_priv_msgs.contains_key(dealer_pk);
            let sent_ack = self.acks_sent.contains(dealer_pk);
            debug!(?dealer_pk, has_pub, has_priv, sent_ack, "Dealer log status");
        }

        let player = self
            .player
            .take()
            .ok_or_else(|| DkgError::CeremonyFailed("Player already consumed".into()))?;

        // Debug: Log dealer log keys vs our config participants
        let log_dealers: Vec<_> =
            self.dealer_logs.keys().map(|d| hex::encode(d.as_ref())).collect();
        let config_participants: Vec<_> =
            self.config.participants.iter().map(|p| hex::encode(p.as_ref())).collect();
        debug!(
            log_dealers = ?log_dealers,
            config_participants = ?config_participants,
            "Comparing dealer log keys to config participants"
        );

        let mut rng = rand::rngs::OsRng;

        // Debug: try to observe the logs first to understand what's failing
        match observe::<MinSig, ed25519::PublicKey, N3f1, ed25519::Batch>(
            &mut rng,
            self.logs_for_verification(),
            &Sequential,
        ) {
            Ok(observed) => {
                info!(
                    dealers = observed.dealers().len(),
                    players = observed.players().len(),
                    "observe() succeeded"
                );
            }
            Err(e) => {
                warn!(
                    ?e,
                    logs = self.dealer_logs.len(),
                    "observe() failed - dealer logs don't pass validation"
                );
            }
        }

        let logs = self.logs_for_verification();
        let (output, share) = player
            .finalize::<N3f1, ed25519::Batch>(&mut rng, logs, &Sequential)
            .map_err(|e| DkgError::Crypto(format!("Failed to finalize: {:?}", e)))?;

        self.finalized = true;

        // Serialize outputs
        let mut group_key_bytes = Vec::new();
        output.public().public().write(&mut group_key_bytes);

        let mut polynomial_bytes = Vec::new();
        output.public().write(&mut polynomial_bytes);

        let mut share_bytes = Vec::new();
        share.write(&mut share_bytes);

        let participant_keys: Vec<Vec<u8>> = self
            .config
            .participants
            .iter()
            .map(|pk| {
                let mut bytes = Vec::new();
                pk.write(&mut bytes);
                bytes
            })
            .collect();

        Ok(DkgOutput {
            group_public_key: group_key_bytes,
            public_polynomial: polynomial_bytes,
            threshold: self.config.t(),
            participants: self.config.n(),
            share_index: usize::from(share.index) as u32,
            share_secret: share_bytes,
            participant_keys,
        })
    }

    /// Take outgoing messages.
    pub fn take_outgoing(&mut self) -> Vec<(Option<ed25519::PublicKey>, ProtocolMessage)> {
        std::mem::take(&mut self.outgoing)
    }

    /// Get our signed dealer log (for sending to leader).
    pub const fn our_signed_log(&self) -> Option<&SignedDealerLog<MinSig, ed25519::PrivateKey>> {
        self.our_signed_log.as_ref()
    }

    /// Get the number of collected dealer logs.
    pub fn dealer_log_count(&self) -> usize {
        self.dealer_logs.len()
    }

    /// Get required quorum.
    pub fn required_quorum(&self) -> usize {
        N3f1::quorum(self.config.participants.len()) as usize
    }

    /// Get the number of dealer logs required for this initial DKG ceremony.
    pub const fn required_dealer_logs(&self) -> usize {
        self.config.n()
    }

    /// Check if a public key is a participant in this DKG.
    pub fn is_participant(&self, pk: &ed25519::PublicKey) -> bool {
        self.config.participants.contains(pk)
    }

    /// Get the leader for this ceremony.
    ///
    /// The leader is derived from the ceremony_id so that different ceremonies
    /// rotate the coordinator role across participants, avoiding a single point
    /// of failure at `participants[0]`.
    fn leader(&self) -> &ed25519::PublicKey {
        let leader_idx = self.leader_index();
        &self.config.participants[leader_idx]
    }

    /// Public accessor for the leader's public key (used by ceremony runner).
    pub fn leader_pk(&self) -> &ed25519::PublicKey {
        self.leader()
    }

    /// Compute the leader index from the ceremony_id.
    fn leader_index(&self) -> usize {
        // Use the first 8 bytes of ceremony_id as a u64 seed for rotation.
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.session.ceremony_id[..8]);
        let seed = u64::from_le_bytes(bytes);
        (seed as usize) % self.config.participants.len()
    }

    /// Count of dealers we've received both pub and priv messages from.
    pub fn received_dealer_count(&self) -> usize {
        self.dealer_pub_msgs.keys().filter(|k| self.dealer_priv_msgs.contains_key(*k)).count()
    }

    /// Count of acks we've sent to dealers.
    pub fn acks_sent_count(&self) -> usize {
        self.acks_sent.len()
    }

    /// True if we've received pub+priv messages from all n participants.
    pub fn received_all_dealer_messages(&self) -> bool {
        self.received_dealer_count() >= self.config.n()
    }

    /// True if we've finalized and broadcast our dealer log.
    pub const fn has_sent_dealer_log(&self) -> bool {
        self.our_signed_log.is_some()
    }

    /// Check if our dealer has been finalized (log sent).
    /// Note: The Dealer tracks acks internally and finalize will use whatever
    /// acks have been received. We can't query the ack count directly.
    pub const fn dealer_has_been_finalized(&self) -> bool {
        self.our_signed_log.is_some()
    }

    /// Get the total number of participants.
    pub const fn total_participants(&self) -> usize {
        self.config.n()
    }

    /// Get the current phase of the DKG protocol.
    pub const fn current_phase(&self) -> DkgPhase {
        self.current_phase
    }

    /// Set the current phase.
    pub const fn set_phase(&mut self, phase: DkgPhase) {
        self.current_phase = phase;
    }

    /// Get the timestamp used for this ceremony.
    pub const fn timestamp_nanos(&self) -> u64 {
        self.timestamp_nanos
    }

    #[cfg(test)]
    pub(crate) fn seen_message_count(&self) -> usize {
        self.seen_messages.len()
    }

    /// Save current state to disk for crash recovery.
    pub fn save_state(&self, data_dir: &std::path::Path) -> Result<(), DkgError> {
        let mut state = PersistedDkgState::new(&self.session, self.timestamp_nanos);
        state.phase = self.current_phase;
        state.dealer_started = self.dealer.is_some() || self.our_signed_log.is_some();
        state.dealer_finalized = self.our_signed_log.is_some();

        if let Some(ref log) = self.our_signed_log {
            let mut bytes = Vec::new();
            log.write(&mut bytes);
            state.set_our_signed_log(bytes);
        }

        for (pk, log) in &self.signed_logs {
            let pk_hex = hex::encode(pk.as_ref());
            let mut bytes = Vec::new();
            log.write(&mut bytes);
            state.add_received_log(pk_hex, bytes);
        }

        state.save(data_dir)
    }

    /// Try to restore from persisted state.
    ///
    /// Returns `Ok(Some(participant))` if state was restored successfully.
    /// Returns `Ok(None)` if no state exists or session doesn't match.
    /// Returns `Err` on I/O or deserialization errors.
    pub fn try_restore(config: &DkgConfig, timestamp_nanos: u64) -> Result<Option<Self>, DkgError> {
        if !PersistedDkgState::exists(&config.data_dir) {
            return Ok(None);
        }

        let state = PersistedDkgState::load(&config.data_dir)?;
        let persisted_session = state.session()?;

        let expected_session =
            CeremonySession::new(config.chain_id, &config.participants, timestamp_nanos);

        if persisted_session.ceremony_id != expected_session.ceremony_id {
            info!(
                persisted = hex::encode(persisted_session.ceremony_id),
                expected = hex::encode(expected_session.ceremony_id),
                "Session mismatch, clearing old state"
            );
            PersistedDkgState::clear(&config.data_dir)?;
            return Ok(None);
        }

        info!(
            phase = %state.phase,
            dealer_started = state.dealer_started,
            dealer_finalized = state.dealer_finalized,
            logs_count = state.received_logs.len(),
            "Restoring DKG state from disk"
        );

        let mut participant = Self::new(config.clone(), timestamp_nanos)?;
        participant.current_phase = state.phase;

        if state.dealer_finalized
            && let Some(log_bytes) = state.get_our_signed_log()
        {
            let max_degree = config.t();
            let mut reader = log_bytes.as_slice();
            match SignedDealerLog::<MinSig, ed25519::PrivateKey>::read_cfg(
                &mut reader,
                &core::num::NonZeroU32::new(max_degree).unwrap(),
            ) {
                Ok(log) => {
                    if let Some((dealer_pk, dealer_log)) = log.clone().check(&participant.info) {
                        participant.dealer_logs.insert(dealer_pk.clone(), dealer_log);
                        participant.signed_logs.insert(dealer_pk, log.clone());
                        participant.our_signed_log = Some(log);
                    } else {
                        warn!(
                            "Failed to verify our own persisted dealer log during state restoration"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        ?e,
                        "Failed to deserialize our own persisted dealer log during state restoration"
                    );
                }
            }
        }

        let mut restored_log_count = 0usize;
        let received_logs = state.get_received_logs();
        let total_persisted_logs = received_logs.len();
        for (pk_hex, log_bytes) in received_logs {
            let max_degree = config.t();
            let mut reader = log_bytes.as_slice();
            match SignedDealerLog::<MinSig, ed25519::PrivateKey>::read_cfg(
                &mut reader,
                &core::num::NonZeroU32::new(max_degree).unwrap(),
            ) {
                Ok(log) => {
                    if let Some((dealer_pk, dealer_log)) = log.clone().check(&participant.info) {
                        participant.dealer_logs.insert(dealer_pk.clone(), dealer_log);
                        participant.signed_logs.insert(dealer_pk, log);
                        restored_log_count += 1;
                    } else {
                        warn!(
                            pk_hex,
                            "Failed to verify persisted dealer log during state restoration"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        pk_hex,
                        ?e,
                        "Failed to deserialize persisted dealer log during state restoration"
                    );
                }
            }
        }
        info!(
            restored_log_count,
            total_persisted_logs, "Restored dealer logs from persisted state"
        );

        Ok(Some(participant))
    }
}
