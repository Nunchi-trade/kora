//! Trusted dealer DKG for devnet.
//!
//! Generates all BLS12-381 threshold shares using a single trusted dealer.
//! This is NOT secure for production but allows testing the validator workflow.

use std::{fs, path::PathBuf};

use clap::Args;
use commonware_codec::{ReadExt, Write as _};
use commonware_cryptography::bls12381::{
    dkg::feldman_desmedt as dkg,
    primitives::{sharing::Mode, variant::MinSig},
};
use commonware_utils::{Faults, N3f1, TryCollect, ordered::Set};
use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::secret_file::write_secret_file;

#[derive(Args, Debug)]
pub(crate) struct DkgDealArgs {
    #[arg(long, default_value = "4")]
    pub validators: usize,

    #[arg(long, default_value = "/shared")]
    pub output_dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct OutputJson {
    group_public_key: String,
    public_polynomial: String,
    threshold: u32,
    participants: usize,
    participant_keys: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct ShareJson {
    index: u32,
    secret: String,
}

impl Drop for ShareJson {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

pub(crate) fn run(args: DkgDealArgs) -> Result<()> {
    let quorum = N3f1::quorum(args.validators);

    tracing::warn!(
        "TRUSTED DEALER MODE: all threshold shares are generated in a single process. \
         This is NOT secure for production. The output directory will contain the complete \
         group secret key material until cleanup."
    );

    tracing::info!(
        validators = args.validators,
        quorum = quorum,
        max_faulty = args.validators as u32 - quorum,
        "Running trusted dealer DKG (quorum determined by N3f1: need {} of {} validators)",
        quorum,
        args.validators
    );

    let mut participants = Vec::with_capacity(args.validators);
    for i in 0..args.validators {
        let node_dir = args.output_dir.join(format!("node{}", i));
        let setup_path = node_dir.join("setup.json");

        let setup_str = fs::read_to_string(&setup_path)
            .wrap_err_with(|| format!("Failed to read setup.json for node{}", i))?;
        let setup: serde_json::Value = serde_json::from_str(&setup_str)?;

        let pk_hex = setup["public_key"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("missing public_key in setup.json"))?;

        let pk_bytes = hex::decode(pk_hex)?;
        let pk = commonware_cryptography::ed25519::PublicKey::read(&mut pk_bytes.as_slice())
            .map_err(|e| eyre::eyre!("Failed to decode public key: {:?}", e))?;

        participants.push(pk);
        tracing::debug!(node = i, pk = %pk_hex, "Loaded participant");
    }

    let participants_set: Set<commonware_cryptography::ed25519::PublicKey> = participants
        .iter()
        .cloned()
        .try_collect()
        .map_err(|_| eyre::eyre!("Duplicate participants"))?;

    let participant_keys: Vec<String> = participants_set
        .iter()
        .map(|pk| {
            let mut bytes = Vec::new();
            pk.write(&mut bytes);
            hex::encode(bytes)
        })
        .collect();

    let mut rng = rand::rngs::OsRng;

    tracing::info!("Generating BLS threshold key shares");
    let (public_output, shares) =
        dkg::deal::<MinSig, _, N3f1>(&mut rng, Mode::default(), participants_set)
            .map_err(|e| eyre::eyre!("DKG deal failed: {:?}", e))?;

    let sharing = public_output.public();

    let mut public_polynomial_bytes = Vec::new();
    sharing.write(&mut public_polynomial_bytes);

    let group_key = sharing.public();
    let mut group_key_bytes = Vec::new();
    group_key.write(&mut group_key_bytes);

    tracing::info!(
        group_key = hex::encode(&group_key_bytes),
        polynomial_len = public_polynomial_bytes.len(),
        "Generated group public key and polynomial"
    );

    for (i, pk) in participants.iter().enumerate() {
        let share =
            shares.get_value(pk).ok_or_else(|| eyre::eyre!("Missing share for node{}", i))?;

        let mut share_bytes = Vec::new();
        share.write(&mut share_bytes);

        let node_dir = args.output_dir.join(format!("node{}", i));

        let output_json = OutputJson {
            group_public_key: hex::encode(&group_key_bytes),
            public_polynomial: hex::encode(&public_polynomial_bytes),
            threshold: quorum,
            participants: args.validators,
            participant_keys: participant_keys.clone(),
        };
        let output_path = node_dir.join("output.json");
        write_secret_file(&output_path, serde_json::to_string_pretty(&output_json)?.as_bytes())?;

        let share_hex = hex::encode(&share_bytes);
        // Zeroize the raw share bytes now that we have the hex encoding.
        share_bytes.zeroize();

        let share_json = ShareJson { index: share.index.get(), secret: share_hex };
        let share_path = node_dir.join("share.key");
        let mut share_content = serde_json::to_string_pretty(&share_json)?;
        write_secret_file(&share_path, share_content.as_bytes())?;
        share_content.zeroize();

        tracing::info!(node = i, "Wrote DKG output and share");
    }

    // Explicitly drop shares to release secret key material (the allocator will
    // reuse the memory, but we cannot call Zeroize on the opaque library type).
    drop(shares);
    drop(public_output);

    tracing::info!("Trusted dealer DKG complete");
    tracing::info!("  Validators: {}", args.validators);
    tracing::info!("  Quorum (N3f1): {}", quorum);

    Ok(())
}
