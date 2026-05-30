#![doc = "Key generation and setup tool for Kora devnet."]

use clap::Parser;
use eyre::Result;
use tracing_subscriber::{EnvFilter, fmt};

mod dkg_deal;
mod secret_file;
mod setup;

#[derive(Parser, Debug)]
#[command(name = "keygen")]
#[command(about = "Key generation and setup tool for Kora devnet")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Generate validator identity keys and network configuration
    Setup(setup::SetupArgs),
    /// Generate BLS threshold shares using trusted dealer (devnet only)
    DkgDeal(dkg_deal::DkgDealArgs),
}

fn main() -> Result<()> {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Setup(args) => setup::run(args),
        Commands::DkgDeal(args) => dkg_deal::run(args),
    }
}
