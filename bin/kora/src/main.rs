#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/refcell/kora/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod cli;

fn main() -> eyre::Result<()> {
    use clap::Parser;
    use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

    kora_cli::Backtracing::enable();
    kora_cli::SigsegvHandler::install();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,kora_runner=info,kora_rpc=info,kora_executor=info,commonware_consensus=info,commonware_p2p=warn",
        )
    });

    let json_format = std::env::var("LOG_FORMAT").map(|v| v == "json").unwrap_or(false);
    if json_format {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().json().boxed())
            .with(filter)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().boxed())
            .with(filter)
            .init();
    }

    cli::Cli::parse().run()
}
