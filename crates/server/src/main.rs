mod api;
mod bootstrap;
mod cli;
mod grpc;
mod local_client;

use clap::Parser;
use std::process::ExitCode;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    match bootstrap::run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
