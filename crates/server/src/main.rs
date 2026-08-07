mod cli;
mod grpc;
mod local_client;
mod role;

use clap::Parser;
use std::process::ExitCode;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    match role::run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
