mod cli;
mod config_dir;
mod run;

use clap::Parser;
use std::{error::Error, path::Path, process::ExitCode};

use crate::cli::{Cli, Command};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    let result = match args.command {
        Command::Run { path } => run_workflow(&path).await,
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run_workflow(path: &Path) -> Result<(), Box<dyn Error>> {
    let yaml = std::fs::read_to_string(path)?;

    run::embedded::run_from_yaml(&yaml).await?;

    Ok(())
}
