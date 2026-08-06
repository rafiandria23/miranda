mod cli;
mod config_dir;
mod db;
mod run;

use clap::Parser;
use std::{error::Error, path::Path, process::ExitCode};

use crate::cli::{Cli, Command, DbCommand};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    let result: Result<(), Box<dyn Error>> = match args.command {
        Command::Run { path } => run_workflow(&path).await,

        Command::Db(DbCommand::Create { database_url }) => db::create(&database_url).await,
        Command::Db(DbCommand::Migrate { database_url }) => db::migrate(&database_url).await,

        Command::Submit { .. } | Command::Register { .. } | Command::Status { .. } => {
            Err("this command requires miranda-server, which does not exist yet".into())
        }
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
