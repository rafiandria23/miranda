mod cli;
mod config_dir;
mod db;
mod remote;
mod run;

use clap::Parser;
use miranda_core::id::ExecutionId;
use std::{error::Error, fs, path::Path, process::ExitCode};

use crate::cli::{Cli, Command, DbCommand};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    let result: Result<(), Box<dyn Error>> = match args.command {
        Command::Run { path } => run_workflow(&path).await,

        Command::Db(DbCommand::Create { database_url }) => db::create(&database_url).await,
        Command::Db(DbCommand::Migrate { database_url }) => db::migrate(&database_url).await,

        Command::Register { path, server } => register_workflow(&path, &server).await,
        Command::Submit { path, server } => submit_workflow(&path, &server).await,
        Command::Status {
            execution_id,
            server,
        } => check_status(&execution_id, &server).await,
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
    let yaml = fs::read_to_string(path)?;

    run::embedded::run_from_yaml(&yaml).await?;

    Ok(())
}

async fn register_workflow(path: &Path, server: &str) -> Result<(), Box<dyn Error>> {
    let yaml = fs::read_to_string(path)?;
    let (workflow, definition) = miranda_core::spec::compile(&yaml)?;

    let response = remote::register(server, workflow.name(), &definition).await?;

    println!(
        "registered: workflow_id={} version_id={}",
        response.workflow_id, response.version_id
    );

    Ok(())
}

async fn submit_workflow(path: &Path, server: &str) -> Result<(), Box<dyn Error>> {
    let yaml = fs::read_to_string(path)?;
    let (workflow, definition) = miranda_core::spec::compile(&yaml)?;

    let registered = remote::register(server, workflow.name(), &definition).await?;
    let submitted = remote::submit(server, registered.version_id).await?;

    println!("submitted: execution_id={}", submitted.execution_id);

    Ok(())
}

async fn check_status(execution_id: &str, server: &str) -> Result<(), Box<dyn Error>> {
    let execution_id: ExecutionId = execution_id.parse()?;
    let response = remote::status(server, execution_id).await?;

    println!(
        "execution {}: {} (version {})",
        response.execution_id, response.status, response.version
    );

    Ok(())
}
