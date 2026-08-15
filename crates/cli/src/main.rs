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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_workflow_fails_when_the_file_does_not_exist() {
        let result = run_workflow(Path::new("/nonexistent/workflow.yaml")).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn register_workflow_fails_when_the_file_does_not_exist() {
        let result = register_workflow(
            Path::new("/nonexistent/workflow.yaml"),
            "http://localhost:1",
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_workflow_fails_when_the_file_does_not_exist() {
        let result = submit_workflow(
            Path::new("/nonexistent/workflow.yaml"),
            "http://localhost:1",
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_status_fails_for_an_invalid_execution_id() {
        let result = check_status("not-a-valid-execution-id", "http://localhost:1").await;

        assert!(result.is_err());
    }

    #[test]
    fn cli_parses_the_run_command() {
        let cli = Cli::parse_from(["miranda", "run", "workflow.yaml"]);

        match cli.command {
            Command::Run { path } => assert_eq!(path, Path::new("workflow.yaml")),
            other => panic!("expected Command::Run, got {other:?}"),
        }
    }

    #[test]
    fn cli_parses_the_status_command() {
        let cli = Cli::parse_from([
            "miranda",
            "status",
            "exec-123",
            "--server",
            "http://localhost:8080",
        ]);

        match cli.command {
            Command::Status {
                execution_id,
                server,
            } => {
                assert_eq!(execution_id, "exec-123");
                assert_eq!(server, "http://localhost:8080");
            }
            other => panic!("expected Command::Status, got {other:?}"),
        }
    }

    #[test]
    fn cli_rejects_an_unknown_command() {
        let result = Cli::try_parse_from(["miranda", "bogus"]);

        assert!(result.is_err());
    }
}
