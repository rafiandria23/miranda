use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "miranda", about = "Miranda workflow engine CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Run {
        path: PathBuf,
    },

    #[command(subcommand)]
    Db(DbCommand),

    Submit {
        path: PathBuf,

        #[arg(long)]
        server: String,
    },

    Register {
        path: PathBuf,

        #[arg(long)]
        server: String,
    },

    Status {
        execution_id: String,

        #[arg(long)]
        server: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum DbCommand {
    Create {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },

    Migrate {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_command_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_run_command() {
        let cli = Cli::try_parse_from(["miranda", "run", "workflow.yaml"]).unwrap();
        assert!(
            matches!(cli.command, Command::Run { path } if path.as_path() == std::path::Path::new("workflow.yaml"))
        );
    }

    #[test]
    fn parses_submit_command_with_server_flag() {
        let cli = Cli::try_parse_from([
            "miranda",
            "submit",
            "workflow.yaml",
            "--server",
            "http://localhost:8080",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Submit { path, server }
                if path.as_path() == std::path::Path::new("workflow.yaml") && server == "http://localhost:8080"
        ));
    }

    #[test]
    fn parses_register_command_with_server_flag() {
        let cli = Cli::try_parse_from([
            "miranda",
            "register",
            "workflow.yaml",
            "--server",
            "http://localhost:8080",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Register { path, server }
                if path.as_path() == std::path::Path::new("workflow.yaml") && server == "http://localhost:8080"
        ));
    }

    #[test]
    fn parses_status_command_with_execution_id_and_server_flag() {
        let cli = Cli::try_parse_from([
            "miranda",
            "status",
            "exec-123",
            "--server",
            "http://localhost:8080",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Status { execution_id, server }
                if execution_id == "exec-123" && server == "http://localhost:8080"
        ));
    }

    #[test]
    fn submit_requires_server_flag() {
        let result = Cli::try_parse_from(["miranda", "submit", "workflow.yaml"]);
        assert!(result.is_err());
    }

    #[test]
    fn parses_db_create_subcommand() {
        let cli = Cli::try_parse_from([
            "miranda",
            "db",
            "create",
            "--database-url",
            "postgres://localhost/db",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Db(DbCommand::Create { database_url }) if database_url == "postgres://localhost/db"
        ));
    }

    #[test]
    fn parses_db_migrate_subcommand() {
        let cli = Cli::try_parse_from([
            "miranda",
            "db",
            "migrate",
            "--database-url",
            "postgres://localhost/db",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Db(DbCommand::Migrate { database_url }) if database_url == "postgres://localhost/db"
        ));
    }

    #[test]
    fn rejects_unknown_command() {
        let result = Cli::try_parse_from(["miranda", "not-a-command"]);
        assert!(result.is_err());
    }
}
