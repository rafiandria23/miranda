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
