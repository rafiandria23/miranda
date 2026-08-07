use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "miranda-server", about = "Miranda workflow engine server")]
pub struct Cli {
    #[arg(long, value_enum)]
    pub role: Role,

    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    #[arg(long)]
    pub control_plane_url: Option<String>,

    #[arg(long, default_value = "0.0.0.0:7433")]
    pub grpc_bind: String,

    #[arg(long, default_value = "0.0.0.0:8080")]
    pub http_bind: String,

    #[arg(long, value_delimiter = ',')]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Role {
    ControlPlane,
    Worker,
    Both,
}
