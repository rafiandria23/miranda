use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "miranda-server", about = "Miranda workflow engine server")]
pub struct Cli {
    #[arg(long)]
    pub control_plane: bool,

    #[arg(long)]
    pub worker: bool,

    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    #[arg(long)]
    pub control_plane_url: Option<String>,

    #[arg(long)]
    pub join_token: Option<String>,

    #[arg(long, default_value = "0.0.0.0:7433")]
    pub grpc_bind: String,

    #[arg(long, default_value = "0.0.0.0:8080")]
    pub http_bind: String,

    #[arg(long)]
    pub advertise_address: Option<String>,

    #[arg(long, value_delimiter = ',')]
    pub capabilities: Vec<String>,

    #[arg(long, default_value = "~/.miranda/artifacts")]
    pub artifact_dir: String,
}
