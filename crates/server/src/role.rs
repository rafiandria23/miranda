use miranda_control_plane::{
    ControlPlane, dispatcher::Dispatcher, queue::InMemoryTaskQueue, router::InMemoryRouter,
};
use miranda_storage::WorkflowStore;
use miranda_storage_mysql::{MySqlConfig, MySqlStore};
use miranda_storage_postgres::{PostgresConfig, PostgresStore};
use miranda_storage_sqlite::{SqliteConfig, SqliteStore};
use miranda_worker::{Worker, WorkerConfig, executor::DispatchExecutor};
use std::{error::Error, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::info;

use crate::cli::{Cli, Role};

pub async fn run(args: Cli) -> Result<(), Box<dyn Error>> {
    match args.role {
        Role::ControlPlane => run_control_plane(args).await,
        Role::Worker => run_worker(args).await,
        Role::Both => run_both(args).await,
    }
}

async fn run_control_plane(args: Cli) -> Result<(), Box<dyn Error>> {
    let database_url = args
        .database_url
        .ok_or("--database-url is required for --role control-plane")?;
    let control_plane = Arc::new(build_control_plane(&database_url).await?);

    let grpc_addr = args.grpc_bind.parse()?;
    let http_addr: SocketAddr = args.http_bind.parse()?;

    info!(grpc = %grpc_addr, http = %http_addr, "starting control-plane servers");

    let grpc_server = crate::grpc::service::serve(control_plane.clone(), grpc_addr);

    let http_router = crate::api::router(control_plane.clone());
    let http_listener = TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_router);

    tokio::select! {
        result = grpc_server => result?,

        result = http_server => result.map_err(|e| Box::new(e) as Box<dyn Error>)?,

        _ = shutdown_signal() => {
            info!("shutdown signal received");
        }
    }

    Ok(())
}

async fn run_worker(args: Cli) -> Result<(), Box<dyn Error>> {
    let control_plane_url = args
        .control_plane_url
        .ok_or("--control-plane-url is required for --role worker")?;

    let client =
        Arc::new(crate::grpc::client::RemoteControlPlaneClient::connect(control_plane_url).await?);
    let executor = Arc::new(DispatchExecutor::new());

    let worker = Worker::new(args.capabilities, client, executor, WorkerConfig::default());

    info!(worker_id = %worker.id(), "starting worker");

    let handle = worker.run();

    shutdown_signal().await;

    info!("shutdown signal received, draining worker");

    handle.shutdown().await.map_err(|e| e.to_string())?;

    Ok(())
}

async fn run_both(args: Cli) -> Result<(), Box<dyn Error>> {
    let database_url = args
        .database_url
        .clone()
        .ok_or("--database-url is required for --role both")?;

    let control_plane = Arc::new(build_control_plane(&database_url).await?);

    let client = Arc::new(crate::local_client::LocalControlPlaneClient::new(
        control_plane.clone(),
    ));
    let executor = Arc::new(DispatchExecutor::new());

    let worker = Worker::new(args.capabilities, client, executor, WorkerConfig::default());
    let worker_handle = worker.run();

    let grpc_addr = args.grpc_bind.parse()?;
    let http_addr: SocketAddr = args.http_bind.parse()?;

    info!(
        grpc = %grpc_addr,
        http = %http_addr,
        "starting control-plane gRPC + HTTP servers (also serving co-located worker in-process)"
    );

    let grpc_server = crate::grpc::service::serve(control_plane.clone(), grpc_addr);

    let http_router = crate::api::router(control_plane.clone());
    let http_listener = TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_router);

    tokio::select! {
        result = grpc_server => result?,

        result = http_server => result.map_err(|e| Box::new(e) as Box<dyn Error>)?,

        _ = shutdown_signal() => {
            info!("shutdown signal received, draining worker");

            worker_handle.shutdown().await.map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

pub type ServerControlPlane = ControlPlane<
    InMemoryTaskQueue,
    InMemoryRouter,
    Arc<dyn WorkflowStore>,
    Dispatcher<InMemoryTaskQueue>,
>;

async fn build_control_plane(database_url: &str) -> Result<ServerControlPlane, Box<dyn Error>> {
    let store: Arc<dyn WorkflowStore> = if database_url.starts_with("mysql://") {
        Arc::new(MySqlStore::connect(MySqlConfig::new(database_url)).await?)
    } else if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        Arc::new(PostgresStore::connect(PostgresConfig::new(database_url)).await?)
    } else if let Some(path) = database_url.strip_prefix("sqlite://") {
        Arc::new(SqliteStore::connect(SqliteConfig::new(path)).await?)
    } else {
        return Err(format!("unsupported database URL scheme: {database_url}").into());
    };

    let queue = InMemoryTaskQueue::new();
    let router = InMemoryRouter::new();
    let dispatch = Dispatcher::new(queue.clone());

    Ok(ControlPlane::new(queue, router, store, dispatch))
}
