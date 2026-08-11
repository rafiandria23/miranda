use miranda_control_plane::{
    control_plane::ControlPlane,
    dispatcher::{DispatchStrategy, Dispatcher},
    queue::{DurableTaskQueue, TaskQueue},
    router::{DurableRouter, Router},
};
use miranda_storage::workflow_store::WorkflowStore;
use miranda_storage_mysql::{config::MySqlConfig, store::MySqlStore};
use miranda_storage_postgres::{config::PostgresConfig, store::PostgresStore};
use miranda_storage_sqlite::{config::SqliteConfig, store::SqliteStore};
use miranda_worker::{
    executor::DispatchExecutor,
    worker::{Worker, WorkerConfig},
};
use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::cli::Cli;

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

pub type ServerControlPlane = ControlPlane<
    Arc<dyn TaskQueue>,
    Arc<dyn Router>,
    Arc<dyn WorkflowStore>,
    Dispatcher<Arc<dyn TaskQueue>>,
>;

async fn build_control_plane(database_url: &str) -> Result<ServerControlPlane, Box<dyn Error>> {
    let store: Arc<dyn WorkflowStore>;
    let queue: Arc<dyn TaskQueue>;
    let router: Arc<dyn Router>;

    if database_url.starts_with("mysql://") {
        let backend = Arc::new(MySqlStore::connect(MySqlConfig::new(database_url)).await?);
        queue = Arc::new(DurableTaskQueue::new(backend.clone()));
        router = Arc::new(DurableRouter::new(backend.clone()));
        store = backend;
    } else if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        let backend = Arc::new(PostgresStore::connect(PostgresConfig::new(database_url)).await?);
        queue = Arc::new(DurableTaskQueue::new(backend.clone()));
        router = Arc::new(DurableRouter::new(backend.clone()));
        store = backend;
    } else if let Some(path) = database_url.strip_prefix("sqlite://") {
        let backend = Arc::new(SqliteStore::connect(SqliteConfig::new(path)).await?);
        queue = Arc::new(DurableTaskQueue::new(backend.clone()));
        router = Arc::new(DurableRouter::new(backend.clone()));
        store = backend;
    } else {
        return Err(format!("unsupported database URL scheme: {database_url}").into());
    };

    let dispatch = Dispatcher::new(queue.clone());

    Ok(ControlPlane::new(queue, router, store, dispatch))
}

async fn run_reaper<Q, R, S, D>(control_plane: Arc<ControlPlane<Q, R, S, D>>, period: Duration)
where
    Q: TaskQueue,
    R: Router,
    S: WorkflowStore,
    D: DispatchStrategy,
{
    let mut ticker = tokio::time::interval(period);

    loop {
        ticker.tick().await;

        match control_plane.reap_expired_leases().await {
            Ok(0) => {}
            Ok(n) => info!(recovered = n, "reaped expired leases"),
            Err(e) => warn!(error = %e, "reap_expired_leases failed"),
        }

        match control_plane.reap_stale_workers().await {
            Ok(0) => {}
            Ok(n) => info!(removed = n, "reaped stale workers"),
            Err(e) => warn!(error = %e, "reap_stale_workers failed"),
        }
    }
}

async fn run_control_plane_only(args: Cli) -> Result<(), Box<dyn Error>> {
    let database_url = args
        .database_url
        .ok_or("--database-url is required when using --control-plane")?;
    let control_plane = Arc::new(build_control_plane(&database_url).await?);

    let grpc_addr = args.grpc_bind.parse()?;
    let http_addr: SocketAddr = args.http_bind.parse()?;

    info!(grpc = %grpc_addr, http = %http_addr, "starting control-plane servers");

    tokio::spawn(run_reaper(control_plane.clone(), Duration::from_secs(30)));

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

async fn run_worker_only(args: Cli) -> Result<(), Box<dyn Error>> {
    let control_plane_url = args
        .control_plane_url
        .ok_or("--control-plane-url is required when using --worker without --control-plane")?;

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

async fn run_colocated(args: Cli) -> Result<(), Box<dyn Error>> {
    let database_url = args
        .database_url
        .clone()
        .ok_or("--database-url is required when using --control-plane and --worker together")?;

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

    tokio::spawn(run_reaper(control_plane.clone(), Duration::from_secs(30)));

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

pub async fn run(args: Cli) -> Result<(), Box<dyn Error>> {
    match (args.control_plane, args.worker) {
        (true, true) => run_colocated(args).await,
        (true, false) => run_control_plane_only(args).await,
        (false, true) => run_worker_only(args).await,
        (false, false) => Err("must specify at least one of --control-plane or --worker".into()),
    }
}
