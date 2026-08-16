use miranda_control_plane::{
    control_plane::ControlPlane,
    dispatcher::{DispatchStrategy, Dispatcher},
    leadership::LeadershipRunner,
    lease_manager::LeaseManager,
    notifier::TaskNotifier,
    queue::{DurableTaskQueue, TaskQueue},
    router::{DurableRouter, Router},
};
use miranda_storage::{
    artifact_store::ArtifactStore, filesystem::FilesystemStore, join_token_store::JoinTokenStore,
    leadership_store::LeadershipStore, lease_store::LeaseStore, peer_store::PeerStore,
    workflow_store::WorkflowStore,
};
use miranda_storage_mysql::{config::MySqlConfig, store::MySqlStore};
use miranda_storage_postgres::{config::PostgresConfig, store::PostgresStore};
use miranda_storage_sqlite::{config::SqliteConfig, store::SqliteStore};
use miranda_worker::{
    executor::DispatchExecutor,
    worker::{Worker, WorkerConfig},
};
use std::{error::Error, net::SocketAddr, path::PathBuf, sync::Arc};
use time::Duration;
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::{
    cli::Cli,
    grpc::{control_plane_service::peers::PeerManager, worker_service::notifier::GrpcTaskNotifier},
};

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn is_unspecified_host(host: &str) -> bool {
    host == "0.0.0.0" || host == "::" || host == "[::]"
}

fn resolve_advertise_address(
    advertise_address: &Option<String>,
    grpc_addr: SocketAddr,
) -> Result<String, Box<dyn Error>> {
    let host = match advertise_address {
        Some(host) => host.clone(),
        None => grpc_addr.ip().to_string(),
    };

    if grpc_addr.ip().is_unspecified() || is_unspecified_host(&host) {
        return Err(format!(
            "cannot advertise an unspecified address ({host}) to peers — pass \
            --advertise-address with a real, dialable host (e.g. 127.0.0.1 for \
            local testing, or this machine's actual reachable IP/hostname for a \
            real deployment)"
        )
        .into());
    }

    Ok(format!("http://{host}:{}", grpc_addr.port()))
}

fn resolve_home_relative(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }

    return PathBuf::from(path);
}

pub type ServerControlPlane = ControlPlane<
    Arc<dyn TaskQueue>,
    Arc<dyn Router>,
    Arc<dyn WorkflowStore>,
    Dispatcher<Arc<dyn TaskQueue>>,
    Arc<dyn TaskNotifier>,
    Arc<dyn LeaseStore>,
>;

async fn build_control_plane(
    database_url: &str,
) -> Result<
    (
        ServerControlPlane,
        GrpcTaskNotifier,
        Arc<dyn JoinTokenStore>,
        Arc<dyn LeadershipStore>,
        Arc<dyn PeerStore>,
    ),
    Box<dyn Error>,
> {
    let store: Arc<dyn WorkflowStore>;
    let queue: Arc<dyn TaskQueue>;
    let router: Arc<dyn Router>;
    let lease_store: Arc<dyn LeaseStore>;
    let join_token_store: Arc<dyn JoinTokenStore>;
    let leadership_store: Arc<dyn LeadershipStore>;
    let peer_store: Arc<dyn PeerStore>;

    let grpc_notifier = GrpcTaskNotifier::new();
    let notifier: Arc<dyn TaskNotifier> = Arc::new(grpc_notifier.clone());

    if database_url.starts_with("mysql://") {
        let backend = Arc::new(MySqlStore::connect(MySqlConfig::new(database_url)).await?);
        queue = Arc::new(DurableTaskQueue::new(backend.clone()));
        router = Arc::new(DurableRouter::new(backend.clone()));
        lease_store = backend.clone();
        join_token_store = backend.clone();
        leadership_store = backend.clone();
        peer_store = backend.clone();
        store = backend;
    } else if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        let backend = Arc::new(PostgresStore::connect(PostgresConfig::new(database_url)).await?);
        queue = Arc::new(DurableTaskQueue::new(backend.clone()));
        router = Arc::new(DurableRouter::new(backend.clone()));
        lease_store = backend.clone();
        join_token_store = backend.clone();
        leadership_store = backend.clone();
        peer_store = backend.clone();
        store = backend;
    } else if let Some(path) = database_url.strip_prefix("sqlite://") {
        let backend = Arc::new(SqliteStore::connect(SqliteConfig::new(path)).await?);
        queue = Arc::new(DurableTaskQueue::new(backend.clone()));
        router = Arc::new(DurableRouter::new(backend.clone()));
        lease_store = backend.clone();
        join_token_store = backend.clone();
        leadership_store = backend.clone();
        peer_store = backend.clone();
        store = backend;
    } else {
        return Err(format!("unsupported database URL scheme: {database_url}").into());
    };

    let dispatch = Dispatcher::new(queue.clone());
    let leases = LeaseManager::new(lease_store);

    let control_plane = ControlPlane::new(queue, router, store, dispatch, notifier, leases);

    Ok((
        control_plane,
        grpc_notifier,
        join_token_store,
        leadership_store,
        peer_store,
    ))
}

async fn run_reaper<Q, R, S, D, N, L, P>(
    control_plane: Arc<ControlPlane<Q, R, S, D, N, L>>,
    leadership: Arc<LeadershipRunner<Arc<dyn LeadershipStore>>>,
    peer_store: P,
    period: Duration,
) where
    Q: TaskQueue,
    R: Router,
    S: WorkflowStore,
    D: DispatchStrategy,
    N: TaskNotifier,
    L: LeaseStore,
    P: PeerStore,
{
    let std_period: std::time::Duration = period
        .try_into()
        .expect("reaper period must be non-negative");

    let mut ticker = tokio::time::interval(std_period);

    loop {
        ticker.tick().await;

        if !leadership.is_leader() {
            continue;
        }

        match control_plane.reap_expired_leases().await {
            Ok(0) => {}
            Ok(n) => tracing::info!(recovered = n, "reaped expired leases"),
            Err(e) => tracing::warn!(error = %e, "reap_expired_leases failed"),
        }

        match control_plane.reap_stale_workers().await {
            Ok(0) => {}
            Ok(n) => tracing::info!(removed = n, "reaped stale workers"),
            Err(e) => tracing::warn!(error = %e, "reap_stale_workers failed"),
        }

        match peer_store.reap_stale(Duration::seconds(120)).await {
            Ok(ids) if ids.is_empty() => {}
            Ok(ids) => tracing::info!(removed = ids.len(), "reaped stale control-plane instances"),
            Err(e) => tracing::warn!(error = %e, "reap_stale peer instances failed"),
        }
    }
}

async fn run_control_plane_only(args: Cli) -> Result<(), Box<dyn Error>> {
    let database_url = args
        .database_url
        .ok_or("--database-url is required when using --control-plane")?;
    let (control_plane, grpc_notifier, join_token_store, leadership_store, peer_store) =
        build_control_plane(&database_url).await?;

    let token = match args.join_token {
        Some(token) => {
            join_token_store.set_token(&token).await?;
            token
        }

        None => match join_token_store.get_token().await? {
            Some(existing) => existing,
            None => {
                let token = Uuid::new_v4().to_string();
                join_token_store.set_token(&token).await?;
                token
            }
        },
    };

    let control_plane = control_plane.with_join_tokens(join_token_store);
    let control_plane = Arc::new(control_plane);

    let leadership = Arc::new(LeadershipRunner::new(leadership_store));
    tokio::spawn({
        let leadership = leadership.clone();
        async move { leadership.run().await }
    });

    let grpc_addr = args.grpc_bind.parse()?;
    let http_addr: SocketAddr = args.http_bind.parse()?;

    let advertise_address = resolve_advertise_address(&args.advertise_address, grpc_addr)?;

    let peer_manager = Arc::new(PeerManager::new(advertise_address, peer_store.clone()));
    grpc_notifier.set_peers(peer_manager.clone()).await;
    tokio::spawn({
        let peer_manager = peer_manager.clone();
        async move { peer_manager.run().await }
    });

    tracing::info!(grpc = %grpc_addr, http = %http_addr, "starting control-plane servers");
    tracing::info!(
        "join a worker with: miranda-server --worker --control-plane-url http://{grpc_addr} --join-token {token}"
    );

    tokio::spawn(run_reaper(
        control_plane.clone(),
        leadership,
        peer_store,
        Duration::seconds(60),
    ));

    let grpc_server = crate::grpc::serve_all(control_plane.clone(), grpc_notifier, grpc_addr);

    let http_router = crate::http::router(control_plane.clone());
    let http_listener = TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_router);

    tokio::select! {
        result = grpc_server => result?,

        result = http_server => result.map_err(|e| Box::new(e) as Box<dyn Error>)?,

        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received");
        }
    }

    Ok(())
}

async fn run_worker_only(args: Cli) -> Result<(), Box<dyn Error>> {
    let control_plane_url = args
        .control_plane_url
        .ok_or("--control-plane-url is required when using --worker without --control-plane")?;

    let token = args
        .join_token
        .ok_or("--join-token is required when using --worker without --control-plane")?;

    let client = Arc::new(
        crate::grpc::worker_service::client::RemoteControlPlaneClient::connect(control_plane_url)
            .await?,
    );

    let artifact_dir = resolve_home_relative(&args.artifact_dir);
    let artifact_store: Arc<dyn ArtifactStore> = Arc::new(FilesystemStore::new(artifact_dir));
    let work_dir_root = resolve_home_relative("~/.miranda/work");

    let executor = Arc::new(DispatchExecutor::new(artifact_store, work_dir_root));

    let worker = Worker::new(
        args.capabilities,
        client,
        executor,
        WorkerConfig::default(),
        token,
    );
    let worker_id = worker.id();

    let (handle, started) = worker.run();

    match started.await {
        Ok(Ok(())) => {
            tracing::info!(worker_id = %worker_id, "worker started successfully");
        }

        Ok(Err(worker_error)) => {
            return Err(format!("worker failed to start: {worker_error}").into());
        }

        Err(_) => {
            return Err("worker task ended before reporting startup result".into());
        }
    }

    shutdown_signal().await;

    tracing::info!("shutdown signal received, draining worker");

    handle.shutdown().await.map_err(|e| e.to_string())?;

    Ok(())
}

async fn run_colocated(args: Cli) -> Result<(), Box<dyn Error>> {
    let database_url = args
        .database_url
        .clone()
        .ok_or("--database-url is required when using --control-plane and --worker together")?;

    let (control_plane, grpc_notifier, join_token_store, leadership_store, peer_store) =
        build_control_plane(&database_url).await?;

    let token = match args.join_token {
        Some(token) => {
            join_token_store.set_token(&token).await?;
            token
        }

        None => match join_token_store.get_token().await? {
            Some(existing) => existing,
            None => {
                let token = Uuid::new_v4().to_string();
                join_token_store.set_token(&token).await?;
                token
            }
        },
    };

    let control_plane = control_plane.with_join_tokens(join_token_store);
    let control_plane = Arc::new(control_plane);

    let leadership = Arc::new(LeadershipRunner::new(leadership_store));
    tokio::spawn({
        let leadership = leadership.clone();
        async move { leadership.run().await }
    });

    let client = Arc::new(crate::local_client::LocalControlPlaneClient::new(
        control_plane.clone(),
    ));

    let artifact_dir = resolve_home_relative(&args.artifact_dir);
    let artifact_store: Arc<dyn ArtifactStore> = Arc::new(FilesystemStore::new(artifact_dir));
    let work_dir_root = resolve_home_relative("~/.miranda/work");

    let executor = Arc::new(DispatchExecutor::new(artifact_store, work_dir_root));

    let worker = Worker::new(
        args.capabilities,
        client,
        executor,
        WorkerConfig::default(),
        String::new(),
    );
    let (worker_handle, _started) = worker.run();

    let grpc_addr = args.grpc_bind.parse()?;
    let http_addr: SocketAddr = args.http_bind.parse()?;

    let advertise_address = resolve_advertise_address(&args.advertise_address, grpc_addr)?;

    let peer_manager = Arc::new(PeerManager::new(advertise_address, peer_store.clone()));
    grpc_notifier.set_peers(peer_manager.clone()).await;
    tokio::spawn({
        let peer_manager = peer_manager.clone();
        async move { peer_manager.run().await }
    });

    tracing::info!(
        grpc = %grpc_addr,
        http = %http_addr,
        "starting control-plane gRPC + HTTP servers (also serving co-located worker in-process)"
    );
    tracing::info!(
        "join a worker with: miranda-server --worker --control-plane-url http://{grpc_addr} --join-token {token}"
    );

    tokio::spawn(run_reaper(
        control_plane.clone(),
        leadership,
        peer_store,
        Duration::seconds(60),
    ));

    let grpc_server = crate::grpc::serve_all(control_plane.clone(), grpc_notifier, grpc_addr);

    let http_router = crate::http::router(control_plane.clone());
    let http_listener = TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_router);

    tokio::select! {
        result = grpc_server => result?,

        result = http_server => result.map_err(|e| Box::new(e) as Box<dyn Error>)?,

        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, draining worker");

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
