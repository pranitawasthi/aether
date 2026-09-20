use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use agent_runtime::{
    api,
    distributed::{DistributedWorker, DistributedWorkerConfig, DurableTaskExecutor},
    memory::{MemoryConfig, MemoryManager},
    runtime::AgentRuntime,
    sandbox::SandboxConfig,
    storage::PostgresStorage,
    tool::{ToolConfig, ToolExecutor},
    worker::MockTaskExecutor,
};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let executor = Arc::new(MockTaskExecutor::new(Duration::from_secs(1)));
    let tool_config = tool_config_from_env();

    if let Ok(worker_id) = std::env::var("AGENT_RUNTIME_WORKER_ID") {
        return run_distributed_worker(worker_id, executor, tool_config).await;
    }

    let memory = MemoryManager::from_config(memory_config_from_env()).await?;
    let durable_storage = durable_storage_from_env().await?;
    let runtime = Arc::new(AgentRuntime::new_with_memory_and_storage(
        executor,
        3,
        tool_config,
        memory,
        durable_storage,
    ));
    let app = api::router(runtime.clone());
    let address =
        std::env::var("AGENT_RUNTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = tokio::net::TcpListener::bind(&address).await?;

    tracing::info!(%address, "Agent runtime API listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    runtime.shutdown().await;
    Ok(())
}

async fn run_distributed_worker(
    worker_id: String,
    generic_executor: Arc<MockTaskExecutor>,
    tool_config: ToolConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("AGENT_RUNTIME_DATABASE_URL")?;
    let max_connections = std::env::var("AGENT_RUNTIME_DATABASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let storage = PostgresStorage::connect(&database_url, max_connections).await?;
    storage.migrate().await?;
    let tools = ToolExecutor::with_defaults(tool_config)?;
    let executor = Arc::new(DurableTaskExecutor::new(
        generic_executor,
        storage.clone(),
        tools,
    ));
    let worker =
        DistributedWorker::new(storage, executor, DistributedWorkerConfig::new(worker_id))?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move { worker.run_until_shutdown(shutdown_rx).await });

    shutdown_signal().await;
    let _ = shutdown_tx.send(true);
    worker_handle.await??;
    Ok(())
}

fn memory_config_from_env() -> MemoryConfig {
    MemoryConfig {
        redis_url: std::env::var("AGENT_RUNTIME_REDIS_URL").ok(),
        postgres_url: std::env::var("AGENT_RUNTIME_DATABASE_URL").ok(),
        postgres_max_connections: std::env::var("AGENT_RUNTIME_DATABASE_MAX_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5),
    }
}

async fn durable_storage_from_env() -> Result<Option<PostgresStorage>, Box<dyn std::error::Error>> {
    let enabled = std::env::var("AGENT_RUNTIME_DURABLE_QUEUE")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE"));
    if !enabled {
        return Ok(None);
    }

    let database_url = std::env::var("AGENT_RUNTIME_DATABASE_URL")?;
    let max_connections = std::env::var("AGENT_RUNTIME_DATABASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let storage = PostgresStorage::connect(&database_url, max_connections).await?;
    storage.migrate().await?;
    Ok(Some(storage))
}

fn tool_config_from_env() -> ToolConfig {
    let mut config = ToolConfig::default();

    if let Ok(root) = std::env::var("AGENT_RUNTIME_TOOL_ROOT") {
        config.file_root = PathBuf::from(root);
    }

    if let Ok(hosts) = std::env::var("AGENT_RUNTIME_ALLOWED_HTTP_HOSTS") {
        config.allowed_http_hosts = hosts
            .split(',')
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .map(str::to_ascii_lowercase)
            .collect::<BTreeSet<_>>();
    }

    if let Ok(programs) = std::env::var("AGENT_RUNTIME_ALLOWED_SHELL_PROGRAMS") {
        config.allowed_shell_programs = programs
            .split(',')
            .map(str::trim)
            .filter(|program| !program.is_empty())
            .map(str::to_ascii_lowercase)
            .collect::<BTreeSet<_>>();
    }

    if let Ok(python) = std::env::var("AGENT_RUNTIME_PYTHON_BIN") {
        if !python.trim().is_empty() {
            config.python_program = python;
        }
    }

    config.enable_host_process_tools = std::env::var("AGENT_RUNTIME_ENABLE_HOST_PROCESS_TOOLS")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE"));

    if let Ok(image) = std::env::var("AGENT_RUNTIME_DOCKER_IMAGE") {
        if !image.trim().is_empty() {
            config.sandbox = SandboxConfig::docker(image);
        }
    }

    config
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C signal handler");
}
