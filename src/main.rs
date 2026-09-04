use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use agent_runtime::{api, runtime::AgentRuntime, tool::ToolConfig, worker::MockTaskExecutor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let executor = Arc::new(MockTaskExecutor::new(Duration::from_secs(1)));
    let runtime = Arc::new(AgentRuntime::new_with_tool_config(
        executor,
        3,
        tool_config_from_env(),
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

    config
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C signal handler");
}
