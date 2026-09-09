use std::{collections::BTreeSet, path::PathBuf, time::Duration};

use crate::error::{Result, RuntimeError};
use crate::sandbox::{SandboxConfig, SandboxManager};

use super::{
    echo::EchoTool, file_read::FileReadTool, http::HttpRequestTool, python::PythonTool,
    shell::ShellTool, PermissionSet, ToolRegistry, ToolRequest, ToolResult,
};

#[derive(Debug, Clone)]
pub struct ToolConfig {
    pub file_root: PathBuf,
    pub max_file_bytes: u64,
    pub allowed_http_hosts: BTreeSet<String>,
    pub max_http_response_bytes: usize,
    pub allowed_shell_programs: BTreeSet<String>,
    pub python_program: String,
    pub max_process_output_bytes: usize,
    pub max_timeout_ms: u64,
    pub enable_host_process_tools: bool,
    pub sandbox: SandboxConfig,
}

impl Default for ToolConfig {
    fn default() -> Self {
        Self {
            file_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            max_file_bytes: 1_048_576,
            allowed_http_hosts: BTreeSet::new(),
            max_http_response_bytes: 1_048_576,
            allowed_shell_programs: BTreeSet::new(),
            python_program: "python3".to_owned(),
            max_process_output_bytes: 1_048_576,
            max_timeout_ms: 60_000,
            enable_host_process_tools: false,
            sandbox: SandboxConfig::default(),
        }
    }
}

#[derive(Clone)]
pub struct ToolExecutor {
    registry: ToolRegistry,
    max_timeout_ms: u64,
}

pub type ToolExecutionEngine = ToolExecutor;

impl ToolExecutor {
    pub fn with_defaults(config: ToolConfig) -> Result<Self> {
        let registry = ToolRegistry::new();
        registry.register(std::sync::Arc::new(EchoTool))?;
        registry.register(std::sync::Arc::new(FileReadTool::new(
            config.file_root.clone(),
            config.max_file_bytes,
        )))?;
        registry.register(std::sync::Arc::new(HttpRequestTool::new(
            config.allowed_http_hosts,
            config.max_http_response_bytes,
        )?))?;
        if config.enable_host_process_tools {
            registry.register(std::sync::Arc::new(ShellTool::new(
                config.file_root.clone(),
                config.allowed_shell_programs,
                config.max_process_output_bytes,
            )))?;
            registry.register(std::sync::Arc::new(PythonTool::new(
                config.file_root.clone(),
                config.python_program,
                config.max_process_output_bytes,
            )))?;
        }
        if let Some(tool) = SandboxManager::new(config.sandbox.clone())
            .docker_tool(config.file_root.clone(), config.max_process_output_bytes)
        {
            registry.register(std::sync::Arc::new(tool))?;
        }

        Ok(Self {
            registry,
            max_timeout_ms: config.max_timeout_ms.max(1),
        })
    }

    pub fn new(registry: ToolRegistry) -> Self {
        Self {
            registry,
            max_timeout_ms: ToolConfig::default().max_timeout_ms,
        }
    }

    pub fn registry(&self) -> &ToolRegistry {
        &self.registry
    }

    pub async fn execute(
        &self,
        permissions: &PermissionSet,
        request: ToolRequest,
    ) -> Result<ToolResult> {
        let tool = self.registry.get(&request.tool)?;

        if let Some(capability) = tool.required_capability() {
            if !permissions.contains(&capability) {
                return Err(RuntimeError::PermissionDenied(format!(
                    "{} requires {capability:?}",
                    request.tool
                )));
            }
        }

        let max_attempts = request.retry_policy.max_attempts.max(1);
        let timeout = Duration::from_millis(request.timeout_ms.min(self.max_timeout_ms).max(1));
        let mut last_error = None;

        for attempt in 1..=max_attempts {
            let execution =
                tokio::time::timeout(timeout, tool.execute(request.arguments.clone())).await;

            match execution {
                Ok(Ok(output)) => {
                    return Ok(ToolResult {
                        output,
                        metadata: serde_json::json!({ "tool": request.tool }),
                        attempts: attempt,
                    });
                }
                Ok(Err(error)) => last_error = Some(error),
                Err(_) => last_error = Some(RuntimeError::ToolTimeout(request.tool.clone())),
            }

            if attempt < max_attempts && request.retry_policy.backoff_ms > 0 {
                tokio::time::sleep(Duration::from_millis(request.retry_policy.backoff_ms)).await;
            }
        }

        Err(last_error.expect("a tool execution attempt always produces a result"))
    }
}
