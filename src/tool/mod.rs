mod echo;
mod executor;
mod file_read;
mod http;
mod permissions;
pub(crate) mod process;
mod python;
mod registry;
mod shell;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

pub use executor::{ToolConfig, ToolExecutionEngine, ToolExecutor};
pub use permissions::{Capability, PermissionSet};
pub use registry::{ToolDescriptor, ToolRegistry};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: 0,
        }
    }
}

fn default_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolRequest {
    pub tool: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub retry_policy: RetryPolicy,
}

fn default_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub output: serde_json::Value,
    pub metadata: serde_json::Value,
    pub attempts: u32,
}

#[async_trait]
pub trait Tool: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    fn required_capability(&self) -> Option<Capability>;

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value>;
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use serde_json::json;

    use super::*;

    struct RetryingTool {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Tool for RetryingTool {
        fn name(&self) -> &'static str {
            "retrying"
        }

        fn required_capability(&self) -> Option<Capability> {
            None
        }

        async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(crate::error::RuntimeError::Execution(
                    "temporary failure".into(),
                ));
            }

            Ok(json!({ "ok": true }))
        }
    }

    struct ProtectedTool;

    #[async_trait]
    impl Tool for ProtectedTool {
        fn name(&self) -> &'static str {
            "protected"
        }

        fn required_capability(&self) -> Option<Capability> {
            Some(Capability::NetworkHttp)
        }

        async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value> {
            Ok(json!({ "ok": true }))
        }
    }

    struct SlowTool;

    #[async_trait]
    impl Tool for SlowTool {
        fn name(&self) -> &'static str {
            "slow"
        }

        fn required_capability(&self) -> Option<Capability> {
            None
        }

        async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(json!({ "ok": true }))
        }
    }

    #[tokio::test]
    async fn default_tools_are_registered_with_phase_two_names() {
        let executor = ToolExecutor::with_defaults(ToolConfig::default()).unwrap();
        let names = executor.registry().names();

        assert!(names.contains(&"echo".to_owned()));
        assert!(names.contains(&"file_read".to_owned()));
        assert!(names.contains(&"file.read".to_owned()));
        assert!(names.contains(&"http_request".to_owned()));
        assert!(names.contains(&"http.get".to_owned()));
        assert!(!names.contains(&"shell".to_owned()));
        assert!(!names.contains(&"python".to_owned()));

        let result = executor
            .execute(
                &PermissionSet::new(),
                ToolRequest {
                    tool: "echo".into(),
                    arguments: json!({ "message": "hello" }),
                    timeout_ms: 10,
                    retry_policy: RetryPolicy::default(),
                },
            )
            .await
            .unwrap();

        assert_eq!(result.output, json!({ "message": "hello" }));
        assert_eq!(result.attempts, 1);
    }

    #[tokio::test]
    async fn execution_retries_transient_failures() {
        let registry = ToolRegistry::new();
        registry
            .register(std::sync::Arc::new(RetryingTool {
                calls: AtomicUsize::new(0),
            }))
            .unwrap();
        let executor = ToolExecutor::new(registry);
        let result = executor
            .execute(
                &PermissionSet::new(),
                ToolRequest {
                    tool: "retrying".into(),
                    arguments: serde_json::Value::Null,
                    timeout_ms: 10,
                    retry_policy: RetryPolicy {
                        max_attempts: 2,
                        backoff_ms: 0,
                    },
                },
            )
            .await
            .unwrap();

        assert_eq!(result.attempts, 2);
    }

    #[tokio::test]
    async fn execution_rejects_missing_capabilities() {
        let registry = ToolRegistry::new();
        registry
            .register(std::sync::Arc::new(ProtectedTool))
            .unwrap();
        let executor = ToolExecutor::new(registry);
        let error = executor
            .execute(
                &PermissionSet::new(),
                ToolRequest {
                    tool: "protected".into(),
                    arguments: serde_json::Value::Null,
                    timeout_ms: 10,
                    retry_policy: RetryPolicy::default(),
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::error::RuntimeError::PermissionDenied(_)
        ));
    }

    #[tokio::test]
    async fn execution_times_out_slow_tools() {
        let registry = ToolRegistry::new();
        registry.register(std::sync::Arc::new(SlowTool)).unwrap();
        let executor = ToolExecutor::new(registry);
        let error = executor
            .execute(
                &PermissionSet::new(),
                ToolRequest {
                    tool: "slow".into(),
                    arguments: serde_json::Value::Null,
                    timeout_ms: 5,
                    retry_policy: RetryPolicy::default(),
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, crate::error::RuntimeError::ToolTimeout(_)));
    }

    #[test]
    fn capabilities_accept_dotted_permission_names() {
        let permissions: PermissionSet =
            serde_json::from_value(json!(["filesystem.read", "network.http", "shell.execute"]))
                .unwrap();

        assert!(permissions.contains(&Capability::FilesystemRead));
        assert!(permissions.contains(&Capability::NetworkHttp));
        assert!(permissions.contains(&Capability::ShellExecute));
    }
}
