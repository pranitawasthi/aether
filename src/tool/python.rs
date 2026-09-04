use std::path::PathBuf;

use async_trait::async_trait;
use serde::Deserialize;

use crate::error::{Result, RuntimeError};

use super::{file_read::resolve_under_root, process::run_confined_process, Capability, Tool};

#[derive(Debug, Deserialize)]
struct PythonArguments {
    path: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    stdin: Option<String>,
}

pub struct PythonTool {
    root: PathBuf,
    python_program: String,
    max_output_bytes: usize,
}

impl PythonTool {
    pub fn new(
        root: impl Into<PathBuf>,
        python_program: impl Into<String>,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            root: root.into(),
            python_program: python_program.into(),
            max_output_bytes,
        }
    }

    fn error(message: impl Into<String>) -> RuntimeError {
        RuntimeError::Execution(format!("python: {}", message.into()))
    }
}

#[async_trait]
impl Tool for PythonTool {
    fn name(&self) -> &'static str {
        "python"
    }

    fn required_capability(&self) -> Option<Capability> {
        Some(Capability::PythonExecute)
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value> {
        let arguments: PythonArguments = serde_json::from_value(arguments)
            .map_err(|error| Self::error(format!("invalid arguments: {error}")))?;
        let script = resolve_under_root(&self.root, &arguments.path)
            .map_err(|error| Self::error(error.to_string()))?;

        if !script.is_file() {
            return Err(Self::error("path must identify a Python file"));
        }

        let cwd = self
            .root
            .canonicalize()
            .map_err(|error| Self::error(format!("invalid configured root: {error}")))?;
        let mut args = vec![script.to_string_lossy().into_owned()];
        args.extend(arguments.args);

        let output = run_confined_process(
            &self.python_program,
            &args,
            &cwd,
            arguments.stdin,
            self.max_output_bytes,
            "python",
        )
        .await?;

        if output.exit_code != 0 {
            return Err(Self::error(format!(
                "process exited with code {}",
                output.exit_code
            )));
        }

        Ok(serde_json::json!({
            "path": arguments.path,
            "exit_code": output.exit_code,
            "stdout": output.stdout,
            "stderr": output.stderr,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn python_is_confined_to_its_configured_root() {
        let parent = std::env::temp_dir().join(format!("agent-runtime-{}", uuid::Uuid::new_v4()));
        let root = parent.join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(parent.join("outside.py"), "print('nope')").unwrap();

        let tool = PythonTool::new(&root, "python3", 1_024);
        let error = tool
            .execute(json!({ "path": "../outside.py" }))
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::Execution(_)));
        fs::remove_dir_all(parent).unwrap();
    }

    #[tokio::test]
    async fn python_runs_a_script_under_the_tool_root() {
        if std::process::Command::new("python3")
            .arg("-V")
            .output()
            .is_err()
        {
            return;
        }

        let parent =
            std::env::temp_dir().join(format!("agent-runtime-py-{}", uuid::Uuid::new_v4()));
        let root = parent.join("root");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("hello.py"), "print('ok')").unwrap();

        let tool = PythonTool::new(&root, "python3", 1_024);
        let output = tool.execute(json!({ "path": "hello.py" })).await.unwrap();

        assert!(output["stdout"].as_str().unwrap().contains("ok"));
        fs::remove_dir_all(parent).unwrap();
    }
}
