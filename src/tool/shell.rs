use std::{collections::BTreeSet, path::PathBuf};

use async_trait::async_trait;
use serde::Deserialize;

use crate::error::{Result, RuntimeError};

use super::{process::run_confined_process, Capability, Tool};

const DENIED_SHELL_PROGRAMS: &[&str] = &[
    "bash",
    "csh",
    "dash",
    "fish",
    "ksh",
    "sh",
    "tcsh",
    "zsh",
    "pwsh",
    "powershell",
    "cmd",
];

#[derive(Debug, Deserialize)]
struct ShellArguments {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    stdin: Option<String>,
}

pub struct ShellTool {
    root: PathBuf,
    allowed_programs: BTreeSet<String>,
    max_output_bytes: usize,
}

impl ShellTool {
    pub fn new(
        root: impl Into<PathBuf>,
        allowed_programs: BTreeSet<String>,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            root: root.into(),
            allowed_programs: allowed_programs
                .into_iter()
                .map(|program| program.to_ascii_lowercase())
                .collect(),
            max_output_bytes,
        }
    }

    fn error(message: impl Into<String>) -> RuntimeError {
        RuntimeError::Execution(format!("shell: {}", message.into()))
    }
}

#[async_trait]
impl Tool for ShellTool {
    fn name(&self) -> &'static str {
        "shell"
    }

    fn required_capability(&self) -> Option<Capability> {
        Some(Capability::ShellExecute)
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value> {
        let arguments: ShellArguments = serde_json::from_value(arguments)
            .map_err(|error| Self::error(format!("invalid arguments: {error}")))?;
        let program = arguments.program.trim();

        if program.is_empty() {
            return Err(Self::error("program must not be empty"));
        }

        if program.contains('/') || program.contains('\\') || program.contains("..") {
            return Err(Self::error(
                "program must be an allowlisted basename, not a path",
            ));
        }

        let normalized = program.to_ascii_lowercase();
        if DENIED_SHELL_PROGRAMS.contains(&normalized.as_str()) {
            return Err(Self::error(
                "interactive shells are not available until sandbox isolation exists",
            ));
        }

        if !self.allowed_programs.contains(&normalized) {
            return Err(RuntimeError::PermissionDenied(format!(
                "shell program '{program}' is not in the runtime allowlist"
            )));
        }

        let cwd = self
            .root
            .canonicalize()
            .map_err(|error| Self::error(format!("invalid configured root: {error}")))?;
        let output = run_confined_process(
            program,
            &arguments.args,
            &cwd,
            arguments.stdin,
            self.max_output_bytes,
            "shell",
        )
        .await?;

        if output.exit_code != 0 {
            return Err(Self::error(format!(
                "process exited with code {}",
                output.exit_code
            )));
        }

        Ok(serde_json::json!({
            "program": program,
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
    async fn shell_rejects_programs_outside_the_allowlist() {
        let tool = ShellTool::new(".", BTreeSet::new(), 1_024);
        let error = tool
            .execute(json!({ "program": "echo", "args": ["hello"] }))
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::PermissionDenied(_)));
    }

    #[tokio::test]
    async fn shell_rejects_interactive_shell_binaries() {
        let allowed = BTreeSet::from(["bash".to_owned()]);
        let tool = ShellTool::new(".", allowed, 1_024);
        let error = tool
            .execute(json!({ "program": "bash", "args": ["-c", "echo hi"] }))
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::Execution(_)));
    }

    #[tokio::test]
    async fn shell_runs_an_allowlisted_program() {
        let root =
            std::env::temp_dir().join(format!("agent-runtime-shell-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();

        let tool = ShellTool::new(&root, BTreeSet::from(["echo".to_owned()]), 1_024);
        let output = tool
            .execute(json!({ "program": "echo", "args": ["hello"] }))
            .await
            .unwrap();

        assert!(output["stdout"].as_str().unwrap().contains("hello"));
        fs::remove_dir_all(root).unwrap();
    }
}
