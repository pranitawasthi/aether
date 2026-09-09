use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    error::{Result, RuntimeError},
    tool::{process::run_confined_process, Capability, Tool},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackend {
    Disabled,
    Docker,
}

#[derive(Debug, Clone)]
pub struct DockerSandboxConfig {
    pub image: String,
    pub cpu_limit: String,
    pub memory_limit_mib: u64,
    pub pids_limit: u64,
}

impl DockerSandboxConfig {
    pub fn new(image: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            cpu_limit: "1.0".to_owned(),
            memory_limit_mib: 512,
            pids_limit: 64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SandboxConfig {
    pub backend: SandboxBackend,
    pub docker: Option<DockerSandboxConfig>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            backend: SandboxBackend::Disabled,
            docker: None,
        }
    }
}

impl SandboxConfig {
    pub fn docker(image: impl Into<String>) -> Self {
        Self {
            backend: SandboxBackend::Docker,
            docker: Some(DockerSandboxConfig::new(image)),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SandboxStatus {
    pub backend: SandboxBackend,
    pub execution_enabled: bool,
    pub network: String,
    pub image: Option<String>,
}

#[derive(Clone)]
pub struct SandboxManager {
    config: SandboxConfig,
}

impl SandboxManager {
    pub fn new(config: SandboxConfig) -> Self {
        Self { config }
    }

    pub fn status(&self) -> SandboxStatus {
        let image = self
            .config
            .docker
            .as_ref()
            .map(|docker| docker.image.clone());

        SandboxStatus {
            backend: self.config.backend.clone(),
            execution_enabled: self.config.backend == SandboxBackend::Docker && image.is_some(),
            network: "none".to_owned(),
            image,
        }
    }

    pub fn docker_tool(
        &self,
        workspace_root: PathBuf,
        max_output_bytes: usize,
    ) -> Option<DockerSandboxTool> {
        let docker = self.config.docker.clone()?;

        if self.config.backend != SandboxBackend::Docker {
            return None;
        }

        Some(DockerSandboxTool {
            config: docker,
            workspace_root,
            max_output_bytes,
        })
    }
}

#[derive(Debug, Deserialize)]
struct SandboxExecArguments {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    stdin: Option<String>,
}

pub struct DockerSandboxTool {
    config: DockerSandboxConfig,
    workspace_root: PathBuf,
    max_output_bytes: usize,
}

impl DockerSandboxTool {
    fn error(message: impl Into<String>) -> RuntimeError {
        RuntimeError::Execution(format!("sandbox.exec: {}", message.into()))
    }

    fn docker_arguments(&self, program: &str, args: &[String]) -> Result<Vec<String>> {
        if program.trim().is_empty() || program.contains('/') || program.contains('\\') {
            return Err(Self::error("program must be a non-empty basename"));
        }

        let workspace = self
            .workspace_root
            .canonicalize()
            .map_err(|error| Self::error(format!("invalid configured workspace root: {error}")))?;
        let mut docker_args = vec![
            "run".to_owned(),
            "--rm".to_owned(),
            "--init".to_owned(),
            "--read-only".to_owned(),
            "--cap-drop=ALL".to_owned(),
            "--security-opt=no-new-privileges".to_owned(),
            "--network=none".to_owned(),
            "--user=65534:65534".to_owned(),
            "--tmpfs=/tmp:rw,noexec,nosuid,size=64m".to_owned(),
            format!("--cpus={}", self.config.cpu_limit),
            format!("--memory={}m", self.config.memory_limit_mib),
            format!("--pids-limit={}", self.config.pids_limit),
            "--volume".to_owned(),
            format!("{}:/workspace:ro", workspace.display()),
            "--workdir".to_owned(),
            "/workspace".to_owned(),
            self.config.image.clone(),
            program.to_owned(),
        ];
        docker_args.extend(args.iter().cloned());

        Ok(docker_args)
    }
}

#[async_trait]
impl Tool for DockerSandboxTool {
    fn name(&self) -> &'static str {
        "sandbox.exec"
    }

    fn required_capability(&self) -> Option<Capability> {
        Some(Capability::ShellExecute)
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value> {
        let arguments: SandboxExecArguments = serde_json::from_value(arguments)
            .map_err(|error| Self::error(format!("invalid arguments: {error}")))?;
        let docker_args = self.docker_arguments(&arguments.program, &arguments.args)?;
        let workspace = self
            .workspace_root
            .canonicalize()
            .map_err(|error| Self::error(format!("invalid configured workspace root: {error}")))?;
        let output = run_confined_process(
            "docker",
            &docker_args,
            &workspace,
            arguments.stdin,
            self.max_output_bytes,
            "sandbox.exec",
        )
        .await?;

        if output.exit_code != 0 {
            return Err(Self::error(format!(
                "container exited with code {}: {}",
                output.exit_code,
                output.stderr.trim()
            )));
        }

        Ok(serde_json::json!({
            "program": arguments.program,
            "exit_code": output.exit_code,
            "stdout": output.stdout,
            "stderr": output.stderr,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_sandbox_fails_closed() {
        let manager = SandboxManager::new(SandboxConfig::default());

        assert!(!manager.status().execution_enabled);
        assert!(manager.docker_tool(PathBuf::from("."), 1_024).is_none());
    }

    #[test]
    fn docker_command_has_mandatory_isolation_flags() {
        let root =
            std::env::temp_dir().join(format!("agent-runtime-sandbox-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let manager = SandboxManager::new(SandboxConfig::docker("alpine:3.21"));
        let tool = manager.docker_tool(root.clone(), 1_024).unwrap();
        let arguments = tool
            .docker_arguments("echo", &["hello".to_owned()])
            .unwrap();

        for flag in [
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--network=none",
            "--user=65534:65534",
            "--tmpfs=/tmp:rw,noexec,nosuid,size=64m",
            "--pids-limit=64",
        ] {
            assert!(arguments.contains(&flag.to_owned()));
        }
        assert!(arguments.contains(&format!(
            "{}:/workspace:ro",
            root.canonicalize().unwrap().display()
        )));

        std::fs::remove_dir_all(root).unwrap();
    }
}
