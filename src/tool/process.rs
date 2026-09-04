use std::{path::PathBuf, process::Stdio};

use crate::error::{Result, RuntimeError};

pub struct ProcessOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

pub async fn run_confined_process(
    program: &str,
    args: &[String],
    cwd: &PathBuf,
    stdin: Option<String>,
    max_output_bytes: usize,
    label: &str,
) -> Result<ProcessOutput> {
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .current_dir(cwd)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().map_err(|error| {
        RuntimeError::Execution(format!("{label}: failed to spawn '{program}': {error}"))
    })?;

    if let Some(stdin) = stdin {
        if let Some(mut handle) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            handle.write_all(stdin.as_bytes()).await.map_err(|error| {
                RuntimeError::Execution(format!("{label}: failed to write stdin: {error}"))
            })?;
            handle.shutdown().await.map_err(|error| {
                RuntimeError::Execution(format!("{label}: failed to close stdin: {error}"))
            })?;
        }
    } else {
        drop(child.stdin.take());
    }

    let output = child.wait_with_output().await.map_err(|error| {
        RuntimeError::Execution(format!(
            "{label}: failed to collect process output: {error}"
        ))
    })?;

    if output.stdout.len() > max_output_bytes || output.stderr.len() > max_output_bytes {
        return Err(RuntimeError::Execution(format!(
            "{label}: process output exceeds the {max_output_bytes} byte limit"
        )));
    }

    Ok(ProcessOutput {
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}
