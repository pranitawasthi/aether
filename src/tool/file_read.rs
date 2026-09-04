use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::Deserialize;

use crate::error::{Result, RuntimeError};

use super::{Capability, Tool};

#[derive(Debug, Deserialize)]
struct FileReadArguments {
    path: String,
}

pub struct FileReadTool {
    root: PathBuf,
    max_bytes: u64,
}

impl FileReadTool {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root: root.into(),
            max_bytes,
        }
    }

    fn error(message: impl Into<String>) -> RuntimeError {
        RuntimeError::Execution(format!("file_read: {}", message.into()))
    }
}

#[async_trait]
impl Tool for FileReadTool {
    fn name(&self) -> &'static str {
        "file_read"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["file.read"]
    }

    fn required_capability(&self) -> Option<Capability> {
        Some(Capability::FilesystemRead)
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value> {
        let arguments: FileReadArguments = serde_json::from_value(arguments)
            .map_err(|error| Self::error(format!("invalid arguments: {error}")))?;
        let path = resolve_under_root(&self.root, &arguments.path)?;

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| Self::error(format!("unable to inspect file: {error}")))?;

        if !metadata.is_file() {
            return Err(Self::error("path must identify a regular file"));
        }

        if metadata.len() > self.max_bytes {
            return Err(Self::error(format!(
                "file exceeds the {max} byte limit",
                max = self.max_bytes
            )));
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|error| Self::error(format!("unable to read UTF-8 text file: {error}")))?;

        Ok(serde_json::json!({
            "path": arguments.path,
            "content": content,
        }))
    }
}

pub fn resolve_under_root(root: &Path, requested: &str) -> Result<PathBuf> {
    let requested_path = Path::new(requested);

    if requested_path.is_absolute() {
        return Err(RuntimeError::Execution(
            "path must be relative to the configured tool root".into(),
        ));
    }

    let root = root
        .canonicalize()
        .map_err(|error| RuntimeError::Execution(format!("invalid configured root: {error}")))?;
    let path = root.join(requested_path).canonicalize().map_err(|error| {
        RuntimeError::Execution(format!(
            "unable to resolve requested path '{requested}': {error}"
        ))
    })?;

    if !path.starts_with(&root) {
        return Err(RuntimeError::Execution(
            "path escapes the configured tool root".into(),
        ));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn file_read_is_confined_to_its_configured_root() {
        let parent = std::env::temp_dir().join(format!("agent-runtime-{}", uuid::Uuid::new_v4()));
        let root = parent.join("root");
        let outside = parent.join("outside.txt");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("inside.txt"), "safe content").unwrap();
        fs::write(&outside, "secret content").unwrap();

        let tool = FileReadTool::new(&root, 1_024);
        let output = tool.execute(json!({ "path": "inside.txt" })).await.unwrap();
        let error = tool
            .execute(json!({ "path": "../outside.txt" }))
            .await
            .unwrap_err();

        assert_eq!(output["content"], "safe content");
        assert!(matches!(error, RuntimeError::Execution(_)));

        fs::remove_dir_all(parent).unwrap();
    }
}
