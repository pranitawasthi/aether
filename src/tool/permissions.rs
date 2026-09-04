use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Capability {
    #[serde(rename = "filesystem_read", alias = "filesystem.read")]
    FilesystemRead,
    #[serde(rename = "filesystem_write", alias = "filesystem.write")]
    FilesystemWrite,
    #[serde(rename = "network_http", alias = "network.http")]
    NetworkHttp,
    #[serde(rename = "database_read", alias = "database.read")]
    DatabaseRead,
    #[serde(rename = "database_write", alias = "database.write")]
    DatabaseWrite,
    #[serde(rename = "shell_execute", alias = "shell.execute")]
    ShellExecute,
    #[serde(rename = "python_execute", alias = "python.execute")]
    PythonExecute,
}

pub type PermissionSet = BTreeSet<Capability>;
