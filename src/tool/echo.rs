use async_trait::async_trait;

use crate::error::Result;

use super::{Capability, Tool};

pub struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &'static str {
        "echo"
    }

    fn required_capability(&self) -> Option<Capability> {
        None
    }

    async fn execute(&self, arguments: serde_json::Value) -> Result<serde_json::Value> {
        Ok(arguments)
    }
}
