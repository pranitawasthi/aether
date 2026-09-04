use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::error::{Result, RuntimeError};

use super::{Capability, Tool};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub required_capability: Option<Capability>,
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: Arc<RwLock<HashMap<String, Arc<dyn Tool>>>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, tool: Arc<dyn Tool>) -> Result<()> {
        let names = std::iter::once(tool.name())
            .chain(tool.aliases().iter().copied())
            .collect::<Vec<_>>();
        let mut tools = self.tools.write().expect("tool registry lock poisoned");

        for name in &names {
            if tools.contains_key(*name) {
                return Err(RuntimeError::ToolAlreadyRegistered((*name).to_owned()));
            }
        }

        for name in names {
            tools.insert(name.to_owned(), tool.clone());
        }

        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn Tool>> {
        self.tools
            .read()
            .expect("tool registry lock poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| RuntimeError::ToolNotFound(name.to_owned()))
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self
            .tools
            .read()
            .expect("tool registry lock poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn descriptors(&self) -> Vec<ToolDescriptor> {
        let tools = self.tools.read().expect("tool registry lock poisoned");
        let mut descriptors = tools
            .iter()
            .map(|(name, tool)| ToolDescriptor {
                name: name.clone(),
                required_capability: tool.required_capability(),
            })
            .collect::<Vec<_>>();
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        descriptors
    }
}
