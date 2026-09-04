use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::{Result, RuntimeError},
    tool::PermissionSet,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub Uuid);

impl AgentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AgentId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    Created,
    Initializing,
    Ready,
    Running,
    Waiting,
    Completed,
    Failed,
    Terminated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub name: String,
    pub status: AgentStatus,
    #[serde(default)]
    pub permissions: PermissionSet,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Agent {
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();

        Self {
            id: AgentId::new(),
            name: name.into(),
            status: AgentStatus::Created,
            permissions: PermissionSet::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_permissions(mut self, permissions: PermissionSet) -> Self {
        self.permissions = permissions;
        self
    }

    pub fn initialize(&mut self) -> Result<()> {
        self.transition(AgentStatus::Initializing)
    }

    pub fn ready(&mut self) -> Result<()> {
        self.transition(AgentStatus::Ready)
    }

    pub fn start(&mut self) -> Result<()> {
        self.transition(AgentStatus::Running)
    }

    pub fn wait(&mut self) -> Result<()> {
        self.transition(AgentStatus::Waiting)
    }

    pub fn complete(&mut self) -> Result<()> {
        self.transition(AgentStatus::Completed)
    }

    pub fn fail(&mut self) -> Result<()> {
        self.transition(AgentStatus::Failed)
    }

    pub fn terminate(&mut self) -> Result<()> {
        self.transition(AgentStatus::Terminated)
    }

    pub fn can_accept_tasks(&self) -> bool {
        matches!(self.status, AgentStatus::Ready | AgentStatus::Running)
    }

    fn transition(&mut self, next: AgentStatus) -> Result<()> {
        if !matches!(
            (self.status, next),
            (
                AgentStatus::Created,
                AgentStatus::Initializing | AgentStatus::Terminated
            ) | (
                AgentStatus::Initializing,
                AgentStatus::Ready | AgentStatus::Failed | AgentStatus::Terminated
            ) | (
                AgentStatus::Ready,
                AgentStatus::Running | AgentStatus::Terminated
            ) | (
                AgentStatus::Running,
                AgentStatus::Waiting
                    | AgentStatus::Completed
                    | AgentStatus::Failed
                    | AgentStatus::Terminated
            ) | (
                AgentStatus::Waiting,
                AgentStatus::Running | AgentStatus::Terminated
            )
        ) {
            return Err(RuntimeError::InvalidStateTransition(format!(
                "Agent {:?} -> {:?}",
                self.status, next
            )));
        }

        self.status = next;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_can_progress_through_a_normal_lifecycle() {
        let mut agent = Agent::new("research-agent");

        agent.initialize().unwrap();
        agent.ready().unwrap();
        agent.start().unwrap();
        agent.wait().unwrap();
        agent.start().unwrap();
        agent.complete().unwrap();

        assert_eq!(agent.status, AgentStatus::Completed);
    }

    #[test]
    fn agent_rejects_invalid_transition() {
        let mut agent = Agent::new("research-agent");

        assert!(agent.start().is_err());
    }
}
