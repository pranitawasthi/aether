use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agent::AgentId;
use crate::error::{Result, RuntimeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Created,
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    Low,

    #[default]
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    #[default]
    Generic,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskResult {
    pub output: serde_json::Value,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,

    pub agent_id: Option<AgentId>,

    #[serde(default)]
    pub kind: TaskKind,

    pub name: String,

    pub payload: serde_json::Value,

    pub status: TaskStatus,

    pub priority: TaskPriority,

    pub result: Option<TaskResult>,

    pub error: Option<String>,

    pub created_at: DateTime<Utc>,

    pub updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(
        name: impl Into<String>,
        payload: serde_json::Value,
        priority: TaskPriority,
    ) -> Self {
        let now = Utc::now();

        Self {
            id: TaskId::new(),
            agent_id: None,
            kind: TaskKind::Generic,
            name: name.into(),
            payload,
            status: TaskStatus::Created,
            priority,
            result: None,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn for_agent(mut self, agent_id: AgentId) -> Self {
        self.agent_id = Some(agent_id);
        self
    }

    pub fn tool(name: impl Into<String>, request: serde_json::Value) -> Self {
        let mut task = Self::new(name, request, TaskPriority::Normal);
        task.kind = TaskKind::Tool;
        task
    }

    pub fn queue(&mut self) -> Result<()> {
        self.transition(TaskStatus::Queued)
    }

    pub fn start(&mut self) -> Result<()> {
        self.transition(TaskStatus::Running)
    }

    pub fn complete(&mut self, result: TaskResult) -> Result<()> {
        self.transition(TaskStatus::Completed)?;
        self.result = Some(result);
        Ok(())
    }

    pub fn fail(&mut self, error: impl Into<String>) -> Result<()> {
        self.transition(TaskStatus::Failed)?;
        self.error = Some(error.into());
        Ok(())
    }

    pub fn cancel(&mut self) -> Result<()> {
        self.transition(TaskStatus::Cancelled)
    }

    fn transition(&mut self, next: TaskStatus) -> Result<()> {
        if !self.is_valid_transition(next) {
            return Err(RuntimeError::InvalidStateTransition(format!(
                "{:?} -> {:?}",
                self.status, next
            )));
        }

        self.status = next;
        self.updated_at = Utc::now();

        Ok(())
    }

    fn is_valid_transition(&self, next: TaskStatus) -> bool {
        matches!(
            (self.status, next),
            (TaskStatus::Created, TaskStatus::Queued)
                | (TaskStatus::Created, TaskStatus::Cancelled)
                | (TaskStatus::Queued, TaskStatus::Running)
                | (TaskStatus::Queued, TaskStatus::Cancelled)
                | (TaskStatus::Running, TaskStatus::Completed)
                | (TaskStatus::Running, TaskStatus::Failed)
                | (TaskStatus::Running, TaskStatus::Cancelled)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn valid_task_lifecycle() {
        let mut task = Task::new("test", json!({}), TaskPriority::Normal);

        assert_eq!(task.status, TaskStatus::Created);

        task.queue().unwrap();
        assert_eq!(task.status, TaskStatus::Queued);

        task.start().unwrap();
        assert_eq!(task.status, TaskStatus::Running);

        task.complete(TaskResult {
            output: json!({"ok": true}),
            metadata: json!({}),
        })
        .unwrap();

        assert_eq!(task.status, TaskStatus::Completed);
    }

    #[test]
    fn invalid_transition_fails() {
        let mut task = Task::new("test", json!({}), TaskPriority::Normal);

        let result = task.start();

        assert!(result.is_err());
    }

    #[test]
    fn failed_task_lifecycle() {
        let mut task = Task::new("test", json!({}), TaskPriority::Normal);

        task.queue().unwrap();
        task.start().unwrap();
        task.fail("something went wrong").unwrap();

        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.error.as_deref(), Some("something went wrong"));
    }
}
