use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{agent::AgentId, memory::MemoryRecord, task::Task};

const DEFAULT_EVENT_CAPACITY: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventKind {
    AgentCreated,
    TaskQueued,
    TaskStarted,
    TaskCompleted,
    TaskFailed,
    TaskCancelled,
    MemoryUpdated,
    MessageSent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub id: u64,
    pub occurred_at: DateTime<Utc>,
    pub kind: RuntimeEventKind,
    pub agent_id: Option<AgentId>,
    pub task_id: Option<crate::task::TaskId>,
    pub data: serde_json::Value,
}

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<RuntimeEvent>,
    history: Arc<Mutex<VecDeque<RuntimeEvent>>>,
    next_id: Arc<AtomicU64>,
    history_capacity: usize,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_EVENT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "Event capacity must be greater than zero");
        let (sender, _) = broadcast::channel(capacity);

        Self {
            sender,
            history: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            next_id: Arc::new(AtomicU64::new(1)),
            history_capacity: capacity,
        }
    }

    pub fn publish(
        &self,
        kind: RuntimeEventKind,
        agent_id: Option<AgentId>,
        task_id: Option<crate::task::TaskId>,
        data: serde_json::Value,
    ) -> RuntimeEvent {
        let event = RuntimeEvent {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            occurred_at: Utc::now(),
            kind,
            agent_id,
            task_id,
            data,
        };

        let mut history = self.history.lock().expect("event history lock poisoned");
        if history.len() == self.history_capacity {
            history.pop_front();
        }
        history.push_back(event.clone());
        drop(history);

        // A bus with no subscribers is still useful because recent events are retained.
        let _ = self.sender.send(event.clone());
        event
    }

    pub fn publish_task(&self, kind: RuntimeEventKind, task: &Task) -> RuntimeEvent {
        self.publish(
            kind,
            task.agent_id,
            Some(task.id),
            serde_json::json!({ "task": task }),
        )
    }

    pub fn publish_memory(&self, record: &MemoryRecord) -> RuntimeEvent {
        self.publish(
            RuntimeEventKind::MemoryUpdated,
            Some(record.agent_id),
            None,
            serde_json::json!({ "memory": record }),
        )
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.sender.subscribe()
    }

    pub fn recent(&self, limit: usize) -> Vec<RuntimeEvent> {
        let history = self.history.lock().expect("event history lock poisoned");
        let skip = history.len().saturating_sub(limit);
        history.iter().skip(skip).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn publishes_to_subscribers_and_retains_a_bounded_history() {
        let bus = EventBus::with_capacity(2);
        let mut receiver = bus.subscribe();

        let first = bus.publish(
            RuntimeEventKind::AgentCreated,
            None,
            None,
            json!({ "name": "a" }),
        );
        assert_eq!(receiver.recv().await.unwrap().id, first.id);
        bus.publish(
            RuntimeEventKind::AgentCreated,
            None,
            None,
            json!({ "name": "b" }),
        );
        let third = bus.publish(
            RuntimeEventKind::AgentCreated,
            None,
            None,
            json!({ "name": "c" }),
        );

        assert_eq!(bus.recent(10).len(), 2);
        assert_eq!(bus.recent(10)[0].id, 2);
        assert_eq!(bus.recent(10)[1].id, third.id);
    }
}
