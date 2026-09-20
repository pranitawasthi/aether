use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::agent::AgentId;

const DEFAULT_INBOX_CAPACITY: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub Uuid);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMessage {
    pub id: MessageId,
    pub from_agent_id: AgentId,
    pub to_agent_id: AgentId,
    pub topic: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl AgentMessage {
    pub fn new(
        from_agent_id: AgentId,
        to_agent_id: AgentId,
        topic: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: MessageId::new(),
            from_agent_id,
            to_agent_id,
            topic: topic.into(),
            payload,
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct MessageRouter {
    inboxes: Arc<Mutex<HashMap<AgentId, VecDeque<AgentMessage>>>>,
    inbox_capacity: usize,
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageRouter {
    pub fn new() -> Self {
        Self::with_inbox_capacity(DEFAULT_INBOX_CAPACITY)
    }

    pub fn with_inbox_capacity(inbox_capacity: usize) -> Self {
        assert!(
            inbox_capacity > 0,
            "Message inbox capacity must be greater than zero"
        );
        Self {
            inboxes: Arc::new(Mutex::new(HashMap::new())),
            inbox_capacity,
        }
    }

    pub async fn deliver(&self, message: AgentMessage) {
        let mut inboxes = self.inboxes.lock().await;
        let inbox = inboxes.entry(message.to_agent_id).or_default();
        if inbox.len() == self.inbox_capacity {
            inbox.pop_front();
        }
        inbox.push_back(message);
    }

    pub async fn inbox(&self, agent_id: AgentId, limit: usize) -> Vec<AgentMessage> {
        let inboxes = self.inboxes.lock().await;
        let Some(inbox) = inboxes.get(&agent_id) else {
            return Vec::new();
        };
        let skip = inbox.len().saturating_sub(limit);
        inbox.iter().skip(skip).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn inboxes_are_isolated_and_bounded() {
        let router = MessageRouter::with_inbox_capacity(2);
        let sender = AgentId::new();
        let recipient = AgentId::new();
        let other = AgentId::new();

        for topic in ["first", "second", "third"] {
            router
                .deliver(AgentMessage::new(sender, recipient, topic, json!({})))
                .await;
        }

        let inbox = router.inbox(recipient, 10).await;
        assert_eq!(inbox.len(), 2);
        assert_eq!(inbox[0].topic, "second");
        assert!(router.inbox(other, 10).await.is_empty());
    }
}
