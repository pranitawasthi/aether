use std::{sync::Arc, time::Duration as StdDuration};

use chrono::Duration;
use tokio::sync::watch;

use crate::{
    error::{Result, RuntimeError},
    storage::{PostgresStorage, TaskLease},
    task::{Task, TaskKind, TaskResult},
    tool::{ToolExecutor, ToolRequest},
    worker::TaskExecutor,
};

#[derive(Debug, Clone)]
pub struct DistributedWorkerConfig {
    pub worker_id: String,
    pub lease_duration: Duration,
    pub idle_poll_interval: StdDuration,
}

impl DistributedWorkerConfig {
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            lease_duration: Duration::seconds(30),
            idle_poll_interval: StdDuration::from_millis(250),
        }
    }
}

pub struct DistributedWorker {
    storage: PostgresStorage,
    executor: Arc<dyn TaskExecutor>,
    config: DistributedWorkerConfig,
}

pub struct DurableTaskExecutor {
    generic_executor: Arc<dyn TaskExecutor>,
    storage: PostgresStorage,
    tools: ToolExecutor,
}

impl DurableTaskExecutor {
    pub fn new(
        generic_executor: Arc<dyn TaskExecutor>,
        storage: PostgresStorage,
        tools: ToolExecutor,
    ) -> Self {
        Self {
            generic_executor,
            storage,
            tools,
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for DurableTaskExecutor {
    async fn execute(&self, task: &Task) -> Result<TaskResult> {
        if task.kind == TaskKind::Generic {
            return self.generic_executor.execute(task).await;
        }

        let agent_id = task.agent_id.ok_or_else(|| {
            RuntimeError::Execution("Tool tasks must be owned by an agent".to_owned())
        })?;
        let agent = self
            .storage
            .get_agent(agent_id)
            .await?
            .ok_or_else(|| RuntimeError::AgentNotFound(agent_id.0.to_string()))?;
        if !agent.can_accept_tasks() {
            return Err(RuntimeError::AgentNotReady(agent_id.0.to_string()));
        }
        let request: ToolRequest =
            serde_json::from_value(task.payload.clone()).map_err(|error| {
                RuntimeError::Execution(format!("Invalid tool task payload: {error}"))
            })?;
        let result = self.tools.execute(&agent.permissions, request).await?;

        Ok(TaskResult {
            output: result.output,
            metadata: serde_json::json!({
                "tool": result.metadata,
                "attempts": result.attempts,
            }),
        })
    }
}

impl DistributedWorker {
    pub fn new(
        storage: PostgresStorage,
        executor: Arc<dyn TaskExecutor>,
        config: DistributedWorkerConfig,
    ) -> Result<Self> {
        if config.worker_id.trim().is_empty() {
            return Err(RuntimeError::Execution(
                "Worker id must not be empty".to_owned(),
            ));
        }
        if config.lease_duration <= Duration::zero() {
            return Err(RuntimeError::Execution(
                "Lease duration must be positive".to_owned(),
            ));
        }

        Ok(Self {
            storage,
            executor,
            config,
        })
    }

    /// Claims and executes at most one durable task. Returns `true` when work was claimed.
    pub async fn process_one(&self) -> Result<bool> {
        let Some(claimed) = self
            .storage
            .claim_next(&self.config.worker_id, self.config.lease_duration)
            .await?
        else {
            return Ok(false);
        };

        let mut task = claimed.task;
        let mut lease = claimed.lease;
        task.start()?;
        self.storage.mark_running(&lease, &task).await?;

        let result = self.execute_with_lease_renewal(&task, &mut lease).await;
        match result {
            Ok(result) => task.complete(result)?,
            Err(error) => task.fail(error.to_string())?,
        }
        self.storage.acknowledge(&lease, &task).await?;

        Ok(true)
    }

    pub async fn run_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        self.storage.register_worker(&self.config.worker_id).await?;
        let heartbeat_interval = self.heartbeat_interval()?;

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            if self.process_one().await? {
                self.storage.register_worker(&self.config.worker_id).await?;
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep(heartbeat_interval) => {
                    self.storage.register_worker(&self.config.worker_id).await?;
                }
                _ = tokio::time::sleep(self.config.idle_poll_interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn heartbeat_interval(&self) -> Result<StdDuration> {
        let lease =
            self.config.lease_duration.to_std().map_err(|error| {
                RuntimeError::Execution(format!("Invalid lease duration: {error}"))
            })?;
        Ok(lease.checked_div(3).unwrap_or(StdDuration::from_millis(1)))
    }

    async fn execute_with_lease_renewal(
        &self,
        task: &Task,
        lease: &mut TaskLease,
    ) -> Result<TaskResult> {
        let lease_std =
            self.config.lease_duration.to_std().map_err(|error| {
                RuntimeError::Execution(format!("Invalid lease duration: {error}"))
            })?;
        let renewal_interval = lease_std
            .checked_div(2)
            .unwrap_or(StdDuration::from_millis(1));
        let execution = self.executor.execute(task);
        tokio::pin!(execution);

        loop {
            tokio::select! {
                result = &mut execution => return result,
                _ = tokio::time::sleep(renewal_interval) => {
                    self.storage.renew_lease(lease, self.config.lease_duration).await?;
                }
            }
        }
    }
}
