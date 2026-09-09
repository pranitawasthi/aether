use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    agent::{Agent, AgentId},
    error::{Result, RuntimeError},
    memory::{MemoryManager, MemoryRecord, MemoryScope, MemoryWrite, SemanticMatch},
    sandbox::{SandboxManager, SandboxStatus},
    scheduler::{Scheduler, SharedTask},
    task::{Task, TaskId, TaskKind, TaskResult},
    tool::{PermissionSet, ToolConfig, ToolDescriptor, ToolExecutor, ToolRequest},
    worker::{TaskExecutor, WorkerPool},
};

type AgentStore = Arc<Mutex<HashMap<AgentId, Arc<Mutex<Agent>>>>>;

struct RuntimeTaskExecutor {
    generic_executor: Arc<dyn TaskExecutor>,
    agents: AgentStore,
    tools: ToolExecutor,
}

#[async_trait]
impl TaskExecutor for RuntimeTaskExecutor {
    async fn execute(&self, task: &Task) -> Result<TaskResult> {
        if task.kind == TaskKind::Generic {
            return self.generic_executor.execute(task).await;
        }

        let agent_id = task.agent_id.ok_or_else(|| {
            RuntimeError::Execution("tool tasks must be owned by an agent".to_owned())
        })?;
        let agent = self
            .agents
            .lock()
            .await
            .get(&agent_id)
            .cloned()
            .ok_or_else(|| RuntimeError::AgentNotFound(agent_id.0.to_string()))?;
        let agent = agent.lock().await;

        if !agent.can_accept_tasks() {
            return Err(RuntimeError::AgentNotReady(agent_id.0.to_string()));
        }

        let permissions = agent.permissions.clone();
        drop(agent);
        let request: ToolRequest =
            serde_json::from_value(task.payload.clone()).map_err(|error| {
                RuntimeError::Execution(format!("invalid tool task payload: {error}"))
            })?;
        let result = self.tools.execute(&permissions, request).await?;

        Ok(TaskResult {
            output: result.output,
            metadata: serde_json::json!({
                "tool": result.metadata,
                "attempts": result.attempts,
            }),
        })
    }
}

pub struct AgentRuntime {
    scheduler: Scheduler,
    workers: Mutex<Option<WorkerPool>>,
    tasks: Mutex<HashMap<TaskId, SharedTask>>,
    agents: AgentStore,
    tools: ToolExecutor,
    sandbox: SandboxManager,
    memory: MemoryManager,
    concurrency: usize,
}

impl AgentRuntime {
    pub fn new(executor: Arc<dyn TaskExecutor>, concurrency: usize) -> Self {
        Self::new_with_tool_config(executor, concurrency, ToolConfig::default())
    }

    pub fn new_with_tool_config(
        executor: Arc<dyn TaskExecutor>,
        concurrency: usize,
        tool_config: ToolConfig,
    ) -> Self {
        Self::new_with_memory_manager(executor, concurrency, tool_config, MemoryManager::new())
    }

    pub fn new_with_memory_manager(
        executor: Arc<dyn TaskExecutor>,
        concurrency: usize,
        tool_config: ToolConfig,
        memory: MemoryManager,
    ) -> Self {
        let scheduler = Scheduler::new();
        let sandbox = SandboxManager::new(tool_config.sandbox.clone());
        let tools = ToolExecutor::with_defaults(tool_config)
            .expect("default tools must register successfully");
        let agents = Arc::new(Mutex::new(HashMap::new()));
        let worker_executor = Arc::new(RuntimeTaskExecutor {
            generic_executor: executor,
            agents: agents.clone(),
            tools: tools.clone(),
        });
        let workers = WorkerPool::new(scheduler.clone(), worker_executor, concurrency);

        Self {
            scheduler,
            workers: Mutex::new(Some(workers)),
            tasks: Mutex::new(HashMap::new()),
            agents,
            tools,
            sandbox,
            memory,
            concurrency,
        }
    }

    pub async fn submit(&self, task: Task) -> Result<SharedTask> {
        let task = self.scheduler.submit(task).await?;
        let id = task.lock().await.id;

        self.tasks.lock().await.insert(id, task.clone());

        Ok(task)
    }

    pub async fn submit_for_agent(&self, agent_id: AgentId, task: Task) -> Result<SharedTask> {
        let agent = self.agent_handle(agent_id).await?;
        let agent = agent.lock().await;

        if !agent.can_accept_tasks() {
            return Err(RuntimeError::AgentNotReady(agent_id.0.to_string()));
        }

        drop(agent);
        self.submit(task.for_agent(agent_id)).await
    }

    pub async fn get_task(&self, id: TaskId) -> Result<Task> {
        let task = self
            .tasks
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| RuntimeError::TaskNotFound(id.0.to_string()))?;

        let snapshot = task.lock().await.clone();

        Ok(snapshot)
    }

    pub async fn cancel_task(&self, id: TaskId) -> Result<Task> {
        let task = self
            .tasks
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| RuntimeError::TaskNotFound(id.0.to_string()))?;

        let mut task = task.lock().await;
        task.cancel()?;
        let snapshot = task.clone();
        drop(task);

        self.scheduler.cancel(id).await;

        Ok(snapshot)
    }

    pub async fn create_agent(&self, name: impl Into<String>) -> Result<Agent> {
        self.create_agent_with_permissions(name, PermissionSet::new())
            .await
    }

    pub async fn create_agent_with_permissions(
        &self,
        name: impl Into<String>,
        permissions: PermissionSet,
    ) -> Result<Agent> {
        let mut agent = Agent::new(name).with_permissions(permissions);
        agent.initialize()?;
        agent.ready()?;
        let snapshot = agent.clone();

        self.agents
            .lock()
            .await
            .insert(agent.id, Arc::new(Mutex::new(agent)));

        Ok(snapshot)
    }

    pub async fn get_agent(&self, id: AgentId) -> Result<Agent> {
        let agent = self.agent_handle(id).await?;
        let snapshot = agent.lock().await.clone();

        Ok(snapshot)
    }

    pub async fn submit_tool_for_agent(
        &self,
        agent_id: AgentId,
        request: ToolRequest,
    ) -> Result<SharedTask> {
        let agent = self.agent_handle(agent_id).await?;
        let agent = agent.lock().await;

        if !agent.can_accept_tasks() {
            return Err(RuntimeError::AgentNotReady(agent_id.0.to_string()));
        }

        drop(agent);
        let task = Task::tool(
            format!("tool:{}", request.tool),
            serde_json::to_value(request).expect("tool requests are serializable"),
        );

        self.submit_for_agent(agent_id, task).await
    }

    pub async fn put_agent_memory(
        &self,
        agent_id: AgentId,
        write: MemoryWrite,
    ) -> Result<MemoryRecord> {
        self.agent_handle(agent_id).await?;
        self.memory.put(agent_id, write).await
    }

    pub async fn get_agent_memory(
        &self,
        agent_id: AgentId,
        scope: MemoryScope,
        key: &str,
    ) -> Result<MemoryRecord> {
        self.agent_handle(agent_id).await?;
        self.memory
            .get(agent_id, scope, key)
            .await?
            .ok_or_else(|| RuntimeError::MemoryNotFound(key.to_owned()))
    }

    pub async fn list_agent_memory(&self, agent_id: AgentId) -> Result<Vec<MemoryRecord>> {
        self.agent_handle(agent_id).await?;
        self.memory.list(agent_id).await
    }

    pub async fn search_agent_memory(
        &self,
        agent_id: AgentId,
        query: &str,
        scope: Option<MemoryScope>,
        limit: usize,
    ) -> Result<Vec<SemanticMatch>> {
        self.agent_handle(agent_id).await?;
        self.memory.search(agent_id, query, scope, limit).await
    }

    pub fn tool_names(&self) -> Vec<String> {
        self.tools.registry().names()
    }

    pub fn tool_descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools.registry().descriptors()
    }

    pub fn sandbox_status(&self) -> SandboxStatus {
        self.sandbox.status()
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub async fn queue_len(&self) -> usize {
        self.scheduler.queue_len().await
    }

    pub async fn task_count(&self) -> usize {
        self.tasks.lock().await.len()
    }

    pub async fn agent_count(&self) -> usize {
        self.agents.lock().await.len()
    }

    pub async fn shutdown(&self) {
        if let Some(workers) = self.workers.lock().await.take() {
            workers.shutdown().await;
        }
    }

    async fn agent_handle(&self, id: AgentId) -> Result<Arc<Mutex<Agent>>> {
        self.agents
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| RuntimeError::AgentNotFound(id.0.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use serde_json::json;

    use super::*;
    use crate::{
        agent::AgentStatus,
        task::{TaskPriority, TaskStatus},
        worker::MockTaskExecutor,
    };

    #[tokio::test]
    async fn submitted_tasks_are_queryable_and_can_be_cancelled_while_running() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::from_millis(75)));
        let runtime = AgentRuntime::new(executor, 1);
        let task = runtime
            .submit(Task::new("long-running", json!({}), TaskPriority::Normal))
            .await
            .unwrap();
        let id = task.lock().await.id;

        assert_eq!(runtime.get_task(id).await.unwrap().id, id);

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            runtime.cancel_task(id).await.unwrap().status,
            TaskStatus::Cancelled
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            runtime.get_task(id).await.unwrap().status,
            TaskStatus::Cancelled
        );

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_task_returns_not_found() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::ZERO));
        let runtime = AgentRuntime::new(executor, 1);

        let error = runtime
            .get_task(crate::task::TaskId::new())
            .await
            .unwrap_err();

        assert!(matches!(error, crate::error::RuntimeError::TaskNotFound(_)));

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn agents_are_ready_and_own_their_submitted_tasks() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::ZERO));
        let runtime = AgentRuntime::new(executor, 1);
        let agent = runtime.create_agent("research-agent").await.unwrap();
        let task = runtime
            .submit_for_agent(
                agent.id,
                Task::new("research", json!({}), TaskPriority::Normal),
            )
            .await
            .unwrap();

        assert_eq!(
            runtime.get_agent(agent.id).await.unwrap().status,
            AgentStatus::Ready
        );
        assert_eq!(task.lock().await.agent_id, Some(agent.id));

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn tool_tasks_execute_through_workers_and_the_permission_boundary() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::ZERO));
        let runtime = AgentRuntime::new(executor, 1);
        let agent = runtime.create_agent("research-agent").await.unwrap();

        let task = runtime
            .submit_tool_for_agent(
                agent.id,
                ToolRequest {
                    tool: "echo".into(),
                    arguments: json!({ "ok": true }),
                    timeout_ms: 10,
                    retry_policy: Default::default(),
                },
            )
            .await
            .unwrap();
        let id = task.lock().await.id;

        tokio::time::sleep(Duration::from_millis(10)).await;
        let task = runtime.get_task(id).await.unwrap();
        assert_eq!(task.kind, TaskKind::Tool);
        assert_eq!(task.result.unwrap().output, json!({ "ok": true }));

        let denied_task = runtime
            .submit_tool_for_agent(
                agent.id,
                ToolRequest {
                    tool: "http_request".into(),
                    arguments: json!({ "url": "https://example.com" }),
                    timeout_ms: 10,
                    retry_policy: Default::default(),
                },
            )
            .await
            .unwrap();
        let denied_id = denied_task.lock().await.id;

        tokio::time::sleep(Duration::from_millis(10)).await;
        let denied = runtime.get_task(denied_id).await.unwrap();
        assert_eq!(denied.status, TaskStatus::Failed);
        assert!(denied.error.unwrap().contains("Permission denied"));

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn agents_can_manage_scoped_memory() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::ZERO));
        let runtime = AgentRuntime::new(executor, 1);
        let agent = runtime.create_agent("memory-agent").await.unwrap();

        runtime
            .put_agent_memory(
                agent.id,
                MemoryWrite {
                    key: "current_plan".to_owned(),
                    value: json!(["research", "report"]),
                    scope: MemoryScope::Working,
                    ttl_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            runtime
                .get_agent_memory(agent.id, MemoryScope::Working, "current_plan")
                .await
                .unwrap()
                .value,
            json!(["research", "report"])
        );

        runtime.shutdown().await;
    }
}
