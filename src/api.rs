use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    agent::{Agent, AgentId},
    error::RuntimeError,
    memory::{MemoryRecord, MemoryScope, MemoryWrite, SemanticMatch},
    runtime::AgentRuntime,
    sandbox::SandboxStatus,
    task::{Task, TaskId, TaskPriority},
    tool::{PermissionSet, ToolDescriptor, ToolRequest},
};

#[derive(Clone)]
struct ApiState {
    runtime: Arc<AgentRuntime>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitTaskRequest {
    pub name: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub priority: TaskPriority,
}

#[derive(Debug, Deserialize)]
pub struct CreateAgentRequest {
    pub name: String,
    #[serde(default)]
    pub permissions: PermissionSet,
}

#[derive(Debug, Serialize)]
pub struct RuntimeStatus {
    pub concurrency: usize,
    pub queued_tasks: usize,
    pub tracked_tasks: usize,
    pub tracked_agents: usize,
    pub sandbox: SandboxStatus,
}

#[derive(Debug, Deserialize)]
struct MemoryReadQuery {
    #[serde(default)]
    scope: MemoryScope,
}

#[derive(Debug, Deserialize)]
struct MemorySearchQuery {
    q: String,
    scope: Option<MemoryScope>,
    #[serde(default = "default_memory_search_limit")]
    limit: usize,
}

fn default_memory_search_limit() -> usize {
    10
}

pub fn router(runtime: Arc<AgentRuntime>) -> Router {
    Router::new()
        .route("/agents", post(create_agent))
        .route("/agents/{id}", get(get_agent))
        .route(
            "/agents/{id}/memory",
            get(list_agent_memory).post(put_agent_memory),
        )
        .route("/agents/{id}/memory/search", get(search_agent_memory))
        .route("/agents/{id}/memory/{key}", get(get_agent_memory))
        .route("/agents/{id}/tasks", post(submit_agent_task))
        .route("/agents/{id}/tools", post(execute_tool))
        .route("/tasks", post(submit_task))
        .route("/tasks/{id}", get(get_task))
        .route("/tasks/{id}/cancel", post(cancel_task))
        .route("/runtime/status", get(runtime_status))
        .route("/tools", get(list_tools))
        .with_state(Arc::new(ApiState { runtime }))
}

async fn submit_task(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<SubmitTaskRequest>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    validate_name(&request.name, "Task name")?;
    let task = Task::new(request.name, request.payload, request.priority);
    let task = state.runtime.submit(task).await?;
    let task = task.lock().await.clone();

    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn create_agent(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<CreateAgentRequest>,
) -> Result<(StatusCode, Json<Agent>), ApiError> {
    validate_name(&request.name, "Agent name")?;
    let agent = state
        .runtime
        .create_agent_with_permissions(request.name, request.permissions)
        .await?;

    Ok((StatusCode::CREATED, Json(agent)))
}

async fn get_agent(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<Json<Agent>, ApiError> {
    let agent = state.runtime.get_agent(parse_agent_id(&id)?).await?;

    Ok(Json(agent))
}

async fn put_agent_memory(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(write): Json<MemoryWrite>,
) -> Result<(StatusCode, Json<MemoryRecord>), ApiError> {
    validate_name(&write.key, "Memory key")?;
    let record = state
        .runtime
        .put_agent_memory(parse_agent_id(&id)?, write)
        .await?;

    Ok((StatusCode::OK, Json(record)))
}

async fn get_agent_memory(
    State(state): State<Arc<ApiState>>,
    Path((id, key)): Path<(String, String)>,
    Query(query): Query<MemoryReadQuery>,
) -> Result<Json<MemoryRecord>, ApiError> {
    let record = state
        .runtime
        .get_agent_memory(parse_agent_id(&id)?, query.scope, &key)
        .await?;

    Ok(Json(record))
}

async fn list_agent_memory(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<MemoryRecord>>, ApiError> {
    let records = state
        .runtime
        .list_agent_memory(parse_agent_id(&id)?)
        .await?;

    Ok(Json(records))
}

async fn search_agent_memory(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Query(query): Query<MemorySearchQuery>,
) -> Result<Json<Vec<SemanticMatch>>, ApiError> {
    validate_name(&query.q, "Search query")?;
    let results = state
        .runtime
        .search_agent_memory(parse_agent_id(&id)?, &query.q, query.scope, query.limit)
        .await?;

    Ok(Json(results))
}

async fn execute_tool(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(request): Json<ToolRequest>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    validate_name(&request.tool, "Tool name")?;
    let task = state
        .runtime
        .submit_tool_for_agent(parse_agent_id(&id)?, request)
        .await?;
    let task = task.lock().await.clone();

    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn list_tools(State(state): State<Arc<ApiState>>) -> Json<Vec<ToolDescriptor>> {
    Json(state.runtime.tool_descriptors())
}

async fn submit_agent_task(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(request): Json<SubmitTaskRequest>,
) -> Result<(StatusCode, Json<Task>), ApiError> {
    validate_name(&request.name, "Task name")?;
    let task = Task::new(request.name, request.payload, request.priority);
    let task = state
        .runtime
        .submit_for_agent(parse_agent_id(&id)?, task)
        .await?;
    let task = task.lock().await.clone();

    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn get_task(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<Json<Task>, ApiError> {
    let id = parse_task_id(&id)?;
    let task = state.runtime.get_task(id).await?;

    Ok(Json(task))
}

async fn cancel_task(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<Json<Task>, ApiError> {
    let id = parse_task_id(&id)?;
    let task = state.runtime.cancel_task(id).await?;

    Ok(Json(task))
}

async fn runtime_status(State(state): State<Arc<ApiState>>) -> Json<RuntimeStatus> {
    Json(RuntimeStatus {
        concurrency: state.runtime.concurrency(),
        queued_tasks: state.runtime.queue_len().await,
        tracked_tasks: state.runtime.task_count().await,
        tracked_agents: state.runtime.agent_count().await,
        sandbox: state.runtime.sandbox_status(),
    })
}

fn parse_agent_id(value: &str) -> Result<AgentId, ApiError> {
    Uuid::parse_str(value)
        .map(AgentId)
        .map_err(|_| ApiError::bad_request("Agent id must be a UUID"))
}

fn validate_name(value: &str, field: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(format!("{field} must not be empty")));
    }

    Ok(())
}

fn parse_task_id(value: &str) -> Result<TaskId, ApiError> {
    Uuid::parse_str(value)
        .map(TaskId)
        .map_err(|_| ApiError::bad_request("Task id must be a UUID"))
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

impl From<RuntimeError> for ApiError {
    fn from(error: RuntimeError) -> Self {
        let status = match error {
            RuntimeError::AgentNotFound(_)
            | RuntimeError::TaskNotFound(_)
            | RuntimeError::MemoryNotFound(_)
            | RuntimeError::ToolNotFound(_) => StatusCode::NOT_FOUND,
            RuntimeError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            RuntimeError::AgentNotReady(_) | RuntimeError::InvalidStateTransition(_) => {
                StatusCode::CONFLICT
            }
            RuntimeError::SchedulerShutdown => StatusCode::SERVICE_UNAVAILABLE,
            RuntimeError::ToolTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
            RuntimeError::ToolAlreadyRegistered(_) => StatusCode::CONFLICT,
            RuntimeError::InvalidMemoryKey(_) => StatusCode::BAD_REQUEST,
            RuntimeError::Execution(_)
            | RuntimeError::Worker(_)
            | RuntimeError::Storage(_)
            | RuntimeError::StorageMigration(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RuntimeError::MemoryBackend(_) => StatusCode::SERVICE_UNAVAILABLE,
        };

        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::{agent::AgentStatus, task::TaskKind, worker::MockTaskExecutor};

    #[tokio::test]
    async fn agent_and_task_routes_preserve_runtime_state() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::from_millis(100)));
        let runtime = Arc::new(AgentRuntime::new(executor, 1));
        let app = router(runtime.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/agents")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"research-agent"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let agent: Agent =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(agent.status, AgentStatus::Ready);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/agents/{}/memory", agent.id.0))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"key":"plan","value":{"step":1},"scope":"working"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let memory: MemoryRecord =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(memory.value, serde_json::json!({ "step": 1 }));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/agents/{}/memory/search?q=plan&scope=working&limit=1",
                        agent.id.0
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let matches: Vec<SemanticMatch> =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].record.key, "plan");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/agents/{}/tools", agent.id.0))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"tool":"echo","arguments":{"message":"hello"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let tool_task: Task =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(tool_task.kind, TaskKind::Tool);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/agents/{}/tasks", agent.id.0))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"collect-sources","payload":{"topic":"Rust"},"priority":"High"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let task: Task =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(task.agent_id, Some(agent.id));

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/tasks/{}", task.id.0))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_request_returns_a_client_error() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::ZERO));
        let runtime = Arc::new(AgentRuntime::new(executor, 1));
        let app = router(runtime.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/agents")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"   "}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn tools_route_lists_phase_two_descriptors() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::ZERO));
        let runtime = Arc::new(AgentRuntime::new(executor, 1));
        let app = router(runtime.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tools: Vec<ToolDescriptor> =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let names = tools.into_iter().map(|tool| tool.name).collect::<Vec<_>>();
        assert!(names.contains(&"http_request".to_owned()));
        assert!(names.contains(&"file_read".to_owned()));
        assert!(!names.contains(&"shell".to_owned()));
        assert!(!names.contains(&"python".to_owned()));

        runtime.shutdown().await;
    }
}
