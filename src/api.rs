use std::sync::Arc;

use axum::{
    extract::{Path, State},
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
    runtime::AgentRuntime,
    task::{Task, TaskId, TaskPriority},
    tool::{PermissionSet, ToolDescriptor, ToolRequest, ToolResult},
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
}

pub fn router(runtime: Arc<AgentRuntime>) -> Router {
    Router::new()
        .route("/agents", post(create_agent))
        .route("/agents/{id}", get(get_agent))
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

async fn execute_tool(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(request): Json<ToolRequest>,
) -> Result<Json<ToolResult>, ApiError> {
    validate_name(&request.tool, "Tool name")?;
    let result = state
        .runtime
        .execute_tool(parse_agent_id(&id)?, request)
        .await?;

    Ok(Json(result))
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
            | RuntimeError::ToolNotFound(_) => StatusCode::NOT_FOUND,
            RuntimeError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            RuntimeError::AgentNotReady(_) | RuntimeError::InvalidStateTransition(_) => {
                StatusCode::CONFLICT
            }
            RuntimeError::SchedulerShutdown => StatusCode::SERVICE_UNAVAILABLE,
            RuntimeError::ToolTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
            RuntimeError::ToolAlreadyRegistered(_) => StatusCode::CONFLICT,
            RuntimeError::Execution(_) | RuntimeError::Worker(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
    use crate::{agent::AgentStatus, worker::MockTaskExecutor};

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
                    .uri(format!("/agents/{}/tools", agent.id.0))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"tool":"echo","arguments":{"message":"hello"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_result: ToolResult =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            tool_result.output,
            serde_json::json!({ "message": "hello" })
        );

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
        assert!(names.contains(&"shell".to_owned()));
        assert!(names.contains(&"python".to_owned()));

        runtime.shutdown().await;
    }
}
