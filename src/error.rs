use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("Agent not found: {0}")]
    AgentNotFound(String),

    #[error("Agent is not ready to accept tasks: {0}")]
    AgentNotReady(String),

    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Tool execution timed out: {0}")]
    ToolTimeout(String),

    #[error("Tool is already registered: {0}")]
    ToolAlreadyRegistered(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Invalid task state transition: {0}")]
    InvalidStateTransition(String),

    #[error("Scheduler is shutting down")]
    SchedulerShutdown,

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Worker error: {0}")]
    Worker(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;
