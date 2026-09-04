use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::json;
use tokio::task::JoinHandle;

use crate::{
    error::{Result, RuntimeError},
    scheduler::Scheduler,
    task::{Task, TaskResult, TaskStatus},
};

#[async_trait]
pub trait TaskExecutor: Send + Sync + 'static {
    async fn execute(&self, task: &Task) -> Result<TaskResult>;
}

#[derive(Clone)]
pub struct MockTaskExecutor {
    delay: Duration,

    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl MockTaskExecutor {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,

            active: Arc::new(AtomicUsize::new(0)),

            max_active: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }

    fn update_max(&self, current: usize) {
        loop {
            let max = self.max_active.load(Ordering::SeqCst);

            if current <= max {
                break;
            }

            if self
                .max_active
                .compare_exchange(max, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }
}

#[async_trait]
impl TaskExecutor for MockTaskExecutor {
    async fn execute(&self, task: &Task) -> Result<TaskResult> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;

        self.update_max(active);

        tracing::info!(
            task_id = %task.id.0,
            task_name = %task.name,
            active_workers = active,
            "Mock executor started task"
        );

        tokio::time::sleep(self.delay).await;

        self.active.fetch_sub(1, Ordering::SeqCst);

        if task.name.contains("fail") {
            return Err(RuntimeError::Execution("Mock execution failure".into()));
        }

        Ok(TaskResult {
            output: json!({
                "message": format!(
                    "Task {} completed",
                    task.name
                )
            }),

            metadata: json!({
                "executor": "mock"
            }),
        })
    }
}

pub struct WorkerPool {
    scheduler: Scheduler,
    workers: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    pub fn new(scheduler: Scheduler, executor: Arc<dyn TaskExecutor>, concurrency: usize) -> Self {
        assert!(
            concurrency > 0,
            "Worker concurrency must be greater than zero"
        );

        let mut workers = Vec::with_capacity(concurrency);

        for worker_id in 0..concurrency {
            let scheduler = scheduler.clone();
            let executor = executor.clone();

            let handle = tokio::spawn(async move {
                worker_loop(worker_id, scheduler, executor).await;
            });

            workers.push(handle);
        }

        Self { scheduler, workers }
    }

    pub fn concurrency(&self) -> usize {
        self.workers.len()
    }

    pub async fn shutdown(self) {
        tracing::info!(workers = self.workers.len(), "Shutting down worker pool");

        self.scheduler.shutdown();

        for worker in self.workers {
            if let Err(error) = worker.await {
                tracing::error!(?error, "Worker terminated unexpectedly");
            }
        }

        tracing::info!("Worker pool stopped");
    }
}

async fn worker_loop(worker_id: usize, scheduler: Scheduler, executor: Arc<dyn TaskExecutor>) {
    tracing::info!(worker_id, "Worker started");

    while let Some(task) = scheduler.next_task().await {
        let snapshot = {
            let mut task_guard = task.lock().await;

            if task_guard.status == TaskStatus::Cancelled {
                tracing::debug!(worker_id, task_id = %task_guard.id.0, "Skipping cancelled task");
                continue;
            }

            if let Err(error) = task_guard.start() {
                tracing::error!(worker_id, ?error, "Failed to start task");

                continue;
            }

            task_guard.clone()
        };

        tracing::info!(
            worker_id,
            task_id = %snapshot.id.0,
            task_name = %snapshot.name,
            "Executing task"
        );

        let result = executor.execute(&snapshot).await;

        let mut task_guard = task.lock().await;

        if task_guard.status == TaskStatus::Cancelled {
            tracing::info!(
                worker_id,
                task_id = %task_guard.id.0,
                "Discarding result for cancelled task"
            );
            continue;
        }

        match result {
            Ok(result) => {
                if let Err(error) = task_guard.complete(result) {
                    tracing::error!(worker_id, ?error, "Failed to complete task");
                } else {
                    tracing::info!(
                        worker_id,
                        task_id = %task_guard.id.0,
                        "Task completed"
                    );
                }
            }

            Err(error) => {
                let message = error.to_string();

                if let Err(state_error) = task_guard.fail(message.clone()) {
                    tracing::error!(worker_id, ?state_error, "Failed to mark task as failed");
                } else {
                    tracing::warn!(
                        worker_id,
                        task_id = %task_guard.id.0,
                        error = %message,
                        "Task failed"
                    );
                }
            }
        }
    }

    tracing::info!(worker_id, "Worker stopped");
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::{sync::Arc, time::Duration};

    use serde_json::json;

    use crate::{
        runtime::AgentRuntime,
        task::{Task, TaskPriority, TaskStatus},
    };

    #[tokio::test]
    async fn worker_pool_respects_concurrency_limit() {
        let executor = Arc::new(MockTaskExecutor::new(Duration::from_millis(100)));

        let runtime = AgentRuntime::new(executor.clone(), 3);

        let mut tasks = Vec::new();

        for i in 0..10 {
            let task = Task::new(format!("task-{i}"), json!({}), TaskPriority::Normal);

            let handle = runtime.submit(task).await.unwrap();

            tasks.push(handle);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(executor.max_concurrency(), 3);

        for task in tasks {
            let task = task.lock().await;

            assert_eq!(task.status, TaskStatus::Completed);
        }

        runtime.shutdown().await;
    }
}
