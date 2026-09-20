use async_trait::async_trait;

use chrono::{DateTime, Duration, Utc};
use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};
use uuid::Uuid;

use crate::{
    agent::{Agent, AgentId},
    error::{Result, RuntimeError},
    task::{Task, TaskId, TaskStatus},
};

#[derive(Debug, Clone)]
pub struct TaskLease {
    pub task_id: TaskId,
    pub worker_id: String,
    pub token: Uuid,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub task: Task,
    pub lease: TaskLease,
}

#[async_trait]
pub trait TaskStorage: Send + Sync {
    async fn create_task(&self, task: &Task) -> Result<()>;

    async fn get_task(&self, id: TaskId) -> Result<Option<Task>>;

    async fn update_task(&self, task: &Task) -> Result<()>;
}

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;

        Ok(Self::new(pool))
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn enqueue(&self, mut task: Task) -> Result<Task> {
        if task.status == TaskStatus::Created {
            task.queue()?;
        }
        if task.status != TaskStatus::Queued {
            return Err(RuntimeError::InvalidStateTransition(format!(
                "Only created or queued tasks can be enqueued; found {:?}",
                task.status
            )));
        }

        let mut transaction = self.pool.begin().await?;
        sqlx::query("INSERT INTO tasks (id, task, created_at, updated_at) VALUES ($1, $2, $3, $4)")
            .bind(task.id.0)
            .bind(Json(&task))
            .bind(task.created_at)
            .bind(task.updated_at)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO task_leases (task_id, priority, available_at) VALUES ($1, $2, NOW())",
        )
        .bind(task.id.0)
        .bind(task.priority as i16)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(task)
    }

    pub async fn create_agent(&self, agent: &Agent) -> Result<()> {
        sqlx::query(
            "INSERT INTO agents (id, agent, created_at, updated_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(agent.id.0)
        .bind(Json(agent))
        .bind(agent.created_at)
        .bind(agent.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_agent(&self, id: AgentId) -> Result<Option<Agent>> {
        let row = sqlx::query("SELECT agent FROM agents WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| row.try_get::<Json<Agent>, _>("agent").map(|agent| agent.0))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn queue_len(&self) -> Result<usize> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_leases WHERE completed_at IS NULL")
                .fetch_one(&self.pool)
                .await?;
        Ok(count as usize)
    }

    pub async fn register_worker(&self, worker_id: &str) -> Result<()> {
        if worker_id.trim().is_empty() {
            return Err(RuntimeError::Execution(
                "Worker id must not be empty".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO worker_heartbeats (worker_id, started_at, last_seen_at) \
             VALUES ($1, NOW(), NOW()) \
             ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = NOW()",
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn active_worker_count(&self, grace_period: Duration) -> Result<usize> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM worker_heartbeats WHERE last_seen_at > NOW() - $1::interval",
        )
        .bind(format!(
            "{} milliseconds",
            grace_period.num_milliseconds().max(0)
        ))
        .fetch_one(&self.pool)
        .await?;
        Ok(count as usize)
    }

    pub async fn cancel(&self, id: TaskId) -> Result<Option<Task>> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT task FROM tasks WHERE id = $1 FOR UPDATE")
            .bind(id.0)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let mut task = row.try_get::<Json<Task>, _>("task")?.0;
        task.cancel()?;
        sqlx::query("UPDATE tasks SET task = $2, updated_at = $3 WHERE id = $1")
            .bind(id.0)
            .bind(Json(&task))
            .bind(task.updated_at)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE task_leases SET completed_at = NOW(), lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL WHERE task_id = $1 AND completed_at IS NULL",
        )
        .bind(id.0)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(task))
    }

    pub async fn claim_next(
        &self,
        worker_id: impl Into<String>,
        lease_duration: Duration,
    ) -> Result<Option<ClaimedTask>> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            return Err(RuntimeError::Execution(
                "Worker id must not be empty".to_owned(),
            ));
        }
        if lease_duration <= Duration::zero() {
            return Err(RuntimeError::Execution(
                "Lease duration must be positive".to_owned(),
            ));
        }

        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT leases.task_id, leases.lease_expires_at, tasks.task \
             FROM task_leases AS leases \
             JOIN tasks ON tasks.id = leases.task_id \
             WHERE leases.completed_at IS NULL \
               AND (leases.lease_owner IS NULL OR leases.lease_expires_at <= NOW()) \
             ORDER BY leases.priority DESC, leases.sequence ASC \
             LIMIT 1 FOR UPDATE OF leases SKIP LOCKED",
        )
        .fetch_optional(&mut *transaction)
        .await?;

        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };

        let task_id = TaskId(row.try_get("task_id")?);
        let previous_expiry: Option<DateTime<Utc>> = row.try_get("lease_expires_at")?;
        let mut task = row.try_get::<Json<Task>, _>("task")?.0;
        if previous_expiry.is_some() && task.status == TaskStatus::Running {
            task.requeue_after_lease_expiry()?;
            sqlx::query("UPDATE tasks SET task = $2, updated_at = $3 WHERE id = $1")
                .bind(task.id.0)
                .bind(Json(&task))
                .bind(task.updated_at)
                .execute(&mut *transaction)
                .await?;
        }

        let token = Uuid::new_v4();
        let expires_at = Utc::now() + lease_duration;
        sqlx::query(
            "UPDATE task_leases SET lease_owner = $2, lease_token = $3, lease_expires_at = $4 \
             WHERE task_id = $1",
        )
        .bind(task_id.0)
        .bind(&worker_id)
        .bind(token)
        .bind(expires_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(Some(ClaimedTask {
            task,
            lease: TaskLease {
                task_id,
                worker_id,
                token,
                expires_at,
            },
        }))
    }

    pub async fn renew_lease(&self, lease: &mut TaskLease, lease_duration: Duration) -> Result<()> {
        let expires_at = Utc::now() + lease_duration;
        let result = sqlx::query(
            "UPDATE task_leases SET lease_expires_at = $4 \
             WHERE task_id = $1 AND lease_owner = $2 AND lease_token = $3 \
               AND completed_at IS NULL AND lease_expires_at > NOW()",
        )
        .bind(lease.task_id.0)
        .bind(&lease.worker_id)
        .bind(lease.token)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::LeaseLost(lease.task_id.0.to_string()));
        }
        lease.expires_at = expires_at;
        Ok(())
    }

    pub async fn mark_running(&self, lease: &TaskLease, task: &Task) -> Result<()> {
        if task.status != TaskStatus::Running {
            return Err(RuntimeError::InvalidStateTransition(
                "Only running tasks can be marked as started".to_owned(),
            ));
        }

        let result = sqlx::query(
            "UPDATE tasks SET task = $2, updated_at = $3 \
             WHERE id = $1 AND EXISTS ( \
                SELECT 1 FROM task_leases \
                WHERE task_id = $1 AND lease_owner = $4 AND lease_token = $5 \
                  AND completed_at IS NULL AND lease_expires_at > NOW() \
             )",
        )
        .bind(task.id.0)
        .bind(Json(task))
        .bind(task.updated_at)
        .bind(&lease.worker_id)
        .bind(lease.token)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::LeaseLost(lease.task_id.0.to_string()));
        }
        Ok(())
    }

    pub async fn acknowledge(&self, lease: &TaskLease, task: &Task) -> Result<()> {
        if !matches!(
            task.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            return Err(RuntimeError::InvalidStateTransition(
                "Only terminal tasks can be acknowledged".to_owned(),
            ));
        }

        let mut transaction = self.pool.begin().await?;
        let lease_update = sqlx::query(
            "UPDATE task_leases SET completed_at = NOW(), lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE task_id = $1 AND lease_owner = $2 AND lease_token = $3 \
               AND completed_at IS NULL AND lease_expires_at > NOW()",
        )
        .bind(lease.task_id.0)
        .bind(&lease.worker_id)
        .bind(lease.token)
        .execute(&mut *transaction)
        .await?;
        if lease_update.rows_affected() != 1 {
            return Err(RuntimeError::LeaseLost(lease.task_id.0.to_string()));
        }
        sqlx::query("UPDATE tasks SET task = $2, updated_at = $3 WHERE id = $1")
            .bind(task.id.0)
            .bind(Json(task))
            .bind(task.updated_at)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl TaskStorage for PostgresStorage {
    async fn create_task(&self, task: &Task) -> Result<()> {
        sqlx::query("INSERT INTO tasks (id, task, created_at, updated_at) VALUES ($1, $2, $3, $4)")
            .bind(task.id.0)
            .bind(Json(task))
            .bind(task.created_at)
            .bind(task.updated_at)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<Task>> {
        let row = sqlx::query("SELECT task FROM tasks WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| row.try_get::<Json<Task>, _>("task").map(|task| task.0))
            .transpose()
            .map_err(Into::into)
    }

    async fn update_task(&self, task: &Task) -> Result<()> {
        sqlx::query("UPDATE tasks SET task = $2, updated_at = $3 WHERE id = $1")
            .bind(task.id.0)
            .bind(Json(task))
            .bind(task.updated_at)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use serde_json::json;

    use super::*;
    use crate::task::{TaskPriority, TaskResult};

    #[test]
    fn task_json_round_trip_preserves_storage_payload() {
        let task = Task::new(
            "storage-test",
            json!({ "source": "test" }),
            TaskPriority::High,
        );
        let serialized = serde_json::to_value(&task).unwrap();
        let restored: Task = serde_json::from_value(serialized).unwrap();

        assert_eq!(restored.id, task.id);
        assert_eq!(restored.priority, TaskPriority::High);
    }

    #[tokio::test]
    async fn postgres_storage_crud_when_configured() {
        let Ok(database_url) = env::var("AGENT_RUNTIME_TEST_DATABASE_URL") else {
            return;
        };
        let storage = PostgresStorage::connect(&database_url, 1).await.unwrap();
        storage.migrate().await.unwrap();

        let mut task = Task::new(
            "postgres-storage-test",
            json!({ "version": 1 }),
            TaskPriority::High,
        );
        storage.create_task(&task).await.unwrap();
        assert_eq!(storage.get_task(task.id).await.unwrap(), Some(task.clone()));

        task.queue().unwrap();
        storage.update_task(&task).await.unwrap();
        assert_eq!(storage.get_task(task.id).await.unwrap(), Some(task));
    }

    #[tokio::test]
    async fn postgres_queue_claims_and_acknowledges_when_configured() {
        let Ok(database_url) = env::var("AGENT_RUNTIME_TEST_DATABASE_URL") else {
            return;
        };
        let storage = PostgresStorage::connect(&database_url, 1).await.unwrap();
        storage.migrate().await.unwrap();
        storage
            .register_worker("storage-test-worker")
            .await
            .unwrap();
        assert_eq!(
            storage
                .active_worker_count(Duration::seconds(5))
                .await
                .unwrap(),
            1
        );

        let queued = storage
            .enqueue(Task::new(
                "leased-task",
                json!({ "version": 1 }),
                TaskPriority::High,
            ))
            .await
            .unwrap();
        assert_eq!(queued.status, TaskStatus::Queued);

        let claimed = storage
            .claim_next("worker-a", Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.task.id, queued.id);
        assert_eq!(claimed.lease.worker_id, "worker-a");

        let mut finished = claimed.task.clone();
        finished.start().unwrap();
        storage
            .mark_running(&claimed.lease, &finished)
            .await
            .unwrap();
        assert_eq!(
            storage.get_task(queued.id).await.unwrap().unwrap().status,
            TaskStatus::Running
        );
        finished
            .complete(TaskResult {
                output: json!({ "ok": true }),
                metadata: json!({}),
            })
            .unwrap();
        storage
            .acknowledge(&claimed.lease, &finished)
            .await
            .unwrap();
        assert_eq!(
            storage.get_task(queued.id).await.unwrap().unwrap().status,
            TaskStatus::Completed
        );

        sqlx::query("DELETE FROM tasks WHERE id = $1")
            .bind(queued.id.0)
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM worker_heartbeats WHERE worker_id = $1")
            .bind("storage-test-worker")
            .execute(storage.pool())
            .await
            .unwrap();
    }
}
