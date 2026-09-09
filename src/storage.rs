use async_trait::async_trait;

use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};

use crate::{
    error::Result,
    task::{Task, TaskId},
};

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
    use crate::task::TaskPriority;

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
}
