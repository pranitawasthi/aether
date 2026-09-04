use async_trait::async_trait;

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

use sqlx::PgPool;

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
}