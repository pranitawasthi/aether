use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};
use tokio::sync::RwLock;

use crate::{
    agent::AgentId,
    error::{Result, RuntimeError},
};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    #[default]
    Working,
    Persistent,
}

impl MemoryScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::Persistent => "persistent",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryWrite {
    pub key: String,
    pub value: serde_json::Value,
    #[serde(default)]
    pub scope: MemoryScope,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub agent_id: AgentId,
    pub key: String,
    pub value: serde_json::Value,
    pub scope: MemoryScope,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl MemoryRecord {
    fn expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticMatch {
    pub record: MemoryRecord,
    pub score: f64,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryConfig {
    pub redis_url: Option<String>,
    pub postgres_url: Option<String>,
    pub postgres_max_connections: u32,
}

#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn put(&self, record: MemoryRecord) -> Result<()>;
    async fn get(
        &self,
        agent_id: AgentId,
        scope: MemoryScope,
        key: &str,
    ) -> Result<Option<MemoryRecord>>;
    async fn list(&self, agent_id: AgentId, scope: MemoryScope) -> Result<Vec<MemoryRecord>>;
}

type MemoryKey = (AgentId, MemoryScope, String);

#[derive(Clone, Default)]
pub struct InMemoryStore {
    records: Arc<RwLock<HashMap<MemoryKey, MemoryRecord>>>,
}

#[async_trait]
impl MemoryStore for InMemoryStore {
    async fn put(&self, record: MemoryRecord) -> Result<()> {
        self.records
            .write()
            .await
            .insert((record.agent_id, record.scope, record.key.clone()), record);
        Ok(())
    }

    async fn get(
        &self,
        agent_id: AgentId,
        scope: MemoryScope,
        key: &str,
    ) -> Result<Option<MemoryRecord>> {
        let memory_key = (agent_id, scope, key.to_owned());
        let mut records = self.records.write().await;
        if records
            .get(&memory_key)
            .is_some_and(|record| record.expired(Utc::now()))
        {
            records.remove(&memory_key);
            return Ok(None);
        }

        Ok(records.get(&memory_key).cloned())
    }

    async fn list(&self, agent_id: AgentId, scope: MemoryScope) -> Result<Vec<MemoryRecord>> {
        let now = Utc::now();
        let mut records = self.records.write().await;
        records.retain(|_, record| !record.expired(now));

        Ok(records
            .values()
            .filter(|record| record.agent_id == agent_id && record.scope == scope)
            .cloned()
            .collect())
    }
}

#[derive(Clone)]
pub struct RedisMemoryStore {
    client: redis::Client,
}

impl RedisMemoryStore {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|error| {
            RuntimeError::MemoryBackend(format!("Redis configuration: {error}"))
        })?;
        Ok(Self { client })
    }

    fn record_key(agent_id: AgentId, scope: MemoryScope, key: &str) -> String {
        format!(
            "agent-runtime:memory:{}:{}:{key}",
            agent_id.0,
            scope.as_str()
        )
    }

    fn index_key(agent_id: AgentId, scope: MemoryScope) -> String {
        format!(
            "agent-runtime:memory:{}:{}:keys",
            agent_id.0,
            scope.as_str()
        )
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| RuntimeError::MemoryBackend(format!("Redis connection: {error}")))
    }
}

#[async_trait]
impl MemoryStore for RedisMemoryStore {
    async fn put(&self, record: MemoryRecord) -> Result<()> {
        let serialized = serde_json::to_string(&record).map_err(|error| {
            RuntimeError::MemoryBackend(format!("Redis serialization: {error}"))
        })?;
        let record_key = Self::record_key(record.agent_id, record.scope, &record.key);
        let index_key = Self::index_key(record.agent_id, record.scope);
        let ttl_ms = record.expires_at.map(|expires_at| {
            (expires_at - Utc::now())
                .num_milliseconds()
                .max(1)
                .try_into()
                .unwrap_or(u64::MAX)
        });
        let mut connection = self.connection().await?;

        if let Some(ttl_ms) = ttl_ms {
            connection
                .pset_ex::<_, _, ()>(&record_key, serialized, ttl_ms)
                .await
                .map_err(|error| RuntimeError::MemoryBackend(format!("Redis write: {error}")))?;
        } else {
            connection
                .set::<_, _, ()>(&record_key, serialized)
                .await
                .map_err(|error| RuntimeError::MemoryBackend(format!("Redis write: {error}")))?;
        }
        connection
            .sadd::<_, _, ()>(index_key, record_key)
            .await
            .map_err(|error| RuntimeError::MemoryBackend(format!("Redis index: {error}")))?;
        Ok(())
    }

    async fn get(
        &self,
        agent_id: AgentId,
        scope: MemoryScope,
        key: &str,
    ) -> Result<Option<MemoryRecord>> {
        let record_key = Self::record_key(agent_id, scope, key);
        let mut connection = self.connection().await?;
        let serialized: Option<String> = connection
            .get(&record_key)
            .await
            .map_err(|error| RuntimeError::MemoryBackend(format!("Redis read: {error}")))?;

        serialized
            .map(|serialized| {
                serde_json::from_str(&serialized).map_err(|error| {
                    RuntimeError::MemoryBackend(format!("Redis decoding: {error}"))
                })
            })
            .transpose()
    }

    async fn list(&self, agent_id: AgentId, scope: MemoryScope) -> Result<Vec<MemoryRecord>> {
        let index_key = Self::index_key(agent_id, scope);
        let mut connection = self.connection().await?;
        let keys: Vec<String> = connection
            .smembers(&index_key)
            .await
            .map_err(|error| RuntimeError::MemoryBackend(format!("Redis index read: {error}")))?;
        let mut records = Vec::new();

        for key in keys {
            let serialized: Option<String> = connection
                .get(&key)
                .await
                .map_err(|error| RuntimeError::MemoryBackend(format!("Redis read: {error}")))?;
            if let Some(serialized) = serialized {
                let record = serde_json::from_str(&serialized).map_err(|error| {
                    RuntimeError::MemoryBackend(format!("Redis decoding: {error}"))
                })?;
                records.push(record);
            } else {
                connection
                    .srem::<_, _, ()>(&index_key, key)
                    .await
                    .map_err(|error| {
                        RuntimeError::MemoryBackend(format!("Redis index cleanup: {error}"))
                    })?;
            }
        }

        Ok(records)
    }
}

#[derive(Clone)]
pub struct PostgresMemoryStore {
    pool: PgPool,
}

impl PostgresMemoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect(database_url)
            .await?;
        let store = Self::new(pool);
        store.migrate().await?;
        Ok(store)
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
impl MemoryStore for PostgresMemoryStore {
    async fn put(&self, record: MemoryRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO memory_records (agent_id, scope, key, record, created_at, updated_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (agent_id, scope, key) DO UPDATE SET \
             record = EXCLUDED.record, updated_at = EXCLUDED.updated_at, expires_at = EXCLUDED.expires_at",
        )
        .bind(record.agent_id.0)
        .bind(record.scope.as_str())
        .bind(&record.key)
        .bind(Json(&record))
        .bind(record.created_at)
        .bind(record.updated_at)
        .bind(record.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get(
        &self,
        agent_id: AgentId,
        scope: MemoryScope,
        key: &str,
    ) -> Result<Option<MemoryRecord>> {
        sqlx::query(
            "DELETE FROM memory_records WHERE expires_at IS NOT NULL AND expires_at <= NOW()",
        )
        .execute(&self.pool)
        .await?;
        let row = sqlx::query(
            "SELECT record FROM memory_records WHERE agent_id = $1 AND scope = $2 AND key = $3",
        )
        .bind(agent_id.0)
        .bind(scope.as_str())
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            row.try_get::<Json<MemoryRecord>, _>("record")
                .map(|record| record.0)
        })
        .transpose()
        .map_err(Into::into)
    }

    async fn list(&self, agent_id: AgentId, scope: MemoryScope) -> Result<Vec<MemoryRecord>> {
        sqlx::query(
            "DELETE FROM memory_records WHERE expires_at IS NOT NULL AND expires_at <= NOW()",
        )
        .execute(&self.pool)
        .await?;
        let rows = sqlx::query(
            "SELECT record FROM memory_records WHERE agent_id = $1 AND scope = $2 ORDER BY updated_at DESC",
        )
        .bind(agent_id.0)
        .bind(scope.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                row.try_get::<Json<MemoryRecord>, _>("record")
                    .map(|record| record.0)
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct MemoryManager {
    working: Arc<dyn MemoryStore>,
    persistent: Arc<dyn MemoryStore>,
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::in_memory()
    }
}

impl MemoryManager {
    pub fn new() -> Self {
        Self::in_memory()
    }

    pub fn in_memory() -> Self {
        Self {
            working: Arc::new(InMemoryStore::default()),
            persistent: Arc::new(InMemoryStore::default()),
        }
    }

    pub fn with_backends(working: Arc<dyn MemoryStore>, persistent: Arc<dyn MemoryStore>) -> Self {
        Self {
            working,
            persistent,
        }
    }

    pub async fn from_config(config: MemoryConfig) -> Result<Self> {
        let working: Arc<dyn MemoryStore> = match config.redis_url {
            Some(redis_url) => Arc::new(RedisMemoryStore::new(&redis_url)?),
            None => Arc::new(InMemoryStore::default()),
        };
        let persistent: Arc<dyn MemoryStore> = match config.postgres_url {
            Some(postgres_url) => Arc::new(
                PostgresMemoryStore::connect(&postgres_url, config.postgres_max_connections.max(1))
                    .await?,
            ),
            None => Arc::new(InMemoryStore::default()),
        };

        Ok(Self::with_backends(working, persistent))
    }

    pub async fn put(&self, agent_id: AgentId, write: MemoryWrite) -> Result<MemoryRecord> {
        if write.key.trim().is_empty() {
            return Err(RuntimeError::InvalidMemoryKey(
                "memory keys must not be empty".to_owned(),
            ));
        }

        let now = Utc::now();
        let expires_at = write.ttl_ms.and_then(|ttl_ms| {
            i64::try_from(ttl_ms)
                .ok()
                .and_then(|ttl_ms| now.checked_add_signed(Duration::milliseconds(ttl_ms)))
        });
        let store = self.store(write.scope);
        let created_at = store
            .get(agent_id, write.scope, &write.key)
            .await?
            .map_or(now, |record| record.created_at);
        let record = MemoryRecord {
            agent_id,
            key: write.key,
            value: write.value,
            scope: write.scope,
            created_at,
            updated_at: now,
            expires_at,
        };
        store.put(record.clone()).await?;
        Ok(record)
    }

    pub async fn get(
        &self,
        agent_id: AgentId,
        scope: MemoryScope,
        key: &str,
    ) -> Result<Option<MemoryRecord>> {
        self.store(scope).get(agent_id, scope, key).await
    }

    pub async fn list(&self, agent_id: AgentId) -> Result<Vec<MemoryRecord>> {
        let mut records = self.working.list(agent_id, MemoryScope::Working).await?;
        records.extend(
            self.persistent
                .list(agent_id, MemoryScope::Persistent)
                .await?,
        );
        records.sort_by(|left, right| {
            left.scope
                .cmp(&right.scope)
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(records)
    }

    pub async fn search(
        &self,
        agent_id: AgentId,
        query: &str,
        scope: Option<MemoryScope>,
        limit: usize,
    ) -> Result<Vec<SemanticMatch>> {
        let records = match scope {
            Some(MemoryScope::Working) => self.working.list(agent_id, MemoryScope::Working).await?,
            Some(MemoryScope::Persistent) => {
                self.persistent
                    .list(agent_id, MemoryScope::Persistent)
                    .await?
            }
            None => self.list(agent_id).await?,
        };
        let query_vector = embed(query);
        let mut matches = records
            .into_iter()
            .map(|record| SemanticMatch {
                score: cosine_similarity(&query_vector, &embed(&record_text(&record))),
                record,
            })
            .filter(|result| result.score > 0.0)
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| right.score.total_cmp(&left.score));
        matches.truncate(limit.max(1));
        Ok(matches)
    }

    fn store(&self, scope: MemoryScope) -> &Arc<dyn MemoryStore> {
        match scope {
            MemoryScope::Working => &self.working,
            MemoryScope::Persistent => &self.persistent,
        }
    }
}

const VECTOR_DIMENSIONS: usize = 128;

fn embed(text: &str) -> [f64; VECTOR_DIMENSIONS] {
    let mut vector = [0.0; VECTOR_DIMENSIONS];
    for token in text
        .to_lowercase()
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let mut hasher = DefaultHasher::new();
        token.hash(&mut hasher);
        vector[(hasher.finish() as usize) % VECTOR_DIMENSIONS] += 1.0;
    }
    vector
}

fn cosine_similarity(left: &[f64; VECTOR_DIMENSIONS], right: &[f64; VECTOR_DIMENSIONS]) -> f64 {
    let dot_product = left
        .iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum::<f64>();
    let left_norm = left.iter().map(|value| value * value).sum::<f64>().sqrt();
    let right_norm = right.iter().map(|value| value * value).sum::<f64>().sqrt();

    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot_product / (left_norm * right_norm)
    }
}

fn record_text(record: &MemoryRecord) -> String {
    format!("{} {}", record.key, record.value)
}

#[cfg(test)]
mod tests {
    use std::{env, time::Duration as StdDuration};

    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn memory_records_are_scoped_to_an_agent_and_expire() {
        let memory = MemoryManager::new();
        let agent = AgentId::new();
        memory
            .put(
                agent,
                MemoryWrite {
                    key: "plan".to_owned(),
                    value: json!({ "step": 1 }),
                    scope: MemoryScope::Working,
                    ttl_ms: Some(5),
                },
            )
            .await
            .unwrap();
        assert!(memory
            .get(agent, MemoryScope::Working, "plan")
            .await
            .unwrap()
            .is_some());

        tokio::time::sleep(StdDuration::from_millis(10)).await;
        assert!(memory
            .get(agent, MemoryScope::Working, "plan")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn updates_preserve_creation_time_and_replace_the_value() {
        let memory = MemoryManager::new();
        let agent = AgentId::new();
        let initial = memory
            .put(
                agent,
                MemoryWrite {
                    key: "preference".to_owned(),
                    value: json!("AWS"),
                    scope: MemoryScope::Persistent,
                    ttl_ms: None,
                },
            )
            .await
            .unwrap();
        let updated = memory
            .put(
                agent,
                MemoryWrite {
                    key: "preference".to_owned(),
                    value: json!("GCP"),
                    scope: MemoryScope::Persistent,
                    ttl_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(updated.created_at, initial.created_at);
        assert_eq!(updated.value, json!("GCP"));
    }

    #[tokio::test]
    async fn semantic_search_ranks_related_memory_records() {
        let memory = MemoryManager::new();
        let agent = AgentId::new();
        memory
            .put(
                agent,
                MemoryWrite {
                    key: "cloud_preference".to_owned(),
                    value: json!("AWS deployment"),
                    scope: MemoryScope::Persistent,
                    ttl_ms: None,
                },
            )
            .await
            .unwrap();
        memory
            .put(
                agent,
                MemoryWrite {
                    key: "favorite_color".to_owned(),
                    value: json!("blue"),
                    scope: MemoryScope::Persistent,
                    ttl_ms: None,
                },
            )
            .await
            .unwrap();

        let results = memory.search(agent, "AWS cloud", None, 1).await.unwrap();
        assert_eq!(results[0].record.key, "cloud_preference");
    }

    #[tokio::test]
    async fn redis_and_postgres_backends_when_configured() {
        let (Ok(redis_url), Ok(postgres_url)) = (
            env::var("AGENT_RUNTIME_TEST_REDIS_URL"),
            env::var("AGENT_RUNTIME_TEST_DATABASE_URL"),
        ) else {
            return;
        };
        let memory = MemoryManager::from_config(MemoryConfig {
            redis_url: Some(redis_url),
            postgres_url: Some(postgres_url),
            postgres_max_connections: 1,
        })
        .await
        .unwrap();
        let agent = AgentId::new();

        memory
            .put(
                agent,
                MemoryWrite {
                    key: "active_plan".to_owned(),
                    value: json!("Redis working state"),
                    scope: MemoryScope::Working,
                    ttl_ms: Some(60_000),
                },
            )
            .await
            .unwrap();
        memory
            .put(
                agent,
                MemoryWrite {
                    key: "preference".to_owned(),
                    value: json!("PostgreSQL durable state"),
                    scope: MemoryScope::Persistent,
                    ttl_ms: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            memory
                .get(agent, MemoryScope::Working, "active_plan")
                .await
                .unwrap()
                .unwrap()
                .value,
            json!("Redis working state")
        );
        assert_eq!(
            memory
                .get(agent, MemoryScope::Persistent, "preference")
                .await
                .unwrap()
                .unwrap()
                .value,
            json!("PostgreSQL durable state")
        );
        assert_eq!(
            memory
                .search(
                    agent,
                    "durable PostgreSQL",
                    Some(MemoryScope::Persistent),
                    1
                )
                .await
                .unwrap()[0]
                .record
                .key,
            "preference"
        );
    }
}
