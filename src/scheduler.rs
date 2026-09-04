use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
};

use tokio::sync::{Mutex, Notify};

use crate::{
    error::{Result, RuntimeError},
    task::{Task, TaskId, TaskPriority},
};

pub type SharedTask = Arc<Mutex<Task>>;

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    queue: Mutex<BinaryHeap<QueuedTask>>,
    notify: Notify,
    sequence: AtomicU64,
    shutdown: AtomicBool,
}

struct QueuedTask {
    task: SharedTask,
    task_id: TaskId,
    priority: TaskPriority,
    sequence: u64,
}

impl PartialEq for QueuedTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for QueuedTask {}

impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority).then_with(|| {
            // Smaller sequence means earlier submission.
            // Reverse comparison gives FIFO ordering
            // inside BinaryHeap.
            other.sequence.cmp(&self.sequence)
        })
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                queue: Mutex::new(BinaryHeap::new()),
                notify: Notify::new(),
                sequence: AtomicU64::new(0),
                shutdown: AtomicBool::new(false),
            }),
        }
    }

    pub async fn submit(&self, mut task: Task) -> Result<SharedTask> {
        if self.inner.shutdown.load(AtomicOrdering::SeqCst) {
            return Err(RuntimeError::SchedulerShutdown);
        }

        task.queue()?;

        let priority = task.priority;
        let task_id = task.id;

        let task = Arc::new(Mutex::new(task));

        let sequence = self.inner.sequence.fetch_add(1, AtomicOrdering::SeqCst);

        {
            let mut queue = self.inner.queue.lock().await;

            queue.push(QueuedTask {
                task: task.clone(),
                task_id,
                priority,
                sequence,
            });
        }

        tracing::debug!(
            sequence,
            priority = ?priority,
            "Task submitted to scheduler"
        );

        self.inner.notify.notify_one();

        Ok(task)
    }

    pub async fn next_task(&self) -> Option<SharedTask> {
        loop {
            if self.inner.shutdown.load(AtomicOrdering::SeqCst) {
                return None;
            }

            let notified = self.inner.notify.notified();

            {
                let mut queue = self.inner.queue.lock().await;

                if let Some(item) = queue.pop() {
                    return Some(item.task);
                }
            }

            notified.await;
        }
    }

    pub async fn queue_len(&self) -> usize {
        self.inner.queue.lock().await.len()
    }

    pub async fn cancel(&self, task_id: TaskId) -> bool {
        let mut queue = self.inner.queue.lock().await;
        let original_len = queue.len();

        queue.retain(|queued| queued.task_id != task_id);

        queue.len() != original_len
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, AtomicOrdering::SeqCst);

        self.inner.notify.notify_waiters();

        tracing::info!("Scheduler shutting down");
    }

    pub fn is_shutdown(&self) -> bool {
        self.inner.shutdown.load(AtomicOrdering::SeqCst)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde_json::json;

    fn task(name: &str, priority: TaskPriority) -> Task {
        Task::new(name, json!({}), priority)
    }

    #[tokio::test]
    async fn higher_priority_is_returned_first() {
        let scheduler = Scheduler::new();

        scheduler
            .submit(task("low", TaskPriority::Low))
            .await
            .unwrap();

        scheduler
            .submit(task("critical", TaskPriority::Critical))
            .await
            .unwrap();

        scheduler
            .submit(task("high", TaskPriority::High))
            .await
            .unwrap();

        let first = scheduler.next_task().await.unwrap();

        let first = first.lock().await;

        assert_eq!(first.name, "critical");
    }

    #[tokio::test]
    async fn equal_priority_is_fifo() {
        let scheduler = Scheduler::new();

        scheduler
            .submit(task("first", TaskPriority::Normal))
            .await
            .unwrap();

        scheduler
            .submit(task("second", TaskPriority::Normal))
            .await
            .unwrap();

        scheduler
            .submit(task("third", TaskPriority::Normal))
            .await
            .unwrap();

        let first = scheduler.next_task().await.unwrap();
        let second = scheduler.next_task().await.unwrap();
        let third = scheduler.next_task().await.unwrap();

        assert_eq!(first.lock().await.name, "first");

        assert_eq!(second.lock().await.name, "second");

        assert_eq!(third.lock().await.name, "third");
    }

    #[tokio::test]
    async fn cancellation_removes_queued_task() {
        let scheduler = Scheduler::new();
        let task = scheduler
            .submit(task("cancel-me", TaskPriority::Normal))
            .await
            .unwrap();
        let task_id = task.lock().await.id;

        assert!(scheduler.cancel(task_id).await);
        assert_eq!(scheduler.queue_len().await, 0);
    }
}
