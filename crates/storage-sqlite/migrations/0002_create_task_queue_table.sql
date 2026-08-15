CREATE TABLE IF NOT EXISTS task_queue (
  id TEXT PRIMARY KEY NOT NULL,
  execution_id TEXT NOT NULL,
  workflow_task_id TEXT NOT NULL,
  enqueued_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_task_queue_enqueued_at ON task_queue (enqueued_at);
