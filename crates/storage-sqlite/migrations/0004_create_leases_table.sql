CREATE TABLE IF NOT EXISTS leases (
  token TEXT PRIMARY KEY NOT NULL,
  execution_id TEXT NOT NULL,
  workflow_task_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_worker_id ON leases (worker_id);
