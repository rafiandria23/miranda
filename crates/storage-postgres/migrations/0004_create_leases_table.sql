CREATE TABLE IF NOT EXISTS leases (
  token TEXT PRIMARY KEY,
  execution_id UUID NOT NULL,
  workflow_task_id UUID NOT NULL,
  worker_id UUID NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_worker_id
ON leases(worker_id);
