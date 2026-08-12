CREATE TABLE IF NOT EXISTS workflows (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_versions (
  id UUID PRIMARY KEY,
  workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  version BIGINT NOT NULL,
  definition JSONB NOT NULL,
  UNIQUE(workflow_id, version)
);

CREATE TABLE IF NOT EXISTS workflow_executions (
  id UUID PRIMARY KEY,
  workflow_version_id UUID NOT NULL REFERENCES workflow_versions(id),
  status TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  state JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_executions_active
ON workflow_executions(status)
WHERE status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS idx_workflow_executions_version
ON workflow_executions(workflow_version_id);

CREATE TABLE IF NOT EXISTS task_queue (
  id UUID PRIMARY KEY,
  execution_id UUID NOT NULL,
  workflow_task_id UUID NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_task_queue_enqueued_at
ON task_queue(enqueued_at);

CREATE TABLE IF NOT EXISTS workers (
  id UUID PRIMARY KEY,
  capabilities TEXT[] NOT NULL,
  last_heartbeat TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
  token TEXT PRIMARY KEY,
  execution_id UUID NOT NULL,
  workflow_task_id UUID NOT NULL,
  worker_id UUID NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_worker_id
ON leases(worker_id);
