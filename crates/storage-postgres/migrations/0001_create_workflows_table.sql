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
