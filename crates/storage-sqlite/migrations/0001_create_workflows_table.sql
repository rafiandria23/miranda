CREATE TABLE IF NOT EXISTS workflows (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_versions (
  id TEXT PRIMARY KEY NOT NULL,
  workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  definition TEXT NOT NULL,
  UNIQUE (workflow_id, version)
);

CREATE INDEX IF NOT EXISTS idx_workflow_versions_workflow_id ON workflow_versions (workflow_id);

CREATE TABLE IF NOT EXISTS workflow_executions (
  id TEXT PRIMARY KEY NOT NULL,
  workflow_version_id TEXT NOT NULL REFERENCES workflow_versions(id),
  status TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions (status);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_version ON workflow_executions (workflow_version_id);
