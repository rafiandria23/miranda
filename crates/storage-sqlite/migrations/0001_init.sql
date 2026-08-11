CREATE TABLE workflows (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE workflow_versions (
    id TEXT PRIMARY KEY NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    definition TEXT NOT NULL,
    UNIQUE (workflow_id, version)
);

CREATE INDEX idx_workflow_versions_workflow_id ON workflow_versions (workflow_id);

CREATE TABLE workflow_executions (
    id TEXT PRIMARY KEY NOT NULL,
    workflow_version_id TEXT NOT NULL REFERENCES workflow_versions(id),
    status TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL
);

CREATE INDEX idx_workflow_executions_status ON workflow_executions (status);
CREATE INDEX idx_workflow_executions_version ON workflow_executions (workflow_version_id);

CREATE TABLE task_queue (
    id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL,
    workflow_task_id TEXT NOT NULL,
    enqueued_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_task_queue_enqueued_at ON task_queue (enqueued_at);

CREATE TABLE workers (
    id TEXT PRIMARY KEY NOT NULL,
    capabilities TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL
);
