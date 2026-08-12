CREATE TABLE IF NOT EXISTS workflows (
  id CHAR(36) PRIMARY KEY,
  name TEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workflow_versions (
  id CHAR(36) PRIMARY KEY,
  workflow_id CHAR(36) NOT NULL,
  version BIGINT NOT NULL,
  definition JSON NOT NULL,
  UNIQUE KEY uq_workflow_versions_workflow_version (workflow_id, version),
  CONSTRAINT fk_workflow_versions_workflow
    FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workflow_executions (
  id CHAR(36) PRIMARY KEY,
  workflow_version_id CHAR(36) NOT NULL,
  status VARCHAR(32) NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  state JSON NOT NULL,
  CONSTRAINT fk_workflow_executions_version
    FOREIGN KEY (workflow_version_id) REFERENCES workflow_versions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_workflow_executions_active
ON workflow_executions(status);

CREATE INDEX idx_workflow_executions_version
ON workflow_executions(workflow_version_id);

CREATE TABLE IF NOT EXISTS task_queue (
  id CHAR(36) PRIMARY KEY,
  execution_id CHAR(36) NOT NULL,
  workflow_task_id CHAR(36) NOT NULL,
  enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_task_queue_enqueued_at ON task_queue (enqueued_at);

CREATE TABLE IF NOT EXISTS workers (
  id CHAR(36) PRIMARY KEY,
  capabilities JSON NOT NULL,
  last_heartbeat TIMESTAMP NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS leases (
  token VARCHAR(255) PRIMARY KEY,
  execution_id CHAR(36) NOT NULL,
  workflow_task_id CHAR(36) NOT NULL,
  worker_id CHAR(36) NOT NULL,
  expires_at TIMESTAMP NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_leases_worker_id ON leases (worker_id);

CREATE TABLE IF NOT EXISTS join_tokens (
  token VARCHAR(255) PRIMARY KEY,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
