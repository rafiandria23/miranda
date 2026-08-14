CREATE TABLE IF NOT EXISTS leases (
  token VARCHAR(255) PRIMARY KEY,
  execution_id CHAR(36) NOT NULL,
  workflow_task_id CHAR(36) NOT NULL,
  worker_id CHAR(36) NOT NULL,
  expires_at TIMESTAMP NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_leases_worker_id ON leases (worker_id);
