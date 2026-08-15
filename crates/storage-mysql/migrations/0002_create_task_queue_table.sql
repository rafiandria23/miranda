CREATE TABLE IF NOT EXISTS task_queue (
  id CHAR(36) PRIMARY KEY,
  execution_id CHAR(36) NOT NULL,
  workflow_task_id CHAR(36) NOT NULL,
  enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_task_queue_enqueued_at ON task_queue (enqueued_at);
