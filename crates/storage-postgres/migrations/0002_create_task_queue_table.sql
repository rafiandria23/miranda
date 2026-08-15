CREATE TABLE IF NOT EXISTS task_queue (
  id UUID PRIMARY KEY,
  execution_id UUID NOT NULL,
  workflow_task_id UUID NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_task_queue_enqueued_at
ON task_queue(enqueued_at);
