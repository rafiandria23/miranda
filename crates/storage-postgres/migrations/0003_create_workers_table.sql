CREATE TABLE IF NOT EXISTS workers (
  id UUID PRIMARY KEY,
  capabilities TEXT[] NOT NULL,
  last_heartbeat TIMESTAMPTZ NOT NULL
);
