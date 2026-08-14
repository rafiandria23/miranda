CREATE TABLE IF NOT EXISTS workers (
  id TEXT PRIMARY KEY NOT NULL,
  capabilities TEXT NOT NULL,
  last_heartbeat TEXT NOT NULL
);
