CREATE TABLE IF NOT EXISTS join_tokens (
  token TEXT PRIMARY KEY NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
