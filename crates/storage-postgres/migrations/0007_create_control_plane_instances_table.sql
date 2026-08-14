CREATE TABLE IF NOT EXISTS control_plane_instances (
  id TEXT PRIMARY KEY,
  grpc_address TEXT NOT NULL,
  last_heartbeat TIMESTAMPTZ NOT NULL
);
