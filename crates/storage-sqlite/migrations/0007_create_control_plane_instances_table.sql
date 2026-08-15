CREATE TABLE IF NOT EXISTS control_plane_instances (
  id TEXT PRIMARY KEY NOT NULL,
  grpc_address TEXT NOT NULL,
  last_heartbeat TEXT NOT NULL
);
