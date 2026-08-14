CREATE TABLE IF NOT EXISTS control_plane_instances (
  id VARCHAR(64) PRIMARY KEY,
  grpc_address TEXT NOT NULL,
  last_heartbeat TIMESTAMP NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
