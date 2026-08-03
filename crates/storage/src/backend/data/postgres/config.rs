use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub url: String,
    pub min_connections: u32,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: 24,
            min_connections: 6,
            acquire_timeout: Duration::from_secs(6),
            idle_timeout: Duration::from_secs(360),
            max_lifetime: Duration::from_secs(3600),
        }
    }
}
