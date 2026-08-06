use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub url: String,
    pub min_connections: u32,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl MySqlConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            min_connections: 0,
            max_connections: 24,
            acquire_timeout: Duration::from_secs(6),
            idle_timeout: Duration::from_secs(360),
            max_lifetime: Duration::from_secs(3600),
        }
    }

    pub fn with_min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = min_connections;
        self
    }

    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn with_max_lifetime(mut self, max_lifetime: Duration) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    pub fn admin_url(&self) -> Result<String, url::ParseError> {
        let mut parsed = Url::parse(&self.url)?;

        parsed.set_path("/mysql");

        Ok(parsed.into())
    }

    pub fn database_name(&self) -> Result<String, url::ParseError> {
        let parsed = Url::parse(&self.url)?;

        Ok(parsed.path().trim_start_matches('/').to_owned())
    }
}
