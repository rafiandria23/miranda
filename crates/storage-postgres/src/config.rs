use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub url: String,
    pub min_connections: u32,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl PostgresConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Self::defaults()
        }
    }

    pub fn defaults() -> Self {
        Self {
            url: String::new(),
            min_connections: 0,
            max_connections: 60,
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

        parsed.set_path("/postgres");

        Ok(parsed.into())
    }

    pub fn database_name(&self) -> Result<String, url::ParseError> {
        let parsed = Url::parse(&self.url)?;

        Ok(parsed.path().trim_start_matches("/").to_owned())
    }
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_defaults() {
        let config = PostgresConfig::new("postgres://user:pass@localhost:5432/mydb");

        assert_eq!(config.url, "postgres://user:pass@localhost:5432/mydb");
        assert_eq!(config.min_connections, 0);
        assert_eq!(config.max_connections, 60);
        assert_eq!(config.acquire_timeout, Duration::from_secs(6));
        assert_eq!(config.idle_timeout, Duration::from_secs(360));
        assert_eq!(config.max_lifetime, Duration::from_secs(3600));
    }

    #[test]
    fn defaults_has_empty_url() {
        let config = PostgresConfig::defaults();

        assert_eq!(config.url, "");
        assert_eq!(config.max_connections, 60);
    }

    #[test]
    fn with_min_connections_overrides_default() {
        let config =
            PostgresConfig::new("postgres://localhost/db").with_min_connections(5);

        assert_eq!(config.min_connections, 5);
    }

    #[test]
    fn with_max_connections_overrides_default() {
        let config =
            PostgresConfig::new("postgres://localhost/db").with_max_connections(50);

        assert_eq!(config.max_connections, 50);
    }

    #[test]
    fn with_acquire_timeout_overrides_default() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_acquire_timeout(Duration::from_secs(10));

        assert_eq!(config.acquire_timeout, Duration::from_secs(10));
    }

    #[test]
    fn with_idle_timeout_overrides_default() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_idle_timeout(Duration::from_secs(120));

        assert_eq!(config.idle_timeout, Duration::from_secs(120));
    }

    #[test]
    fn with_max_lifetime_overrides_default() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_max_lifetime(Duration::from_secs(7200));

        assert_eq!(config.max_lifetime, Duration::from_secs(7200));
    }

    #[test]
    fn builder_methods_can_be_chained() {
        let config = PostgresConfig::new("postgres://localhost/db")
            .with_min_connections(2)
            .with_max_connections(10)
            .with_acquire_timeout(Duration::from_secs(1))
            .with_idle_timeout(Duration::from_secs(2))
            .with_max_lifetime(Duration::from_secs(3));

        assert_eq!(config.min_connections, 2);
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.acquire_timeout, Duration::from_secs(1));
        assert_eq!(config.idle_timeout, Duration::from_secs(2));
        assert_eq!(config.max_lifetime, Duration::from_secs(3));
    }

    #[test]
    fn admin_url_replaces_path_with_postgres() {
        let config = PostgresConfig::new("postgres://user:pass@localhost:5432/mydb");

        let admin_url = config.admin_url().unwrap();

        assert_eq!(admin_url, "postgres://user:pass@localhost:5432/postgres");
    }

    #[test]
    fn admin_url_with_invalid_url_returns_err() {
        let config = PostgresConfig::new("not a valid url");

        assert!(config.admin_url().is_err());
    }

    #[test]
    fn database_name_extracts_path_without_leading_slash() {
        let config = PostgresConfig::new("postgres://user:pass@localhost:5432/mydb");

        assert_eq!(config.database_name().unwrap(), "mydb");
    }

    #[test]
    fn database_name_with_no_path_returns_empty_string() {
        let config = PostgresConfig::new("postgres://user:pass@localhost:5432");

        assert_eq!(config.database_name().unwrap(), "");
    }

    #[test]
    fn database_name_with_invalid_url_returns_err() {
        let config = PostgresConfig::new("not a valid url");

        assert!(config.database_name().is_err());
    }
}
