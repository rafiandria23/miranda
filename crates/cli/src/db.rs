use miranda_storage_mysql::{MySqlConfig, MySqlStore};
use miranda_storage_postgres::{PostgresConfig, PostgresStore};
use std::error::Error;

pub async fn create(database_url: &str) -> Result<(), Box<dyn Error>> {
    if database_url.starts_with("mysql://") {
        let store = MySqlStore::connect(MySqlConfig::new(database_url)).await?;

        drop(store);

        println!("database ready (created if missing, migrated)");

        Ok(())
    } else if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        let store = PostgresStore::connect(PostgresConfig::new(database_url)).await?;

        drop(store);

        println!("database ready (created if missing, migrated)");

        Ok(())
    } else {
        Err(format!("unsupported or unrecognized database URL scheme: {database_url}").into())
    }
}

pub async fn migrate(database_url: &str) -> Result<(), Box<dyn Error>> {
    create(database_url).await
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_rejects_a_sqlite_url() {
        let result = create("sqlite://miranda.sqlite").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_rejects_an_unrecognized_scheme() {
        let result = create("redis://localhost:6379").await;

        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported or unrecognized database URL scheme")
        );
    }

    #[tokio::test]
    async fn create_rejects_a_malformed_url() {
        let result = create("not a url").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_fails_to_connect_to_an_unreachable_mysql_host() {
        let result = create("mysql://user:pass@127.0.0.1:1/miranda").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_fails_to_connect_to_an_unreachable_postgres_host() {
        let result = create("postgres://user:pass@127.0.0.1:1/miranda").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_accepts_the_postgresql_scheme_alias() {
        let result = create("postgresql://user:pass@127.0.0.1:1/miranda").await;

        assert!(result.is_err());
        assert!(
            !result
                .unwrap_err()
                .to_string()
                .contains("unsupported or unrecognized database URL scheme")
        );
    }

    #[tokio::test]
    async fn migrate_delegates_to_create() {
        let result = migrate("redis://localhost:6379").await;

        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported or unrecognized database URL scheme")
        );
    }
}
