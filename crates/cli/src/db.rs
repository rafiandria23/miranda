use miranda_storage_mysql::{MySqlConfig, MySqlStore};
use miranda_storage_postgres::{PostgresConfig, PostgresStore};
use std::error::Error;

pub async fn create(database_url: &str) -> Result<(), Box<dyn Error>> {
    if database_url.starts_with("postgres://") || database_url.starts_with("postgresql://") {
        let store = PostgresStore::connect(PostgresConfig::new(database_url)).await?;

        drop(store);

        println!("database ready (created if missing, migrated)");

        Ok(())
    } else if database_url.starts_with("mysql://") {
        let store = MySqlStore::connect(MySqlConfig::new(database_url)).await?;

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
