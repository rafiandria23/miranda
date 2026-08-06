#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub path: String,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "miranda.sqlite".to_owned(),
        }
    }
}
