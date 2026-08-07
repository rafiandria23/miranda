#[derive(Debug, Clone)]
pub struct SqliteConfig {
    pub path: String,
}

impl SqliteConfig {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: "miranda.sqlite".to_owned(),
        }
    }
}
