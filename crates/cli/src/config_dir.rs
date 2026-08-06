use std::{
    fs, io,
    path::{Path, PathBuf},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigDirError {
    #[error("could not determine home directory")]
    NoHomeDir,

    #[error("failed to create config directory at {path}: {source}")]
    CreateFailed { path: PathBuf, source: io::Error },
}

pub fn resolve() -> Result<PathBuf, ConfigDirError> {
    let home = dirs::home_dir().ok_or(ConfigDirError::NoHomeDir)?;
    let config_dir = home.join(".miranda");

    ensure_exists(&config_dir)?;

    Ok(config_dir)
}

fn ensure_exists(path: &Path) -> Result<(), ConfigDirError> {
    if !path.exists() {
        fs::create_dir_all(path).map_err(|source| ConfigDirError::CreateFailed {
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}
