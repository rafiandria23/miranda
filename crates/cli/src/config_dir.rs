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

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use std::{
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static HOME_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn unique_temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        std::env::temp_dir().join(format!("miranda-{label}-{}-{nanos}", std::process::id()))
    }

    fn with_temp_home<F: FnOnce(&Path)>(f: F) {
        let _guard = HOME_ENV_LOCK.lock().unwrap();

        let original_home = std::env::var_os("HOME");
        let temp_home = unique_temp_path("config-dir-test");
        fs::create_dir_all(&temp_home).unwrap();

        unsafe {
            std::env::set_var("HOME", &temp_home);
        }

        f(&temp_home);

        match original_home {
            Some(value) => unsafe { std::env::set_var("HOME", value) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        fs::remove_dir_all(&temp_home).ok();
    }

    #[test]
    fn resolve_creates_the_config_directory_when_missing() {
        with_temp_home(|temp_home| {
            let config_dir = resolve().expect("resolve should succeed");

            assert_eq!(config_dir, temp_home.join(".miranda"));
            assert!(config_dir.is_dir());
        });
    }

    #[test]
    fn resolve_is_idempotent_when_the_config_directory_already_exists() {
        with_temp_home(|_temp_home| {
            let first = resolve().expect("first resolve should succeed");
            let second = resolve().expect("second resolve should succeed");

            assert_eq!(first, second);
            assert!(second.is_dir());
        });
    }

    #[test]
    fn ensure_exists_creates_a_missing_directory() {
        let root = unique_temp_path("ensure-exists-missing");
        let target = root.join("nested").join(".miranda");
        assert!(!target.exists());

        let result = ensure_exists(&target);

        assert!(result.is_ok());
        assert!(target.is_dir());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn ensure_exists_is_a_noop_when_the_directory_already_exists() {
        let target = unique_temp_path("ensure-exists-noop");
        fs::create_dir_all(&target).unwrap();

        let result = ensure_exists(&target);

        assert!(result.is_ok());
        assert!(target.is_dir());

        fs::remove_dir_all(&target).ok();
    }

    #[test]
    fn ensure_exists_treats_an_existing_file_as_already_present() {
        let target = unique_temp_path("ensure-exists-file");
        fs::write(&target, b"not a directory").unwrap();

        let result = ensure_exists(&target);

        assert!(result.is_ok());
        assert!(target.is_file());

        fs::remove_file(&target).ok();
    }
}
