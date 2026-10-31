//! Local transport credentials. No runtime content enters this directory.
//!
//! The state root resolves once per process: an explicit override installed by
//! the embedding host (`--home` / [`set_state_root_override`]), then the
//! `MCPD_HOME` environment variable, then the platform user-data directory.
//! All custody primitives — owner-only directories, link rejection, bounded
//! reads, atomic publishes — are the lifted contract unchanged.

use anyhow::{Result, anyhow};
use std::{
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::RwLock,
};

/// The environment variable naming the state root when no explicit override
/// is installed.
pub const STATE_ROOT_ENV: &str = "MCPD_HOME";

static STATE_ROOT_OVERRIDE: RwLock<Option<PathBuf>> = RwLock::new(None);

/// Install the process-local state-root override. The serving entry installs
/// it once before any path resolves; lifecycle forwards it to the child it
/// spawns as `--home` so both processes resolve the same root.
pub fn set_state_root_override(path: PathBuf) {
    if let Ok(mut guard) = STATE_ROOT_OVERRIDE.write() {
        *guard = Some(path);
    }
}

/// The installed override, if any — the lifecycle spawner forwards it.
pub fn state_root_override() -> Option<PathBuf> {
    STATE_ROOT_OVERRIDE
        .read()
        .ok()
        .and_then(|guard| guard.clone())
}

fn resolved_state_root() -> Result<PathBuf> {
    if let Some(override_path) = state_root_override() {
        return Ok(override_path);
    }
    if let Some(path) = env::var_os(STATE_ROOT_ENV)
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path));
    }
    platform_default_state_root()
}

/// The platform user-data directory for the dispatcher when neither an
/// override nor `MCPD_HOME` names one.
fn platform_default_state_root() -> Result<PathBuf> {
    platform_default_root().ok_or_else(|| anyhow!("mcp_state_unavailable"))
}

#[cfg(target_os = "macos")]
fn platform_default_root() -> Option<PathBuf> {
    env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("mcpd")
    })
}

#[cfg(target_os = "windows")]
fn platform_default_root() -> Option<PathBuf> {
    ["APPDATA", "LOCALAPPDATA"]
        .iter()
        .copied()
        .find_map(env::var_os)
        .map(|root| PathBuf::from(root).join("mcpd"))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn platform_default_root() -> Option<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_DATA_HOME") {
        let xdg = PathBuf::from(xdg);
        if xdg.is_absolute() {
            return Some(xdg.join("mcpd"));
        }
    }
    env::var_os("HOME").map(|home| {
        PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("mcpd")
    })
}

#[cfg(not(any(unix, target_os = "windows")))]
fn platform_default_root() -> Option<PathBuf> {
    None
}

/// The writer path: the resolved state root, canonicalized when it already
/// exists. Creating and hardening it belongs to `ensure_private_dir`.
pub fn state_root() -> Result<PathBuf> {
    let path = resolved_state_root().map_err(|_| anyhow!("mcp_state_unavailable"))?;
    Ok(path.canonicalize().unwrap_or(path))
}

/// The reader path: resolved lexically and never creating anything, so a
/// refused connector can report why without leaving directories behind.
pub fn state_root_read_only() -> Result<PathBuf> {
    resolved_state_root().map_err(|_| anyhow!("mcp_state_unavailable"))
}

fn reject_links(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(anyhow!("mcp_state_unsafe"));
            }
            Ok(metadata) => {
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    if metadata.file_attributes() & 0x400 != 0 {
                        return Err(anyhow!("mcp_state_unsafe"));
                    }
                }
                #[cfg(not(windows))]
                let _ = metadata;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(anyhow!("mcp_state_unavailable")),
        }
    }
    Ok(())
}
#[cfg(unix)]
fn private_metadata(path: &Path, directory: bool) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || (if directory {
            !metadata.is_dir()
        } else {
            !metadata.is_file()
        })
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(anyhow!("mcp_state_unsafe"));
    }
    Ok(())
}
#[cfg(windows)]
fn private_metadata(path: &Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || metadata.is_dir() != directory {
        return Err(anyhow!("mcp_state_unsafe"));
    }
    crate::windows_private_state::validate(path)
}
pub fn ensure_private_dir(path: &Path) -> Result<()> {
    reject_links(path)?;
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(path)?;
        }
        #[cfg(windows)]
        fs::create_dir(path)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if fs::symlink_metadata(path)?.uid() != unsafe { libc::geteuid() } {
            return Err(anyhow!("mcp_state_unsafe"));
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(windows)]
    crate::windows_private_state::harden(path)?;
    private_metadata(path, true)
}
pub fn read_existing_private_text_bounded(path: &Path, limit: usize) -> Result<Option<String>> {
    reject_links(path)?;
    if !path.exists() {
        return Ok(None);
    }
    private_metadata(path, false)?;
    if let Some(parent) = path.parent() {
        private_metadata(parent, true)?;
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        let named = fs::symlink_metadata(path)?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || metadata.dev() != named.dev()
            || metadata.ino() != named.ino()
        {
            return Err(anyhow!("mcp_state_unsafe"));
        }
    }
    let mut text = String::new();
    file.take(limit as u64 + 1).read_to_string(&mut text)?;
    if text.len() > limit {
        return Err(anyhow!("mcp_state_invalid"));
    }
    Ok(Some(text))
}
pub fn atomic_write_private_text_bounded(path: &Path, text: &str, limit: usize) -> Result<()> {
    if text.len() > limit {
        return Err(anyhow!("mcp_state_invalid"));
    }
    let parent = path.parent().ok_or_else(|| anyhow!("mcp_state_invalid"))?;
    ensure_private_dir(parent)?;
    reject_links(path)?;
    let temporary = parent.join(format!(".publish-{}", uuid::Uuid::new_v4().simple()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let result = (|| -> Result<()> {
        let mut file = options.open(&temporary)?;
        #[cfg(windows)]
        crate::windows_private_state::harden(&temporary)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::env;
    use std::os::unix::fs::{PermissionsExt, symlink};
    #[test]
    fn discovery_rejects_public_permissions_and_symlink_substitution() {
        let root = env::temp_dir().join(format!("mcp-private-fixture-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let private = root.join("private");
        ensure_private_dir(&private).unwrap();
        let file = private.join("fixture.json");
        atomic_write_private_text_bounded(&file, "synthetic", 32).unwrap();
        assert_eq!(
            read_existing_private_text_bounded(&file, 32)
                .unwrap()
                .as_deref(),
            Some("synthetic")
        );
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_existing_private_text_bounded(&file, 32).is_err());
        fs::remove_file(&file).unwrap();
        let other = root.join("other");
        fs::write(&other, "synthetic").unwrap();
        symlink(&other, &file).unwrap();
        assert!(read_existing_private_text_bounded(&file, 32).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
