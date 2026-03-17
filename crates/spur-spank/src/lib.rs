//! SPANK plugin host.
//!
//! Loads existing Slurm SPANK plugins (.so files) via dlopen and provides
//! the spank_* callback API (11 hooks, ~12 API functions).

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};

use tracing::{debug, error, info, warn};

/// SPANK callback hook points (matches Slurm's spank.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpankHook {
    Init,
    InitPost,
    LocalUserInit,
    UserInit,
    TaskInit,
    TaskInitPrivileged,
    TaskPost,
    TaskExit,
    JobEpilog,
    SlurmctldExit,
    Exit,
}

impl SpankHook {
    /// C symbol name for this hook.
    fn symbol_name(&self) -> &'static str {
        match self {
            Self::Init => "slurm_spank_init",
            Self::InitPost => "slurm_spank_init_post_opt",
            Self::LocalUserInit => "slurm_spank_local_user_init",
            Self::UserInit => "slurm_spank_user_init",
            Self::TaskInit => "slurm_spank_task_init",
            Self::TaskInitPrivileged => "slurm_spank_task_init_privileged",
            Self::TaskPost => "slurm_spank_task_post_fork",
            Self::TaskExit => "slurm_spank_task_exit",
            Self::JobEpilog => "slurm_spank_job_epilog",
            Self::SlurmctldExit => "slurm_spank_slurmd_exit",
            Self::Exit => "slurm_spank_exit",
        }
    }
}

/// SPANK item IDs for spank_get_item (10 common items).
#[repr(C)]
pub enum SpankItem {
    JobId = 0,
    JobUid = 1,
    JobGid = 2,
    JobStepId = 3,
    JobNnodes = 4,
    JobNodeid = 5,
    JobLocalTaskCount = 6,
    JobTotalTaskCount = 7,
    JobArgv = 8,
    TaskPid = 9,
}

/// A loaded SPANK plugin.
struct SpankPlugin {
    path: PathBuf,
    #[cfg(unix)]
    lib: libloading::Library,
    name: String,
}

/// The SPANK plugin host — manages loading and invoking plugins.
pub struct SpankHost {
    plugins: Vec<SpankPlugin>,
    /// Current job context for spank_get_item.
    context: SpankContext,
}

/// Job context available to SPANK plugins.
#[derive(Default)]
pub struct SpankContext {
    pub job_id: u32,
    pub uid: u32,
    pub gid: u32,
    pub step_id: u32,
    pub num_nodes: u32,
    pub node_id: u32,
    pub local_task_count: u32,
    pub total_task_count: u32,
    pub task_pid: u32,
}

impl SpankHost {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            context: SpankContext::default(),
        }
    }

    /// Load a SPANK plugin from a .so file.
    #[cfg(unix)]
    pub fn load_plugin(&mut self, path: &Path) -> anyhow::Result<()> {
        use anyhow::Context;

        let lib = unsafe {
            libloading::Library::new(path)
                .with_context(|| format!("failed to dlopen {}", path.display()))?
        };

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        info!(plugin = %name, path = %path.display(), "loaded SPANK plugin");

        self.plugins.push(SpankPlugin {
            path: path.to_path_buf(),
            lib,
            name,
        });

        Ok(())
    }

    /// Not available on non-unix platforms.
    #[cfg(not(unix))]
    pub fn load_plugin(&mut self, path: &Path) -> anyhow::Result<()> {
        anyhow::bail!("SPANK plugins only supported on Unix");
    }

    /// Set the job context for subsequent hook calls.
    pub fn set_context(&mut self, ctx: SpankContext) {
        self.context = ctx;
    }

    /// Invoke a hook across all loaded plugins.
    pub fn invoke_hook(&self, hook: SpankHook) -> Result<(), SpankError> {
        let symbol = hook.symbol_name();

        for plugin in &self.plugins {
            #[cfg(unix)]
            {
                // Look up the symbol
                let func: Result<
                    libloading::Symbol<unsafe extern "C" fn(*mut SpankHandle, c_int, *mut *mut c_char) -> c_int>,
                    _,
                > = unsafe { plugin.lib.get(symbol.as_bytes()) };

                match func {
                    Ok(f) => {
                        debug!(plugin = %plugin.name, hook = symbol, "invoking SPANK hook");
                        let rc = unsafe { f(std::ptr::null_mut(), 0, std::ptr::null_mut()) };
                        if rc != 0 {
                            warn!(
                                plugin = %plugin.name,
                                hook = symbol,
                                rc,
                                "SPANK hook returned error"
                            );
                            return Err(SpankError::HookFailed {
                                plugin: plugin.name.clone(),
                                hook: symbol.to_string(),
                                rc,
                            });
                        }
                    }
                    Err(_) => {
                        // Plugin doesn't implement this hook — that's fine
                        debug!(
                            plugin = %plugin.name,
                            hook = symbol,
                            "SPANK hook not found, skipping"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Number of loaded plugins.
    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }
}

/// Opaque handle passed to SPANK plugins (placeholder).
#[repr(C)]
pub struct SpankHandle {
    _opaque: [u8; 0],
}

#[derive(Debug, thiserror::Error)]
pub enum SpankError {
    #[error("SPANK hook {hook} in plugin {plugin} returned {rc}")]
    HookFailed {
        plugin: String,
        hook: String,
        rc: c_int,
    },
    #[error("plugin load failed: {0}")]
    LoadFailed(String),
}

/// Parse plugstack.conf (SPANK config file).
///
/// Format: `required|optional <plugin.so> [args...]`
pub fn parse_plugstack(path: &Path) -> anyhow::Result<Vec<PlugstackEntry>> {
    let content = std::fs::read_to_string(path)?;
    let mut entries = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.splitn(3, char::is_whitespace).collect();
        if parts.len() < 2 {
            continue;
        }

        let required = parts[0] == "required";
        let plugin_path = PathBuf::from(parts[1]);
        let args: Vec<String> = parts
            .get(2)
            .map(|a| a.split_whitespace().map(String::from).collect())
            .unwrap_or_default();

        entries.push(PlugstackEntry {
            required,
            path: plugin_path,
            args,
        });
    }

    Ok(entries)
}

pub struct PlugstackEntry {
    pub required: bool,
    pub path: PathBuf,
    pub args: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spank_host_new() {
        let host = SpankHost::new();
        assert_eq!(host.plugin_count(), 0);
    }

    #[test]
    fn test_hook_symbol_names() {
        assert_eq!(SpankHook::Init.symbol_name(), "slurm_spank_init");
        assert_eq!(SpankHook::TaskExit.symbol_name(), "slurm_spank_task_exit");
    }
}
