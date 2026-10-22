//! talos-pilot: A terminal UI for managing Talos Linux clusters

use clap::Parser;
use color_eyre::Result;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;
use talos_pilot_tui::App;
use tracing::Level;
use tracing_subscriber::{EnvFilter, prelude::*};

/// talos-pilot: Terminal UI for Talos Linux clusters
#[derive(Parser, Debug)]
#[command(name = "talos-pilot")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Talos context to use (from talosconfig)
    #[arg(short, long)]
    context: Option<String>,

    /// Path to talosconfig file
    #[arg(long)]
    config: Option<String>,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,

    /// Log file path (default: <temp_dir>/talos-pilot-<pid>.log)
    #[arg(long)]
    log_file: Option<String>,

    /// Number of log lines to fetch (default: 500)
    #[arg(short, long, default_value = "500")]
    tail: i32,

    /// Connect without TLS client certificates (for maintenance mode nodes)
    #[arg(short, long)]
    insecure: bool,

    /// Endpoint to connect to in insecure mode (e.g., 192.168.1.100 or 192.168.1.100:50000)
    #[arg(short, long, requires = "insecure")]
    endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let cli = Cli::parse();

    // Initialize error handling
    color_eyre::install()?;

    // Initialize logging to file (not stdout, which would corrupt TUI)
    let log_file = match cli.log_file {
        Some(path) => File::create(path)?,
        None => {
            prune_old_logs(&std::env::temp_dir(), LOG_MAX_AGE);
            create_private_file(&resolve_log_path(None))?
        }
    };

    // Build filter: set base level, but quiet down noisy HTTP/gRPC libraries
    let filter = if cli.debug {
        EnvFilter::from_default_env()
            .add_directive(Level::DEBUG.into())
            .add_directive("h2=info".parse().unwrap())
            .add_directive("hyper=info".parse().unwrap())
            .add_directive("tower=info".parse().unwrap())
            .add_directive("tonic=info".parse().unwrap())
            .add_directive("rustls=info".parse().unwrap())
    } else {
        EnvFilter::from_default_env().add_directive(Level::INFO.into())
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(log_file)
                .with_ansi(true)
                .with_target(false),
        )
        .with(filter)
        .init();

    tracing::info!("Starting talos-pilot");

    // Validate insecure mode requires endpoint
    if cli.insecure && cli.endpoint.is_none() {
        eprintln!("Error: --insecure requires --endpoint <ip>");
        eprintln!("Usage: talos-pilot --insecure --endpoint 192.168.1.100");
        std::process::exit(1);
    }

    if cli.insecure {
        tracing::info!("Insecure mode enabled");
        if let Some(ep) = &cli.endpoint {
            tracing::info!("Endpoint: {}", ep);
        }
    } else {
        if let Some(ctx) = &cli.context {
            tracing::info!("Using context: {}", ctx);
        }
        if let Some(cfg) = &cli.config {
            tracing::info!("Using config: {}", cfg);
        }
    }

    // Run the TUI
    let mut app = App::new(
        cli.config,
        cli.context,
        cli.tail,
        cli.insecure,
        cli.endpoint,
    );
    app.run().await?;

    tracing::info!("Goodbye!");
    Ok(())
}

/// Default logs older than this are removed at startup
const LOG_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Resolve the log file path, falling back to the platform temp directory.
///
/// The default name carries the process id, so starting another instance
/// never wipes the log of one that is still running.
fn resolve_log_path(log_file: Option<String>) -> PathBuf {
    match log_file {
        Some(path) => PathBuf::from(path),
        None => std::env::temp_dir().join(default_log_name(std::process::id())),
    }
}

fn default_log_name(pid: u32) -> String {
    format!("talos-pilot-{pid}.log")
}

/// Whether `name` is a default log name (`talos-pilot-<pid>.log`)
fn is_default_log_name(name: &str) -> bool {
    name.strip_prefix("talos-pilot-")
        .and_then(|rest| rest.strip_suffix(".log"))
        .is_some_and(|pid| !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit()))
}

/// Remove default logs in `dir` not written to for `max_age`, so per-instance
/// logs don't pile up. Only regular files are removed (never what a symlink
/// points to), and failures are ignored: another user's file in a shared
/// temp dir simply stays.
fn prune_old_logs(dir: &Path, max_age: Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.file_name().to_str().is_some_and(is_default_log_name) {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let stale = meta
            .modified()
            .ok()
            .and_then(|at| at.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if meta.is_file() && stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Create a fresh log file readable only by the current user.
///
/// The temp dir may be shared (`/tmp` on Linux), so a fixed name there could be
/// pre-created by another user as a symlink or as a file they can read. Remove
/// any previous file, then create exclusively: `create_new` (O_EXCL) never
/// follows a symlink and fails if someone re-creates the path in between.
fn create_private_file(path: &Path) -> std::io::Result<File> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_log_path_uses_temp_dir() {
        let path = resolve_log_path(None);
        let expected = std::env::temp_dir().join(format!("talos-pilot-{}.log", std::process::id()));
        assert_eq!(path, expected);
    }

    #[test]
    fn recognises_default_log_names() {
        assert!(is_default_log_name(&default_log_name(86727)));
        assert!(!is_default_log_name("talos-pilot.log"));
        assert!(!is_default_log_name("talos-pilot-.log"));
        assert!(!is_default_log_name("talos-pilot-12x.log"));
        assert!(!is_default_log_name("other-12.log"));
    }

    #[test]
    fn prunes_only_old_default_logs() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("talos-pilot-1.log");
        let fresh = dir.path().join("talos-pilot-2.log");
        let other = dir.path().join("notes-3.log");
        for path in [&old, &fresh, &other] {
            std::fs::write(path, "x").unwrap();
        }
        let week_ago = std::time::SystemTime::now() - Duration::from_secs(8 * 24 * 60 * 60);
        for path in [&old, &other] {
            File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(week_ago)
                .unwrap();
        }

        prune_old_logs(dir.path(), LOG_MAX_AGE);

        assert!(!old.exists());
        assert!(fresh.exists());
        assert!(other.exists());
    }

    #[test]
    fn default_log_path_parent_exists() {
        let path = resolve_log_path(None);
        assert!(
            path.parent().unwrap().exists(),
            "default log path parent directory does not exist: {}",
            path.display()
        );
    }

    #[test]
    fn private_file_replaces_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("talos-pilot.log");
        std::fs::write(&path, "old").unwrap();

        create_private_file(&path).unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[cfg(unix)]
    #[test]
    fn private_file_does_not_follow_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("victim");
        std::fs::write(&target, "keep").unwrap();
        let link = dir.path().join("talos-pilot.log");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        create_private_file(&link).unwrap();

        assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep");
        assert!(!std::fs::symlink_metadata(&link).unwrap().is_symlink());
    }

    #[test]
    fn explicit_log_path_is_used() {
        let custom = "/some/custom/path.log".to_string();
        let path = resolve_log_path(Some(custom.clone()));
        assert_eq!(path, PathBuf::from(custom));
    }
}
