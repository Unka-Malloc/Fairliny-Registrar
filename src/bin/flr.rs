//! `flr` — serve declaratively-registered tool services over authenticated
//! MCP, bridge callers to them over stdio, and control the local lifecycle.
//!
//! ```text
//! flr serve --config <services.toml> [--home <dir>]
//! flr service start --config <services.toml> [--home <dir>]
//! flr service stop|status [--home <dir>]
//! flr service reload --config <services.toml> [--home <dir>]
//! flr connector --caller <provider> [--home <dir>]
//! ```
//!
//! `serve` is the supervised entry: it checks the approved-payload consent,
//! claims the OS lease under the state root, publishes per-caller bearer
//! discovery and serves the union catalog on numeric loopback. `service
//! start` spawns that same entry detached. `connector` is the thin stdio
//! bridge an MCP caller process runs. The state root resolves as `--home`,
//! then `FLR_HOME`, then the platform user-data directory.

use fairliny_registrar::{connector, lifecycle, private_state};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn usage() {
    eprintln!(
        "usage:\n  \
         flr serve --config <services.toml> [--home <dir>]\n  \
         flr service start --config <services.toml> [--home <dir>]\n  \
         flr service stop|status [--home <dir>]\n  \
         flr service reload --config <services.toml> [--home <dir>]\n  \
         flr connector --caller <provider> [--home <dir>]"
    );
}

/// The `--config`/`--home` flag pairs one verb admits.
struct Flags {
    config: Option<PathBuf>,
    home: Option<PathBuf>,
}

fn parse_flags(args: &[String]) -> Result<Flags, ()> {
    let mut flags = Flags {
        config: None,
        home: None,
    };
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" if index + 1 < args.len() && flags.config.is_none() => {
                flags.config = Some(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            "--home" if index + 1 < args.len() && flags.home.is_none() => {
                flags.home = Some(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            _ => return Err(()),
        }
    }
    Ok(flags)
}

fn install_home(home: Option<PathBuf>) {
    if let Some(home) = home {
        private_state::set_state_root_override(home);
    }
}

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let Some(verb) = args.first() else {
        usage();
        return ExitCode::FAILURE;
    };
    match verb.as_str() {
        "serve" => {
            let Ok(flags) = parse_flags(&args[1..]) else {
                usage();
                return ExitCode::FAILURE;
            };
            let Some(config) = flags.config else {
                usage();
                return ExitCode::FAILURE;
            };
            install_home(flags.home);
            match lifecycle::serve(&config) {
                Ok(result) => {
                    println!("{result}");
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("flr: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        "service" => {
            let Some(action) = args.get(1) else {
                usage();
                return ExitCode::FAILURE;
            };
            let Ok(flags) = parse_flags(&args[2..]) else {
                usage();
                return ExitCode::FAILURE;
            };
            install_home(flags.home);
            match lifecycle::execute(action, flags.config.as_deref()) {
                Ok(result) => {
                    println!("{result}");
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("flr: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        "connector" => connector::main(&args[1..]),
        _ => {
            usage();
            ExitCode::FAILURE
        }
    }
}
