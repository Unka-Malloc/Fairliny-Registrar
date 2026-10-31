//! Thin stdio connector for the dispatcher's loopback HTTP service.
//!
//! This module owns no tools, catalog, repeat-attempt policy, caller
//! adaptation or conversation state. One stdio frame maps to one
//! authenticated loopback HTTP request and one response frame. The caller
//! context mapping and the no-replay tool list arrive inside the published
//! discovery document — the connector learns them, it does not carry them.

use crate::application::{MAX_MCP_FRAME_BYTES, PROTOCOL_REVISION, supports_protocol_revision};
use crate::transport::{
    ConnectorDiscoveryError, connector_close_session, connector_exchange, load_connector_discovery,
    published_callers,
};
use crate::{McpMessage, McpStdioFrame, encode_http_body, encode_stdio_line, read_stdio_frame};
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;

const MAX_IN_FLIGHT_FRAMES: usize = 32;

/// Stdout carries protocol frames only, so every startup refusal is reported
/// on stderr. Without one the client sees a bare transport-level connection
/// close and reads a refused caller as a connectivity fault.
const DIAGNOSTIC_PREFIX: &str = "mcpd";

/// The environment variable a caller may declare its provider id through,
/// instead of `--caller`.
const CALLER_ENV: &str = "MCPD_CALLER";

/// Why this process refused to start. Each cause names itself so the client
/// can act without inspecting anything else.
#[derive(Clone, Debug, Eq, PartialEq)]
enum CallerRejection {
    Undeclared,
    Conflicting,
    Unexpected,
    /// A declared provider absent from the published caller capability set.
    NotAdmitted {
        provider: String,
    },
}

impl CallerRejection {
    fn describe(&self, supported: &[String]) -> String {
        let suffix = if supported.is_empty() {
            String::new()
        } else {
            format!(" (supported: {})", supported.join(", "))
        };
        match self {
            Self::Undeclared => format!(
                "caller provider is not declared; pass --caller <provider> or set \
                 {CALLER_ENV}{suffix}"
            ),
            Self::Conflicting => {
                format!("--caller and {CALLER_ENV} disagree; declare exactly one caller provider")
            }
            Self::Unexpected => {
                "unexpected arguments; the accepted form is --caller <provider> [--home <dir>]"
                    .to_owned()
            }
            Self::NotAdmitted { provider } => {
                format!("'{provider}' is not an admitted caller{suffix}")
            }
        }
    }
}

fn report(reason: impl std::fmt::Display) {
    eprintln!("{DIAGNOSTIC_PREFIX}: {reason}");
}

pub fn main(args: &[String]) -> ExitCode {
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => ExitCode::FAILURE,
    }
}

fn run(args: &[String]) -> Result<(), ()> {
    let (caller_args, home) = split_home(args).map_err(|rejection| {
        report(rejection.describe(&published_callers()));
    })?;
    if let Some(home) = home {
        crate::private_state::set_state_root_override(home);
    }
    let provider = match caller_provider(caller_args, env::var(CALLER_ENV).ok()) {
        Ok(provider) => provider,
        Err(rejection) => {
            report(rejection.describe(&published_callers()));
            return Err(());
        }
    };
    // Missing discovery exits before the first stdio frame. The client then
    // reports EOF on `initialize`; the stderr diagnostic names the cause.
    let mut discovery = match load_connector_discovery(&provider) {
        Ok(discovery) => discovery,
        Err(ConnectorDiscoveryError::CallerNotSupported(supported)) => {
            // Membership is the published capability set, not a list compiled
            // into this binary. A provider without a seat is refused before
            // any frame reaches the loopback service.
            report(CallerRejection::NotAdmitted { provider }.describe(&supported));
            return Err(());
        }
        Err(error) => {
            report(error.code());
            return Err(());
        }
    };
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let writer = Arc::new(Mutex::new(io::stdout()));
    let mut session_id: Option<String> = None;
    let mut initialization: Option<McpMessage> = None;
    let mut protocol_revision = PROTOCOL_REVISION.to_owned();
    let mut workers = Vec::<thread::JoinHandle<Result<(), ()>>>::new();
    let result = (|| -> Result<(), ()> {
        loop {
            let mut retained = Vec::with_capacity(workers.len());
            for worker in workers.drain(..) {
                if worker.is_finished() {
                    worker.join().map_err(|_| ())??;
                } else {
                    retained.push(worker);
                }
            }
            workers = retained;
            let message = match read_stdio_frame(&mut reader, MAX_MCP_FRAME_BYTES) {
                McpStdioFrame::Eof => break,
                McpStdioFrame::Invalid(response) => {
                    writer
                        .lock()
                        .map_err(|_| ())?
                        .write_all(
                            &encode_stdio_line(&response, MAX_MCP_FRAME_BYTES).map_err(|_| ())?,
                        )
                        .map_err(|_| ())?;
                    writer.lock().map_err(|_| ())?.flush().map_err(|_| ())?;
                    continue;
                }
                McpStdioFrame::Message(message) => message,
            };
            // A service reload replaces only endpoint sessions. Renew the
            // handshake on the next frame; never replay a tool call whose
            // effect is uncertain.
            let current = match load_connector_discovery(&provider) {
                Ok(current) => current,
                Err(_) => {
                    module_unavailable(&message, &writer, &discovery, false)?;
                    continue;
                }
            };
            if current != discovery {
                discovery = current;
                session_id = None;
            }
            if session_id.is_none()
                && initialize_protocol_revision(&message).is_none()
                && let Some(initialize) = initialization.as_ref()
            {
                let body = encode_http_body(initialize, MAX_MCP_FRAME_BYTES).map_err(|_| ())?;
                match connector_exchange(&discovery, None, &protocol_revision, &body) {
                    Ok((200, Some(id), _)) => {
                        session_id = Some(id);
                    }
                    _ => {
                        module_unavailable(&message, &writer, &discovery, false)?;
                        continue;
                    }
                }
            }
            if initialize_protocol_revision(&message).is_some() {
                initialization = Some(message.clone());
            }
            if session_id.is_none()
                && let Some(requested) = initialize_protocol_revision(&message)
                && supports_protocol_revision(requested)
            {
                protocol_revision = requested.to_owned();
            }
            if let Some(session_id) = session_id.clone() {
                if workers.len() >= MAX_IN_FLIGHT_FRAMES {
                    return Err(());
                }
                let discovery = discovery.clone();
                let protocol_revision = protocol_revision.clone();
                let writer = Arc::clone(&writer);
                workers.push(thread::spawn(move || {
                    forward(
                        &discovery,
                        Some(&session_id),
                        None,
                        &protocol_revision,
                        message,
                        &writer,
                    )
                    .map(|_| ())
                }));
            } else {
                let returned_session = forward(
                    &discovery,
                    None,
                    Some(&mut session_id),
                    &protocol_revision,
                    message,
                    &writer,
                )?;
                session_id = returned_session;
            }
        }
        Ok(())
    })();
    // Settle every admitted frame on success and failure before releasing its
    // session. A connector failure never detaches or cancels backend work.
    let mut result = result;
    for worker in workers {
        if worker
            .join()
            .map_err(|_| ())
            .and_then(|result| result)
            .is_err()
        {
            result = Err(());
        }
    }
    if let Some(session_id) = session_id.as_deref() {
        let _ = connector_close_session(&discovery, session_id);
    }
    result
}

fn forward(
    discovery: &crate::transport::ConnectorDiscovery,
    session_id: Option<&str>,
    allocated_session: Option<&mut Option<String>>,
    protocol_revision: &str,
    message: McpMessage,
    writer: &Mutex<io::Stdout>,
) -> Result<Option<String>, ()> {
    let body = encode_http_body(&message, MAX_MCP_FRAME_BYTES).map_err(|_| ())?;
    let (status, returned_session, response) =
        match connector_exchange(discovery, session_id, protocol_revision, &body) {
            Ok(response) => response,
            Err(_) => {
                module_unavailable(&message, writer, discovery, true)?;
                return Ok(session_id.map(str::to_owned));
            }
        };
    if session_id.is_none()
        && let Some(allocated_session) = allocated_session
    {
        *allocated_session = returned_session.clone();
    }
    if session_id.is_some() && returned_session.as_deref() != session_id {
        module_unavailable(&message, writer, discovery, status == 200)?;
        return Ok(session_id.map(str::to_owned));
    }
    match status {
        200 => {
            let response = match crate::decode_http_body(&response, MAX_MCP_FRAME_BYTES) {
                Ok(response) => response,
                Err(_) => {
                    module_unavailable(&message, writer, discovery, true)?;
                    return Ok(returned_session);
                }
            };
            let mut writer = writer.lock().map_err(|_| ())?;
            writer
                .write_all(&encode_stdio_line(&response, MAX_MCP_FRAME_BYTES).map_err(|_| ())?)
                .map_err(|_| ())?;
            writer.flush().map_err(|_| ())?;
        }
        202 if response.is_empty() => {}
        _ => {
            module_unavailable(&message, writer, discovery, false)?;
            return Ok(session_id.map(str::to_owned));
        }
    }
    Ok(returned_session)
}

/// A call to an effect-bearing tool whose request already left the connector
/// has an uncertain outcome — it may have executed. The response marks it
/// `mcp_outcome_unknown` and forbids replay; every other failure is a plain
/// module-unavailable error the client may retry after recovery.
fn module_unavailable(
    message: &McpMessage,
    writer: &Mutex<io::Stdout>,
    discovery: &crate::transport::ConnectorDiscovery,
    sent: bool,
) -> Result<(), ()> {
    let McpMessage::Request { id, method, params } = message else {
        return Ok(());
    };
    let uncertain = sent
        && method == "tools/call"
        && params
            .as_ref()
            .and_then(|params| params.get("name"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|name| discovery.effect_tools.iter().any(|tool| tool == name));
    let response = McpMessage::error(
        Some(id.clone()),
        -32000,
        "MCP module unavailable",
        Some(serde_json::json!({"reasonCode":if uncertain {"mcp_outcome_unknown"} else {"mcp_module_unavailable"},
            "requestMayHaveExecuted":uncertain,"retryable":!uncertain,
            "recovery":if uncertain {"reconcile_before_retry"} else {"retry_after_recovery"}})),
    )
    .map_err(|_| ())?;
    let mut writer = writer.lock().map_err(|_| ())?;
    writer
        .write_all(&encode_stdio_line(&response, MAX_MCP_FRAME_BYTES).map_err(|_| ())?)
        .map_err(|_| ())?;
    writer.flush().map_err(|_| ())
}

fn initialize_protocol_revision(message: &McpMessage) -> Option<&str> {
    match message {
        McpMessage::Request { method, params, .. } if method == "initialize" => params
            .as_ref()
            .and_then(|params| params.get("protocolVersion"))
            .and_then(serde_json::Value::as_str),
        _ => None,
    }
}

/// Separate the optional `--home <dir>` pair from the caller arguments, so
/// the connector may serve a state root other than the environment's.
fn split_home(args: &[String]) -> Result<(Vec<String>, Option<PathBuf>), CallerRejection> {
    let mut caller_args = Vec::with_capacity(args.len());
    let mut home = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--home" => {
                if home.is_some() || index + 1 >= args.len() {
                    return Err(CallerRejection::Unexpected);
                }
                home = Some(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            _ => {
                caller_args.push(args[index].clone());
                index += 1;
            }
        }
    }
    Ok((caller_args, home))
}

/// Resolve the declared caller from the command line or the environment.
///
/// Only the declaration is resolved here: exactly one source, non-empty, no
/// conflicting pair. Whether the declared caller is admitted is decided later
/// against the published capability set, so this binary never carries a list
/// of its own that could drift from the service that mints the seats.
fn caller_provider(
    args: impl IntoIterator<Item = String>,
    environment: Option<String>,
) -> Result<String, CallerRejection> {
    let args = args.into_iter().collect::<Vec<_>>();
    let argument = match args.as_slice() {
        [] => None,
        [flag, provider] if flag == "--caller" => Some(provider.as_str()),
        _ => return Err(CallerRejection::Unexpected),
    };
    // A blank declaration declares nothing, so it reports as undeclared
    // rather than as an unknown provider name.
    let environment = environment
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    if argument.is_some() && environment.is_some() && argument != environment {
        return Err(CallerRejection::Conflicting);
    }
    let provider = argument
        .or(environment)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(CallerRejection::Undeclared)?;
    Ok(provider.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn caller_identity_is_exact_and_non_ambiguous() {
        assert_eq!(
            caller_provider(args(&["--caller", "cursor"]), None).unwrap(),
            "cursor"
        );
        assert_eq!(
            caller_provider(std::iter::empty(), Some("codex".into())).unwrap(),
            "codex"
        );
        assert_eq!(
            caller_provider(args(&["--caller", "cursor"]), Some("codex".into())).unwrap_err(),
            CallerRejection::Conflicting
        );
        assert_eq!(
            caller_provider(args(&["--caller", "claude-code"]), None).unwrap(),
            "claude-code"
        );
        // Membership is not decided here: a declared name this build does not
        // recognise is refused against the published capability set instead,
        // so the two causes stay distinguishable.
        assert_eq!(
            caller_provider(args(&["--caller", "other"]), None).unwrap(),
            "other"
        );
    }

    #[test]
    fn refused_callers_carry_an_actionable_diagnostic() {
        assert_eq!(
            caller_provider(std::iter::empty(), None).unwrap_err(),
            CallerRejection::Undeclared
        );
        assert_eq!(
            caller_provider(std::iter::empty(), Some("   ".into())).unwrap_err(),
            CallerRejection::Undeclared
        );
        assert_eq!(
            caller_provider(args(&["--caller"]), None).unwrap_err(),
            CallerRejection::Unexpected
        );
        assert_eq!(
            caller_provider(args(&["--caller", "cursor", "extra"]), None).unwrap_err(),
            CallerRejection::Unexpected
        );

        let supported = vec!["codex".to_owned(), "cursor".to_owned()];
        let undeclared = CallerRejection::Undeclared.describe(&supported);
        assert!(undeclared.contains("is not declared"), "{undeclared}");
        assert!(undeclared.contains("MCPD_CALLER"), "{undeclared}");
        assert!(
            undeclared.contains("(supported: codex, cursor)"),
            "{undeclared}"
        );

        let unsupported = CallerRejection::NotAdmitted {
            provider: "grok".to_owned(),
        }
        .describe(&supported);
        assert!(
            unsupported.contains("'grok' is not an admitted caller"),
            "{unsupported}"
        );
        assert!(unsupported.contains("codex, cursor"), "{unsupported}");

        let unknown = CallerRejection::NotAdmitted {
            provider: "claude".to_owned(),
        }
        .describe(&supported);
        assert!(
            unknown.contains("'claude' is not an admitted caller"),
            "{unknown}"
        );

        // A refusal must stay useful when the service is not running and the
        // capability set is therefore unknown.
        let bare = CallerRejection::Undeclared.describe(&[]);
        assert!(bare.contains("is not declared"), "{bare}");
        assert!(!bare.contains("supported"), "{bare}");

        assert!(
            CallerRejection::Conflicting
                .describe(&supported)
                .contains("declare exactly one caller provider")
        );
        assert!(
            CallerRejection::Unexpected
                .describe(&supported)
                .contains("--caller <provider>")
        );
    }

    #[test]
    fn initialize_revision_is_forwarded_from_the_client_handshake() {
        let initialize = McpMessage::request(
            1_i64,
            "initialize",
            serde_json::json!({
                "protocolVersion":"2025-11-25",
                "capabilities":{},
                "clientInfo":{"name":"fixture","version":"1"}
            })
            .as_object()
            .cloned(),
        )
        .unwrap();
        let list = McpMessage::request(2_i64, "tools/list", Some(serde_json::Map::new())).unwrap();

        assert_eq!(
            initialize_protocol_revision(&initialize),
            Some("2025-11-25")
        );
        assert_eq!(initialize_protocol_revision(&list), None);
    }

    #[test]
    fn home_flag_splits_from_caller_arguments() {
        let (caller_args, home) =
            split_home(&args(&["--caller", "codex", "--home", "/tmp/fixture-home"])).unwrap();
        assert_eq!(caller_args, args(&["--caller", "codex"]));
        assert_eq!(
            home.as_deref(),
            Some(std::path::Path::new("/tmp/fixture-home"))
        );
        let (caller_args, home) = split_home(&args(&[])).unwrap();
        assert!(caller_args.is_empty() && home.is_none());
        assert!(split_home(&args(&["--home"])).is_err());
        assert!(split_home(&args(&["--home", "/a", "--home", "/b"])).is_err());
    }
}
