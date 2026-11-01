//! Thin stdio connector for the dispatcher's loopback HTTP service.
//!
//! This module owns no tools, catalog, repeat-attempt policy, caller
//! adaptation or conversation state. One stdio frame maps to one
//! authenticated loopback HTTP request and one response frame. The caller
//! context mapping and the no-replay tool list arrive inside the published
//! discovery document — the connector learns them, it does not carry them.

use crate::application::{COMPATIBLE_PROTOCOL_REVISIONS, MAX_MCP_FRAME_BYTES, PROTOCOL_REVISION};
use crate::transport::{
    ConnectorDiscoveryError, connector_close_session, connector_exchange, load_connector_discovery,
    published_callers,
};
use crate::{
    McpMessage, McpServerDefinition, McpStdioFrame, encode_http_body, encode_stdio_line,
    read_stdio_frame,
};
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

/// The wire contract this connector speaks: the crate's fixed revision set
/// and frame bound as one `McpServerDefinition`, so the client checks the
/// same predicate the serving side enforces — the two can never drift on
/// what a peer may ask for or how large a frame may be.
fn connector_definition() -> McpServerDefinition {
    McpServerDefinition {
        protocol_revision: PROTOCOL_REVISION,
        compatible_protocol_revisions: COMPATIBLE_PROTOCOL_REVISIONS,
        server_name: "mcpd-connector",
        server_version: env!("CARGO_PKG_VERSION"),
        max_message_bytes: MAX_MCP_FRAME_BYTES,
    }
}

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
    let definition = connector_definition();
    let session = Arc::new(Mutex::new(ConnectorSession {
        id: None,
        initialize: None,
        protocol_revision: PROTOCOL_REVISION.to_owned(),
    }));
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
            // Record the client's own handshake before the frame is
            // dispatched: any worker that finds the session reaped replays
            // this verbatim initialize to mint the replacement.
            let (live_id, revision, has_initialize) = {
                let mut guard = session.lock().map_err(|_| ())?;
                if current != discovery {
                    discovery = current;
                    guard.id = None;
                }
                if initialize_protocol_revision(&message).is_some() {
                    guard.initialize = Some(message.clone());
                }
                if guard.id.is_none()
                    && let Some(requested) = initialize_protocol_revision(&message)
                    && definition.supports_protocol_revision(requested)
                {
                    guard.protocol_revision = requested.to_owned();
                }
                (
                    guard.id.clone(),
                    guard.protocol_revision.clone(),
                    guard.initialize.is_some(),
                )
            };
            // A non-initialize frame with no live session mints one by
            // replaying the stored initialize before it is dispatched.
            let live_id = match live_id {
                Some(id) => Some(id),
                None if has_initialize && initialize_protocol_revision(&message).is_none() => {
                    match heal_session(&session, None, &discovery, &definition) {
                        Some(id) => Some(id),
                        None => {
                            module_unavailable(&message, &writer, &discovery, false)?;
                            continue;
                        }
                    }
                }
                None => None,
            };
            if let Some(session_id) = live_id {
                if workers.len() >= MAX_IN_FLIGHT_FRAMES {
                    return Err(());
                }
                let discovery = discovery.clone();
                let definition = definition.clone();
                let writer = Arc::clone(&writer);
                let session = Arc::clone(&session);
                workers.push(thread::spawn(move || {
                    forward(
                        &discovery,
                        Some(&session_id),
                        &revision,
                        &definition,
                        message,
                        &writer,
                        &session,
                    )
                    .map(|_| ())
                }));
            } else {
                let returned_session = forward(
                    &discovery,
                    None,
                    &revision,
                    &definition,
                    message,
                    &writer,
                    &session,
                )?;
                session.lock().map_err(|_| ())?.id = returned_session;
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
    if let Ok(guard) = session.lock()
        && let Some(session_id) = guard.id.as_deref()
    {
        let _ = connector_close_session(&discovery, session_id);
    }
    result
}

/// The connector's live server session, shared with the in-flight forward
/// workers so whichever of them first meets a reaped session mints the
/// replacement for everyone. `id` is the session the next session-bound
/// frame rides; `initialize` is the client's verbatim handshake, replayed
/// to mint that replacement; `protocol_revision` is the revision header
/// the session negotiated and every session-bound frame must carry.
struct ConnectorSession {
    id: Option<String>,
    initialize: Option<McpMessage>,
    protocol_revision: String,
}

/// Whether a session-bound exchange proves the session is gone: the server
/// refused it outright (404 — the only POST response issued for an unknown
/// session), or it answered success without echoing the session id the
/// connector sent. Other refusals (400/415/503) are request failures, not
/// session loss; a sessionless exchange has nothing to lose.
fn session_gone(status: u16, session_id: Option<&str>, returned: Option<&str>) -> bool {
    session_id.is_some()
        && (status == 404 || (matches!(status, 200 | 202) && returned != session_id))
}

/// Whether this frame is a call to an effect-bearing tool — decided against
/// the published discovery set, never a list compiled into the connector.
/// An effect call whose request left the connector is never resent.
fn effect_call(message: &McpMessage, discovery: &crate::transport::ConnectorDiscovery) -> bool {
    matches!(message, McpMessage::Request { method, params, .. }
        if method == "tools/call"
            && params
                .as_ref()
                .and_then(|params| params.get("name"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| discovery.effect_tools.iter().any(|tool| tool == name)))
}

/// Mint a replacement session by replaying the client's stored initialize,
/// serialized on the session lock so a dead session is healed exactly once
/// no matter how many in-flight workers meet it. `dead_id` is the session
/// the caller found gone; if the shared id already moved past it, a peer
/// healed (or a new handshake superseded) and that live id is reused. The
/// negotiated revision the replay returns replaces the stored one, so
/// later frames carry the session's true revision.
fn heal_session(
    session: &Mutex<ConnectorSession>,
    dead_id: Option<&str>,
    discovery: &crate::transport::ConnectorDiscovery,
    definition: &McpServerDefinition,
) -> Option<String> {
    let mut guard = session.lock().ok()?;
    if let Some(id) = guard.id.as_ref().filter(|id| Some(id.as_str()) != dead_id) {
        return Some(id.clone());
    }
    guard.id = None;
    let initialize = guard.initialize.clone()?;
    let body = encode_http_body(&initialize, definition.max_message_bytes).ok()?;
    let protocol_revision = guard.protocol_revision.clone();
    let Ok((200, Some(minted), response)) =
        connector_exchange(discovery, None, &protocol_revision, &body, definition)
    else {
        return None;
    };
    if let Ok(response) = crate::decode_http_body(&response, definition.max_message_bytes)
        && let Some(negotiated) = response
            .to_value()
            .pointer("/result/protocolVersion")
            .and_then(serde_json::Value::as_str)
    {
        guard.protocol_revision = negotiated.to_owned();
    }
    guard.id = Some(minted.clone());
    Some(minted)
}

fn forward<W: Write>(
    discovery: &crate::transport::ConnectorDiscovery,
    session_id: Option<&str>,
    protocol_revision: &str,
    definition: &McpServerDefinition,
    message: McpMessage,
    writer: &Mutex<W>,
    session: &Mutex<ConnectorSession>,
) -> Result<Option<String>, ()> {
    let body = encode_http_body(&message, definition.max_message_bytes).map_err(|_| ())?;
    let mut attempt_session = session_id.map(str::to_owned);
    // A reaped session buys this frame exactly one retry on the healed
    // session — never more, and never at all for an effect call.
    let mut retried = false;
    loop {
        let (status, returned_session, response) = match connector_exchange(
            discovery,
            attempt_session.as_deref(),
            protocol_revision,
            &body,
            definition,
        ) {
            Ok(response) => response,
            Err(_) => {
                module_unavailable(&message, writer, discovery, true)?;
                return Ok(attempt_session);
            }
        };
        if session_gone(
            status,
            attempt_session.as_deref(),
            returned_session.as_deref(),
        ) {
            // The service reaped the session the connector still held. Mint
            // a replacement by replaying the stored initialize, then settle
            // this frame: an effect call is never resent (the no-replay
            // rule), so its lost answer is reported outcome-unknown; any
            // other frame is retried once on the healed session.
            let healed = heal_session(session, attempt_session.as_deref(), discovery, definition);
            if effect_call(&message, discovery) {
                module_unavailable(&message, writer, discovery, true)?;
                return Ok(healed);
            }
            match healed {
                Some(healed_id) if !retried => {
                    retried = true;
                    attempt_session = Some(healed_id);
                    continue;
                }
                _ => {
                    module_unavailable(&message, writer, discovery, false)?;
                    return Ok(healed);
                }
            }
        }
        if attempt_session.is_some() && returned_session.as_deref() != attempt_session.as_deref() {
            module_unavailable(&message, writer, discovery, false)?;
            return Ok(attempt_session);
        }
        match status {
            200 => {
                let response =
                    match crate::decode_http_body(&response, definition.max_message_bytes) {
                        Ok(response) => response,
                        Err(_) => {
                            module_unavailable(&message, writer, discovery, true)?;
                            return Ok(returned_session);
                        }
                    };
                // Adopt the revision the server negotiated: a mid-session
                // re-initialize moves the session's revision, and later
                // frames must carry it to stay admissible.
                if initialize_protocol_revision(&message).is_some()
                    && let Some(negotiated) = response
                        .to_value()
                        .pointer("/result/protocolVersion")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                    && let Ok(mut guard) = session.lock()
                {
                    guard.protocol_revision = negotiated;
                }
                let mut writer = writer.lock().map_err(|_| ())?;
                writer
                    .write_all(&encode_stdio_line(&response, MAX_MCP_FRAME_BYTES).map_err(|_| ())?)
                    .map_err(|_| ())?;
                writer.flush().map_err(|_| ())?;
            }
            202 if response.is_empty() => {}
            _ => {
                module_unavailable(&message, writer, discovery, false)?;
                return Ok(attempt_session);
            }
        }
        return Ok(returned_session);
    }
}

/// A call to an effect-bearing tool whose request already left the connector
/// has an uncertain outcome — it may have executed. The response marks it
/// `mcp_outcome_unknown` and forbids replay; every other failure is a plain
/// module-unavailable error the client may retry after recovery.
fn module_unavailable<W: Write>(
    message: &McpMessage,
    writer: &Mutex<W>,
    discovery: &crate::transport::ConnectorDiscovery,
    sent: bool,
) -> Result<(), ()> {
    let McpMessage::Request { id, .. } = message else {
        return Ok(());
    };
    let uncertain = sent && effect_call(message, discovery);
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
    use crate::transport::{HttpService, load_connector_discovery};
    use crate::{Dispatcher, InProcessBackend, Registration, ToolSpec};
    use serde_json::{Map, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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

    #[test]
    fn the_session_gone_predicate_needs_a_bound_session() {
        // Only a session-bound exchange can lose a session: 404 is the
        // server's only POST answer for an unknown id, and a successful
        // answer must echo the session it served.
        assert!(session_gone(404, Some("s"), None));
        assert!(session_gone(200, Some("s"), None));
        assert!(session_gone(202, Some("s"), Some("other")));
        assert!(!session_gone(200, Some("s"), Some("s")));
        assert!(!session_gone(400, Some("s"), None));
        assert!(!session_gone(503, Some("s"), None));
        assert!(!session_gone(404, None, None));
    }

    /// One caller, one ordinary tool and one effect tool, over a real
    /// `HttpService` — the smallest deployment the connector side can heal
    /// against. `calls` counts how often the backend actually ran.
    fn fixture_service(calls: Arc<AtomicUsize>) -> (HttpService, McpServerDefinition) {
        let schema = json!({"type":"object","additionalProperties":false,"properties":{}});
        let dispatcher = Dispatcher::from_registrations(vec![
            Registration::new(
                "fixture",
                "1",
                vec![
                    ToolSpec::declared("fixture_tool", None, schema.clone(), false, false).unwrap(),
                    ToolSpec::declared("fixture_effect", None, schema, false, true).unwrap(),
                ],
                vec!["fixture".to_owned()],
                InProcessBackend::new(move |request| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    Ok(json!({"tool": request.tool}))
                }),
            )
            .unwrap(),
        ])
        .unwrap();
        let definition = McpServerDefinition {
            protocol_revision: PROTOCOL_REVISION,
            compatible_protocol_revisions: COMPATIBLE_PROTOCOL_REVISIONS,
            server_name: "fixture-service",
            server_version: "1",
            max_message_bytes: MAX_MCP_FRAME_BYTES,
        };
        (
            HttpService::start(dispatcher, definition.clone(), Vec::new()).unwrap(),
            definition,
        )
    }

    fn initialize() -> McpMessage {
        McpMessage::request(
            1_i64,
            "initialize",
            json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}})
                .as_object()
                .cloned(),
        )
        .unwrap()
    }

    fn tool_call(id: i64, name: &str) -> McpMessage {
        McpMessage::request(
            id,
            "tools/call",
            json!({"name": name, "arguments": {}}).as_object().cloned(),
        )
        .unwrap()
    }

    fn written(writer: &Mutex<Vec<u8>>) -> serde_json::Value {
        crate::decode_stdio_line(&writer.lock().unwrap(), MAX_MCP_FRAME_BYTES)
            .unwrap()
            .to_value()
    }

    /// The blocking-finding proof: a session reaped for idleness does not
    /// kill the connector — a session-bound frame on the dead id is healed
    /// and retried once, transparently, while an effect call is reported
    /// outcome-unknown and never resent.
    #[test]
    fn a_reaped_session_heals_and_effect_calls_stay_uncertain() {
        let _serial = crate::private_state::STATE_ROOT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let root =
            std::env::temp_dir().join(format!("mcpd-heal-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        crate::private_state::set_state_root_override(root.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let (service, definition) = fixture_service(Arc::clone(&calls));
        let discovery = load_connector_discovery("fixture").unwrap();
        let writer = Mutex::new(Vec::<u8>::new());
        let session = Mutex::new(ConnectorSession {
            id: None,
            initialize: Some(initialize()),
            protocol_revision: PROTOCOL_REVISION.to_owned(),
        });

        // The client's handshake mints the session — run()'s bookkeeping,
        // driven directly here.
        let minted = forward(
            &discovery,
            None,
            PROTOCOL_REVISION,
            &definition,
            initialize(),
            &writer,
            &session,
        )
        .unwrap()
        .expect("initialize mints a session");
        session.lock().unwrap().id = Some(minted.clone());
        writer.lock().unwrap().clear();

        // Idle past the TTL: the next table access reaps the row. A plain
        // frame on the dead id is healed and retried once — the caller sees
        // the answer, never an error.
        service.set_session_idle_ttl(Duration::from_millis(20));
        thread::sleep(Duration::from_millis(80));
        forward(
            &discovery,
            Some(&minted),
            PROTOCOL_REVISION,
            &definition,
            McpMessage::request(2_i64, "tools/list", Some(Map::new())).unwrap(),
            &writer,
            &session,
        )
        .unwrap();
        let healed = session.lock().unwrap().id.clone().unwrap();
        assert_ne!(healed, minted, "the dead session must be replaced");
        let listed = written(&writer);
        assert_eq!(listed["id"], 2);
        assert_eq!(listed["result"]["tools"][0]["name"], "fixture_tool");
        writer.lock().unwrap().clear();

        // The healed session serves effect calls normally while live.
        forward(
            &discovery,
            Some(&healed),
            PROTOCOL_REVISION,
            &definition,
            tool_call(3, "fixture_effect"),
            &writer,
            &session,
        )
        .unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 1);
        writer.lock().unwrap().clear();

        // Expired again: an effect call meets the dead id. The session is
        // still healed for the next frame, but the call itself is never
        // resent — it is reported outcome-unknown.
        thread::sleep(Duration::from_millis(80));
        forward(
            &discovery,
            Some(&healed),
            PROTOCOL_REVISION,
            &definition,
            tool_call(4, "fixture_effect"),
            &writer,
            &session,
        )
        .unwrap();
        let lost = written(&writer);
        assert_eq!(lost["id"], 4);
        assert_eq!(lost["error"]["data"]["reasonCode"], "mcp_outcome_unknown");
        assert_eq!(lost["error"]["data"]["requestMayHaveExecuted"], true);
        assert_eq!(lost["error"]["data"]["retryable"], false);
        assert_eq!(
            calls.load(Ordering::Acquire),
            1,
            "an effect call is never resent after a reaped session"
        );
        let rehealed = session.lock().unwrap().id.clone().unwrap();
        assert_ne!(rehealed, healed, "the dead session must still be replaced");

        drop(service);
        crate::private_state::clear_state_root_override();
        std::fs::remove_dir_all(&root).unwrap();
    }
}
