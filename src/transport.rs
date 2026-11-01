//! Independently owned authenticated loopback Streamable HTTP MCP service.
//!
//! Admission happens before any effect: exact numeric-loopback Host, no
//! browser Origin, then the bearer client identity. Per-caller ephemeral
//! tokens live only in the owner-only discovery document; sessions, tool
//! workers and control calls are all bounded.

use crate::private_state;
use crate::registry::{CallerContext, ContextField, valid_caller_id, valid_tool_name};
use crate::{
    Dispatcher, McpMessage, McpRequestId, McpServerDefinition, McpServerEngine, McpSessionState,
    decode_http_body, encode_http_body,
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DISCOVERY_SCHEMA: &str = "mcpd.discovery.v1";
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_CONNECTIONS: usize = 32;
const MAX_SESSIONS: usize = 64;
const MAX_TOOL_WORKERS: usize = 8;
const MAX_CONTEXT_FIELDS: usize = 16;
const MAX_PUBLISHED_TOOLS: usize = 256;
/// A session no request has touched within this TTL is reaped on the next
/// table access: a crashed connector cannot hold its slot forever. The TTL
/// bounds only how long a *dead* session keeps its slot — a live connector
/// whose session was reaped mints a replacement by replaying its stored
/// handshake, so a connector process may idle far longer than this (an
/// overnight caller is the normal case, not an anomaly). A session with an
/// in-flight request is never reaped: its answer must reach the caller.
const SESSION_IDLE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiscoveryDocument {
    pub(crate) schema_version: String,
    pub(crate) endpoint: String,
    pub(crate) generation: String,
    pub(crate) tokens: HashMap<String, String>,
    pub(crate) control_token: String,
    /// The declared caller-context mapping; the connector learns which
    /// environment variables become which loopback headers from here, never
    /// from a compiled-in list.
    pub(crate) context_headers: Vec<ContextField>,
    /// Effect-bearing tool names: a call to one whose transport failed
    /// mid-flight has an uncertain outcome and must never be replayed.
    pub(crate) effect_tools: Vec<String>,
}

/// One caller's authenticated loopback identity. `PartialEq` compares only
/// the endpoint and token — a redeployed service mints both fresh, so that is
/// also the connector's "the service changed" signal; the riding context
/// fields never decide identity on their own.
#[derive(Clone)]
pub struct ConnectorDiscovery {
    pub(crate) endpoint: String,
    bearer_token: String,
    pub(crate) context_headers: Vec<ContextField>,
    pub(crate) effect_tools: Vec<String>,
}

impl PartialEq for ConnectorDiscovery {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint && self.bearer_token == other.bearer_token
    }
}

/// The supervised loopback service: owns the listener thread, the published
/// discovery document and the stop flag. Dropping it stops the listener,
/// drains in-flight connections and removes only the discovery generation it
/// itself published.
pub struct HttpService {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    discovery_path: PathBuf,
    pub(crate) generation: String,
    address: SocketAddr,
    probe_identity: ConnectorDiscovery,
    probe_definition: McpServerDefinition,
    probe_catalog: Vec<String>,
    /// Test-only handle on the reaper's TTL so a test can expire a session
    /// without waiting out the production idle window.
    #[cfg(test)]
    session_idle_ttl: Arc<AtomicU64>,
}

impl HttpService {
    /// Bind the numeric-loopback listener, publish discovery, and start
    /// serving the dispatcher's union catalog.
    pub fn start(
        application: Dispatcher,
        definition: McpServerDefinition,
        context_fields: Vec<ContextField>,
    ) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        if !address.ip().is_loopback() {
            return Err(anyhow!("mcpd_loopback_required"));
        }
        for field in &context_fields {
            field.validate()?;
        }
        // Complete every fallible in-process construction before publishing
        // discovery. After publication, the only fallible step is thread
        // creation, whose error path performs generation-bound cleanup.
        let probe_definition = definition.clone();
        let probe_catalog = application.catalog_names();
        let engine = McpServerEngine::new(definition, application.clone())?;
        let generation = uuid::Uuid::new_v4().simple().to_string();
        // The token map is the capability set made concrete: exactly the
        // callers the registry admits. Publishing it here also lets the
        // connector distinguish "this caller has no seat" from "the service
        // is unreachable", without consulting a list of its own.
        let control_token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let tokens = application
            .caller_providers()
            .into_iter()
            .map(|provider| {
                (
                    provider,
                    format!(
                        "{}{}",
                        uuid::Uuid::new_v4().simple(),
                        uuid::Uuid::new_v4().simple()
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let discovery_path = discovery_path()?;
        write_discovery(
            &discovery_path,
            &DiscoveryDocument {
                schema_version: DISCOVERY_SCHEMA.to_owned(),
                endpoint: format!("http://{address}/mcp"),
                generation: generation.clone(),
                tokens: tokens.clone(),
                control_token: control_token.clone(),
                context_headers: context_fields.clone(),
                effect_tools: application.effect_tools(),
            },
        )?;
        let probe_identity = ConnectorDiscovery {
            endpoint: format!("http://{address}/mcp"),
            bearer_token: tokens
                .keys()
                .min()
                .and_then(|provider| tokens.get(provider))
                .cloned()
                .ok_or_else(|| anyhow!("mcpd_unavailable"))?,
            context_headers: Vec::new(),
            effect_tools: Vec::new(),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let session_idle_ttl = Arc::new(AtomicU64::new(SESSION_IDLE_TTL.as_millis() as u64));
        let service = Arc::new(ServiceState {
            application,
            engine,
            tokens,
            control_token,
            context_fields,
            stop: Arc::clone(&stop),
            sessions: Mutex::new(HashMap::new()),
            session_idle_ttl: Arc::clone(&session_idle_ttl),
            active_connections: AtomicUsize::new(0),
            active_tool_calls: AtomicUsize::new(0),
            active_control_calls: AtomicUsize::new(0),
            address,
        });
        let thread_stop = Arc::clone(&stop);
        let thread = match thread::Builder::new()
            .name("mcpd-http".to_owned())
            .spawn(move || serve(listener, service, thread_stop))
        {
            Ok(thread) => thread,
            Err(error) => {
                cleanup_discovery_generation(&discovery_path, &generation);
                return Err(error.into());
            }
        };
        Ok(Self {
            stop,
            thread: Some(thread),
            discovery_path,
            generation,
            address,
            probe_identity,
            probe_definition,
            probe_catalog,
            #[cfg(test)]
            session_idle_ttl,
        })
    }

    /// Test-only lever: shrink the reaper's TTL so a test can expire a live
    /// session on demand instead of waiting out the production idle window.
    #[cfg(test)]
    pub(crate) fn set_session_idle_ttl(&self, ttl: Duration) {
        self.session_idle_ttl
            .store(ttl.as_millis() as u64, Ordering::Release);
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    pub fn healthy(&self) -> bool {
        self.thread
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
            && self.address.ip().is_loopback()
    }

    /// Health is the real contract, not thread liveness: one authenticated
    /// initialize plus the exact ordered union catalog. The probe identity
    /// never leaves the process and the probe session is closed afterwards.
    /// The probe speaks the served definition's own revision and frame
    /// bound, so a host-built definition is probed on its own terms.
    pub fn health_probe(&self) -> bool {
        let definition = &self.probe_definition;
        let initialize = McpMessage::request(
            McpRequestId::from(1_i64),
            "initialize",
            Some(
                serde_json::json!({
                    "protocolVersion": definition.protocol_revision,
                    "capabilities": {},
                    "clientInfo": {"name": "mcpd-supervisor-health", "version": "1"}
                })
                .as_object()
                .cloned()
                .unwrap_or_default(),
            ),
        );
        let Ok(initialize) = initialize else {
            return false;
        };
        let Ok(body) = encode_http_body(&initialize, definition.max_message_bytes) else {
            return false;
        };
        let Ok((status, session, response)) =
            health_exchange(&self.probe_identity, None, &body, definition)
        else {
            return false;
        };
        let session = session.filter(|session| !session.is_empty());
        if status != 200 {
            if let Some(session) = session.as_deref() {
                let _ = connector_close_session(&self.probe_identity, session);
            }
            return false;
        }
        let Some(session) = session else {
            return false;
        };
        let healthy = (|| {
            let negotiated = decode_http_body(&response, definition.max_message_bytes)
                .ok()
                .map(|message| message.to_value());
            let valid_initialize = negotiated.as_ref().is_some_and(|value| {
                value
                    .pointer("/result/protocolVersion")
                    .and_then(Value::as_str)
                    == Some(definition.protocol_revision)
                    && value
                        .pointer("/result/serverInfo/name")
                        .and_then(Value::as_str)
                        == Some(definition.server_name)
                    && value
                        .pointer("/result/serverInfo/version")
                        .and_then(Value::as_str)
                        == Some(definition.server_version)
            });
            if !valid_initialize {
                return false;
            }
            let Ok(list) = McpMessage::request(2_i64, "tools/list", Some(Map::new())) else {
                return false;
            };
            let Ok(body) = encode_http_body(&list, definition.max_message_bytes) else {
                return false;
            };
            let listed = health_exchange(&self.probe_identity, Some(&session), &body, definition)
                .ok()
                .and_then(|(status, returned, response)| {
                    (status == 200 && returned.as_deref() == Some(session.as_str()))
                        .then(|| decode_http_body(&response, definition.max_message_bytes).ok())
                        .flatten()
                })
                .map(|message| message.to_value());
            let expected = self
                .probe_catalog
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            listed.is_some_and(|value| {
                value
                    .pointer("/result/tools")
                    .and_then(Value::as_array)
                    .map(|tools| {
                        tools
                            .iter()
                            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                    })
                    .as_deref()
                    == Some(expected.as_slice())
            })
        })();
        let closed = connector_close_session(&self.probe_identity, &session).is_ok();
        healthy && closed
    }
}

impl Drop for HttpService {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_millis(100));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        cleanup_discovery_generation(&self.discovery_path, &self.generation);
    }
}

struct ServiceState {
    application: Dispatcher,
    engine: McpServerEngine<Dispatcher>,
    control_token: String,
    stop: Arc<AtomicBool>,
    tokens: HashMap<String, String>,
    context_fields: Vec<ContextField>,
    sessions: Mutex<HashMap<String, ServerSession>>,
    /// The idle TTL the reaper enforces, in milliseconds. A field rather
    /// than the bare constant so a test can shrink it without an hour of
    /// wall-clock idle; production only ever stores `SESSION_IDLE_TTL`.
    session_idle_ttl: Arc<AtomicU64>,
    active_connections: AtomicUsize,
    active_tool_calls: AtomicUsize,
    active_control_calls: AtomicUsize,
    address: SocketAddr,
}

/// The session-bound caller attributes, keyed by declared attribute name.
/// Values merge forward: a header the caller omits later keeps its earlier
/// session value, a header it resends replaces it.
type SessionCallerBinding = BTreeMap<String, String>;

struct ServerSession {
    provider: String,
    state: Arc<McpSessionState>,
    caller: Mutex<SessionCallerBinding>,
    /// The last time a request touched this session — the idle clock the
    /// table reaper reads.
    last_seen: Instant,
    /// Requests admitted against this session that have not yet answered.
    /// Outstanding work pins the row: the reaper never collects a session
    /// whose answer the caller is still owed.
    in_flight: usize,
}

impl ServiceState {
    fn session_idle_ttl(&self) -> Duration {
        Duration::from_millis(self.session_idle_ttl.load(Ordering::Acquire))
    }
}

/// Reap sessions idle past `ttl`, evaluated against `now` — both injected
/// so the predicate is decidable in a unit test. Runs under the table lock
/// on every acquisition — the table is bounded (`MAX_SESSIONS`), so the
/// sweep is cheap and needs no separate reaper thread. A session with an
/// in-flight request is never reaped: an answer owed to a caller must not
/// lose its row mid-flight.
fn reap_idle_sessions(sessions: &mut HashMap<String, ServerSession>, now: Instant, ttl: Duration) {
    sessions.retain(|_, session| {
        session.in_flight > 0 || now.saturating_duration_since(session.last_seen) < ttl
    });
}

/// Pins a session row while its request executes; dropping the guard
/// releases the pin whether the answer left cleanly or the handler bailed.
struct SessionInFlight<'a> {
    sessions: &'a Mutex<HashMap<String, ServerSession>>,
    id: String,
}

impl Drop for SessionInFlight<'_> {
    fn drop(&mut self) {
        if let Some(session) = self
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get_mut(&self.id)
        {
            session.in_flight = session.in_flight.saturating_sub(1);
            // Finishing a request is the latest touch too: the idle clock
            // restarts here, so a session busy a moment ago is not reaped.
            session.last_seen = Instant::now();
        }
    }
}

fn merge_session_caller_binding(
    existing: &SessionCallerBinding,
    headers: &HashMap<String, String>,
    context_fields: &[ContextField],
) -> SessionCallerBinding {
    let mut merged = existing.clone();
    for field in context_fields {
        if let Some(value) = headers.get(field.header.as_str()) {
            merged.insert(field.attribute.clone(), value.clone());
        }
    }
    merged
}

fn serve(listener: TcpListener, service: Arc<ServiceState>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, peer)) => {
                if !peer.ip().is_loopback()
                    || service
                        .active_connections
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                            (count < MAX_HTTP_CONNECTIONS).then_some(count + 1)
                        })
                        .is_err()
                {
                    continue;
                }
                let request_service = Arc::clone(&service);
                if thread::Builder::new()
                    .name("mcpd-request".to_owned())
                    .spawn(move || {
                        let _permit = ConnectionPermit(&request_service.active_connections);
                        let _ = handle_connection(stream, &request_service);
                    })
                    .is_err()
                {
                    service.active_connections.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    }
    while service.active_connections.load(Ordering::Acquire) != 0 {
        thread::sleep(Duration::from_millis(10));
    }
}

struct ConnectionPermit<'a>(&'a AtomicUsize);

impl Drop for ConnectionPermit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn handle_connection(mut stream: TcpStream, service: &ServiceState) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    // The served definition is the single frame-bound source: what the
    // transport admits is exactly what the engine negotiated to serve.
    let max_frame_bytes = service.engine.definition().max_message_bytes;
    let request = match read_http_request(&mut stream, max_frame_bytes) {
        Ok(request) => request,
        Err(_) => return write_http(&mut stream, 400, &[], b""),
    };
    // Admission happens before any effect: exact numeric loopback Host, no
    // browser Origin, then the bearer client identity. A browser-origin or
    // misdirected request never reaches token comparison or session state.
    let expected_host = service.address.to_string();
    if request.headers.get("host").map(String::as_str) != Some(expected_host.as_str()) {
        return write_http(&mut stream, 400, &[], b"");
    }
    if request.headers.contains_key("origin") {
        return write_http(&mut stream, 400, &[], b"");
    }
    let Some(token) = request
        .headers
        .get("authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return write_http(&mut stream, 401, &[], b"");
    };
    if request.path == "/control/stop" {
        if request.method != "POST" || !request.body.is_empty() {
            return write_http(&mut stream, 400, &[], b"");
        }
        if !constant_time_eq(token, &service.control_token) {
            return write_http(&mut stream, 401, &[], b"");
        }
        service.stop.store(true, Ordering::Release);
        return write_http(&mut stream, 202, &[], b"");
    }
    if service.context_fields.iter().any(|field| {
        request
            .headers
            .get(field.header.as_str())
            .is_some_and(|value| !valid_context_header(value))
    }) {
        return write_http(&mut stream, 400, &[], b"");
    }
    let Some(provider) = service
        .tokens
        .iter()
        .find_map(|(provider, expected)| (constant_time_eq(token, expected)).then_some(provider))
    else {
        return write_http(&mut stream, 401, &[], b"");
    };
    if request.method == "GET" && request.path == "/health" {
        return write_http(&mut stream, 204, &[], b"");
    }
    if request.method == "DELETE" && request.path == "/mcp" {
        let Some(session_id) = request.headers.get("mcp-session-id") else {
            return write_http(&mut stream, 400, &[], b"");
        };
        let mut sessions = service
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reap_idle_sessions(&mut sessions, Instant::now(), service.session_idle_ttl());
        let removed = sessions
            .get(session_id)
            .is_some_and(|session| session.provider == *provider)
            && sessions.remove(session_id).is_some();
        return write_http(&mut stream, if removed { 204 } else { 404 }, &[], b"");
    }
    if request.method != "POST" || request.path != "/mcp" {
        return write_http(&mut stream, 404, &[], b"");
    }
    if request.headers.get("content-type").map(String::as_str) != Some("application/json") {
        return write_http(&mut stream, 415, &[], b"");
    }
    let message = match decode_http_body(&request.body, max_frame_bytes) {
        Ok(message) => message,
        Err(_) => return write_http(&mut stream, 400, &[], b""),
    };
    let initialize =
        matches!(&message, McpMessage::Request { method, .. } if method == "initialize");
    // The protocol revision is admitted before effects: a present header must
    // be one of the server's explicit revisions, and every post-initialize
    // request must carry the revision negotiated for that session.
    let request_protocol_revision = request
        .headers
        .get("mcp-protocol-version")
        .map(String::as_str);
    match request_protocol_revision {
        Some(revision)
            if service
                .engine
                .definition()
                .supports_protocol_revision(revision) => {}
        Some(_) => return write_http(&mut stream, 400, &[], b""),
        None if initialize => {}
        None => return write_http(&mut stream, 400, &[], b""),
    }
    let requested_session = request.headers.get("mcp-session-id").cloned();
    let fresh_session = initialize && requested_session.is_none();
    let (session_id, session, binding) = if fresh_session {
        let mut sessions = service
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reap_idle_sessions(&mut sessions, Instant::now(), service.session_idle_ttl());
        if sessions.len() >= MAX_SESSIONS {
            return write_http(&mut stream, 503, &[], b"");
        }
        let id = uuid::Uuid::new_v4().simple().to_string();
        let session = Arc::new(McpSessionState::default());
        let binding = merge_session_caller_binding(
            &SessionCallerBinding::default(),
            &request.headers,
            &service.context_fields,
        );
        sessions.insert(
            id.clone(),
            ServerSession {
                provider: provider.clone(),
                state: Arc::clone(&session),
                caller: Mutex::new(binding.clone()),
                last_seen: Instant::now(),
                in_flight: 1,
            },
        );
        (id, session, binding)
    } else {
        let Some(id) = requested_session else {
            return write_http(&mut stream, 400, &[], b"");
        };
        let mut sessions = service
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reap_idle_sessions(&mut sessions, Instant::now(), service.session_idle_ttl());
        let Some(stored) = sessions
            .get_mut(&id)
            .filter(|session| session.provider == *provider)
        else {
            return write_http(&mut stream, 404, &[], b"");
        };
        stored.last_seen = Instant::now();
        stored.in_flight += 1;
        let session = Arc::clone(&stored.state);
        let binding = {
            let mut guard = stored
                .caller
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            *guard =
                merge_session_caller_binding(&guard, &request.headers, &service.context_fields);
            guard.clone()
        };
        (id, session, binding)
    };
    // The admitted request pins its session row until the answer leaves:
    // the reaper keeps any session whose work is still outstanding.
    let _in_flight = SessionInFlight {
        sessions: &service.sessions,
        id: session_id.clone(),
    };
    if !initialize && session.protocol_revision() != request_protocol_revision {
        return write_http(&mut stream, 400, &[], b"");
    }
    // The bounded tool-effect permit is claimed only after the request has
    // proven its identity, revision, and session, so invalid calls can never
    // occupy an effect slot.
    let _tool_permit = if matches!(&message, McpMessage::Request { method, .. } if method == "tools/call")
    {
        let control = matches!(&message, McpMessage::Request { params: Some(params), .. }
            if params
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| service.application.is_control_tool(name)));
        let (counter, limit) = if control {
            (
                &service.active_control_calls,
                service.application.control_limit(),
            )
        } else {
            (&service.active_tool_calls, MAX_TOOL_WORKERS)
        };
        match ToolPermit::acquire(counter, limit) {
            Some(permit) => Some(permit),
            None => return write_http(&mut stream, 503, &[], b""),
        }
    } else {
        None
    };
    let caller = CallerContext {
        provider_id: provider.clone(),
        attributes: binding,
        authenticated: true,
    };
    match service.engine.handle(&session, &caller, message) {
        Some(response) => {
            let body = encode_http_body(&response, max_frame_bytes)?;
            // A session minted for this handshake lives only while
            // initialize succeeded: a refused handshake frees its slot at
            // once, so retrying bad handshakes can never exhaust the table
            // and a crashed connector cannot leak a usable id.
            let mut session_live = true;
            if fresh_session && !session.initialized() {
                service
                    .sessions
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .remove(&session_id);
                session_live = false;
            }
            let response_protocol_revision = session
                .protocol_revision()
                .unwrap_or(service.engine.definition().protocol_revision);
            if session_live {
                write_http(
                    &mut stream,
                    200,
                    &[
                        ("content-type", "application/json"),
                        ("mcp-session-id", session_id.as_str()),
                        ("mcp-protocol-version", response_protocol_revision),
                    ],
                    &body,
                )
            } else {
                write_http(
                    &mut stream,
                    200,
                    &[("content-type", "application/json")],
                    &body,
                )
            }
        }
        None => write_http(
            &mut stream,
            202,
            &[("mcp-session-id", session_id.as_str())],
            b"",
        ),
    }
}

struct ToolPermit<'a>(&'a AtomicUsize);

impl<'a> ToolPermit<'a> {
    fn acquire(counter: &'a AtomicUsize, limit: usize) -> Option<Self> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < limit).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(counter))
    }
}

impl Drop for ToolPermit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn read_http_request(stream: &mut TcpStream, max_body_bytes: usize) -> Result<HttpRequest> {
    let mut bytes = Vec::with_capacity(1024);
    let header_end = loop {
        if bytes.len() >= MAX_HTTP_HEADER_BYTES {
            return Err(anyhow!("http_headers_too_large"));
        }
        let mut buffer = [0u8; 1024];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(anyhow!("http_request_incomplete"));
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = index + 4;
            if header_end > MAX_HTTP_HEADER_BYTES {
                return Err(anyhow!("http_headers_too_large"));
            }
            break header_end;
        }
    };
    let head = std::str::from_utf8(&bytes[..header_end - 4])?;
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow!("http_request_invalid"))?;
    let mut request_parts = request_line.split(' ');
    let method = request_parts.next().unwrap_or_default().to_owned();
    let path = request_parts.next().unwrap_or_default().to_owned();
    if request_parts.next() != Some("HTTP/1.1") || request_parts.next().is_some() {
        return Err(anyhow!("http_request_invalid"));
    }
    let mut headers = HashMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("http_header_invalid"))?;
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() || headers.insert(name, value.trim().to_owned()).is_some() {
            return Err(anyhow!("http_header_invalid"));
        }
    }
    if headers.contains_key("transfer-encoding") {
        return Err(anyhow!("http_chunked_unsupported"));
    }
    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    if content_length > max_body_bytes {
        return Err(anyhow!("http_body_too_large"));
    }
    let mut body = bytes[header_end..].to_vec();
    while body.len() < content_length {
        let mut buffer = [0u8; 4096];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(anyhow!("http_body_incomplete"));
        }
        body.extend_from_slice(&buffer[..count]);
        if body.len() > content_length {
            return Err(anyhow!("http_body_overrun"));
        }
    }
    if body.len() != content_length {
        return Err(anyhow!("http_body_invalid"));
    }
    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn write_http(
    stream: &mut TcpStream,
    status: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        415 => "Unsupported Media Type",
        503 => "Service Unavailable",
        _ => "Error",
    };
    write!(stream, "HTTP/1.1 {status} {reason}\r\n")?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    write!(
        stream,
        "content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

/// Owner path for publishing discovery. Constructing it creates and hardens
/// the state root, so it belongs to the service that owns that state — never
/// to a reader.
fn discovery_path() -> Result<PathBuf> {
    let root = private_state::state_root()?;
    private_state::ensure_private_dir(&root)?;
    Ok(root.join("discovery.json"))
}

/// Reader path for discovery. Resolves lexically and never creates the state
/// root, so reading is side-effect free even when nothing has been published
/// yet. A refused connector must be able to report why without leaving the
/// directories it was refused before creating.
pub(crate) fn discovery_path_read_only() -> Result<PathBuf> {
    Ok(private_state::state_root_read_only()?.join("discovery.json"))
}

fn write_discovery(path: &Path, document: &DiscoveryDocument) -> Result<()> {
    let text = serde_json::to_string(document)?;
    private_state::atomic_write_private_text_bounded(path, &text, 16 * 1024)
}

pub(crate) fn read_discovery(path: &Path) -> Result<DiscoveryDocument> {
    let text = private_state::read_existing_private_text_bounded(path, 16 * 1024)?
        .ok_or_else(|| anyhow!("mcpd_discovery_unavailable"))?;
    let document: DiscoveryDocument = serde_json::from_str(&text)?;
    if !valid_discovery_document(&document) {
        return Err(anyhow!("mcpd_discovery_invalid"));
    }
    Ok(document)
}

fn valid_discovery_document(document: &DiscoveryDocument) -> bool {
    let port = document
        .endpoint
        .strip_prefix("http://127.0.0.1:")
        .and_then(|value| value.strip_suffix("/mcp"));
    let valid_port = port.is_some_and(|value| {
        !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && value
                .parse::<u16>()
                .ok()
                .is_some_and(|port| port > 0 && port.to_string() == value)
    });
    document.schema_version == DISCOVERY_SCHEMA
        && valid_port
        && lowercase_hex(&document.generation, 32)
        && lowercase_hex(&document.control_token, 64)
        && !document.tokens.is_empty()
        && document
            .tokens
            .iter()
            .all(|(provider, token)| valid_caller_id(provider) && lowercase_hex(token, 64))
        && document.context_headers.len() <= MAX_CONTEXT_FIELDS
        && document
            .context_headers
            .iter()
            .all(|field| field.validate().is_ok())
        && document.effect_tools.len() <= MAX_PUBLISHED_TOOLS
        && document
            .effect_tools
            .iter()
            .all(|tool| valid_tool_name(tool))
}

fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn cleanup_discovery_generation(path: &Path, generation: &str) {
    let owned = private_state::read_existing_private_text_bounded(path, 16 * 1024)
        .ok()
        .flatten()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|value| {
            value
                .get("generation")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|observed| observed == generation);
    if owned {
        let _ = fs::remove_file(path);
    }
}

/// Why a connector could not obtain its authenticated loopback identity.
///
/// The two causes need different user actions, so they are never collapsed
/// into one opaque failure: an unreachable service is an installation or
/// lifecycle problem, while a caller without a seat is simply not admitted
/// and will not become admitted by retrying.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorDiscoveryError {
    Unavailable,
    /// The document is valid but admits no seat for this caller. The payload
    /// is the caller set it does admit, in caller-id order.
    CallerNotSupported(Vec<String>),
}

impl ConnectorDiscoveryError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Unavailable => "mcpd_discovery_unavailable",
            Self::CallerNotSupported(_) => "mcpd_caller_unsupported",
        }
    }
}

impl std::fmt::Display for ConnectorDiscoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

/// The callers the currently published service admits, in caller-id order.
/// Diagnostics only: absent, stale, or unreadable discovery yields an empty
/// list rather than an error, because it must never change an exit path.
pub fn published_callers() -> Vec<String> {
    discovery_path_read_only()
        .ok()
        .and_then(|path| read_discovery(&path).ok())
        .map(|document| {
            let mut providers = document.tokens.into_keys().collect::<Vec<_>>();
            providers.sort();
            providers
        })
        .unwrap_or_default()
}

pub fn load_connector_discovery(
    provider: &str,
) -> Result<ConnectorDiscovery, ConnectorDiscoveryError> {
    // A connector only ever reads discovery; the service owns creating it.
    let path = discovery_path_read_only().map_err(|_| ConnectorDiscoveryError::Unavailable)?;
    let document = read_discovery(&path).map_err(|_| ConnectorDiscoveryError::Unavailable)?;
    // Membership is decided by the published capability set, not by a list of
    // known names compiled into this binary.
    let Some(token) = document.tokens.get(provider).cloned() else {
        let mut supported = document.tokens.into_keys().collect::<Vec<_>>();
        supported.sort();
        return Err(ConnectorDiscoveryError::CallerNotSupported(supported));
    };
    Ok(ConnectorDiscovery {
        endpoint: document.endpoint,
        bearer_token: token,
        context_headers: document.context_headers,
        effect_tools: document.effect_tools,
    })
}

/// The caller-side deadlines one exchange admits: the connector lets a
/// live call run as long as the service answers, while the health probe
/// stays tightly bounded so the monitor responds quickly.
struct ExchangeDeadline {
    connect: Duration,
    read: Option<Duration>,
    write: Option<Duration>,
}

/// Why an exchange failed before the caller could see a status line —
/// call sites keep only their own error naming.
enum ExchangeFailure {
    Transport,
    ResponseTooLarge,
}

/// The one client-side wire contract, shared by the connector and the
/// service's own health probe so the two can never drift: one POST to the
/// endpoint with the bearer identity, the negotiated revision header, an
/// optional session id, the declared context headers when the caller asks
/// for them, and a reply read bounded by the definition's own frame bound.
fn exchange(
    discovery: &ConnectorDiscovery,
    session_id: Option<&str>,
    protocol_revision: &str,
    body: &[u8],
    definition: &McpServerDefinition,
    deadline: ExchangeDeadline,
    forward_context: bool,
) -> Result<(u16, Option<String>, Vec<u8>), ExchangeFailure> {
    let mut builder = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(deadline.connect);
    if let Some(read) = deadline.read {
        builder = builder.timeout_read(read);
    }
    if let Some(write) = deadline.write {
        builder = builder.timeout_write(write);
    }
    let mut request = builder
        .build()
        .post(&discovery.endpoint)
        .set(
            "authorization",
            &format!("Bearer {}", discovery.bearer_token),
        )
        .set("content-type", "application/json")
        .set("accept", "application/json")
        .set("mcp-protocol-version", protocol_revision);
    // Forward each declared context field whose environment variable is set:
    // the mapping arrives inside the published discovery document, so the
    // connector never carries its own copy.
    if forward_context {
        for field in &discovery.context_headers {
            if let Ok(value) = std::env::var(&field.env)
                && valid_context_header(&value)
            {
                request = request.set(&field.header, &value);
            }
        }
    }
    if let Some(session_id) = session_id {
        request = request.set("mcp-session-id", session_id);
    }
    let response = match request.send_bytes(body) {
        Ok(response) => response,
        Err(ureq::Error::Status(_, response)) => response,
        Err(_) => return Err(ExchangeFailure::Transport),
    };
    let status = response.status();
    let session = response.header("mcp-session-id").map(str::to_owned);
    let mut body = Vec::new();
    response
        .into_reader()
        .take((definition.max_message_bytes + 1) as u64)
        .read_to_end(&mut body)
        .map_err(|_| ExchangeFailure::Transport)?;
    if body.len() > definition.max_message_bytes {
        return Err(ExchangeFailure::ResponseTooLarge);
    }
    Ok((status, session, body))
}

/// The connector's exchange with the served endpoint. `definition` carries
/// the revision set and frame bound the client enforces — the same
/// `McpServerDefinition` predicate the serving side checks, so server and
/// client share one rule instead of two copies that could drift.
pub fn connector_exchange(
    discovery: &ConnectorDiscovery,
    session_id: Option<&str>,
    protocol_revision: &str,
    body: &[u8],
    definition: &McpServerDefinition,
) -> Result<(u16, Option<String>, Vec<u8>)> {
    if !definition.supports_protocol_revision(protocol_revision) {
        return Err(anyhow!("mcpd_protocol_revision_invalid"));
    }
    exchange(
        discovery,
        session_id,
        protocol_revision,
        body,
        definition,
        ExchangeDeadline {
            connect: Duration::from_secs(2),
            read: None,
            write: None,
        },
        true,
    )
    .map_err(|failure| {
        anyhow!(match failure {
            ExchangeFailure::Transport => "mcpd_connector_transport_failed",
            ExchangeFailure::ResponseTooLarge => "mcpd_connector_response_too_large",
        })
    })
}

pub fn connector_close_session(discovery: &ConnectorDiscovery, session_id: &str) -> Result<()> {
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(Duration::from_secs(2))
        .timeout_read(Duration::from_secs(3))
        .timeout_write(Duration::from_secs(3))
        .build();
    let response = agent
        .delete(&discovery.endpoint)
        .set(
            "authorization",
            &format!("Bearer {}", discovery.bearer_token),
        )
        .set("mcp-session-id", session_id)
        .call();
    match response {
        Ok(response) if response.status() == 204 => Ok(()),
        _ => Err(anyhow!("mcpd_connector_close_failed")),
    }
}

/// Bounded health exchange used only by the service's own probe. Tight
/// deadlines keep the monitor responsive, the probe never carries caller
/// context, and it speaks the served definition's own revision — a
/// host-built definition is probed on its own terms.
fn health_exchange(
    discovery: &ConnectorDiscovery,
    session_id: Option<&str>,
    body: &[u8],
    definition: &McpServerDefinition,
) -> Result<(u16, Option<String>, Vec<u8>)> {
    exchange(
        discovery,
        session_id,
        definition.protocol_revision,
        body,
        definition,
        ExchangeDeadline {
            connect: Duration::from_secs(1),
            read: Some(Duration::from_secs(2)),
            write: Some(Duration::from_secs(2)),
        },
        false,
    )
    .map_err(|failure| {
        anyhow!(match failure {
            ExchangeFailure::Transport => "mcpd_health_transport_failed",
            ExchangeFailure::ResponseTooLarge => "mcpd_health_response_too_large",
        })
    })
}

fn valid_context_header(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'.' | b'_' | b'-'))
}

/// Readiness observes transport only; no tool invocation or backend probe
/// runs.
pub fn connector_health(discovery: &ConnectorDiscovery) -> bool {
    let Some(endpoint) = discovery.endpoint.strip_suffix("/mcp") else {
        return false;
    };
    ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(Duration::from_secs(2))
        .timeout_read(Duration::from_secs(2))
        .build()
        .get(&format!("{endpoint}/health"))
        .set(
            "authorization",
            &format!("Bearer {}", discovery.bearer_token),
        )
        .call()
        .is_ok_and(|response| response.status() == 204)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InProcessBackend, Registration, ToolSpec};

    fn context_fields() -> Vec<ContextField> {
        vec![
            ContextField::new(
                "conversationId",
                "x-fixture-conversation",
                "FIXTURE_CONVERSATION",
            )
            .unwrap(),
            ContextField::new("membershipId", "x-fixture-membership", "FIXTURE_MEMBERSHIP")
                .unwrap(),
            ContextField::new(
                "parentDispatchId",
                "x-fixture-parent-dispatch",
                "FIXTURE_PARENT_DISPATCH",
            )
            .unwrap(),
        ]
    }

    /// A `ServerSession` fixture carrying exactly the fields the reaper
    /// predicate reads: last-touch instant and outstanding request count.
    fn stored_session(last_seen: Instant, in_flight: usize) -> ServerSession {
        ServerSession {
            provider: "fixture".to_owned(),
            state: Arc::new(McpSessionState::default()),
            caller: Mutex::new(SessionCallerBinding::new()),
            last_seen,
            in_flight,
        }
    }

    #[test]
    fn the_reaper_keeps_fresh_and_in_flight_sessions() {
        let base = Instant::now();
        let ttl = Duration::from_secs(60);
        let mut sessions = HashMap::from([
            ("old-idle".to_owned(), stored_session(base, 0)),
            ("old-busy".to_owned(), stored_session(base, 1)),
            (
                "fresh-idle".to_owned(),
                stored_session(base + Duration::from_secs(58), 0),
            ),
        ]);
        // Past the TTL an idle row is collected; outstanding work pins its
        // row no matter how stale the clock reads, and a young idle row is
        // untouched.
        reap_idle_sessions(&mut sessions, base + ttl + Duration::from_secs(1), ttl);
        assert!(!sessions.contains_key("old-idle"));
        assert!(sessions.contains_key("old-busy"));
        assert!(sessions.contains_key("fresh-idle"));
    }

    /// One caller, one tool — the smallest dispatcher `HttpService` serves.
    /// The caller-supplied handler decides what the backend does.
    fn fixture_service(handler: InProcessBackend) -> (HttpService, McpServerDefinition) {
        let dispatcher = Dispatcher::from_registrations(vec![
            Registration::new(
                "fixture",
                "1",
                vec![
                    ToolSpec::declared(
                        "fixture_tool",
                        None,
                        serde_json::json!({"type":"object","additionalProperties":false,"properties":{}}),
                        false,
                        false,
                    )
                    .unwrap(),
                ],
                vec!["fixture".to_owned()],
                handler,
            )
            .unwrap(),
        ])
        .unwrap();
        let definition = McpServerDefinition {
            protocol_revision: crate::application::PROTOCOL_REVISION,
            compatible_protocol_revisions: crate::application::COMPATIBLE_PROTOCOL_REVISIONS,
            server_name: "fixture-service",
            server_version: "1",
            max_message_bytes: crate::application::MAX_MCP_FRAME_BYTES,
        };
        (
            HttpService::start(dispatcher, definition.clone(), Vec::new()).unwrap(),
            definition,
        )
    }

    /// A unique canonicalized state root installed as the override — the
    /// caller holds `STATE_ROOT_TEST_LOCK` for the whole test.
    fn fixture_root(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("mcpd-{name}-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        crate::private_state::set_state_root_override(root.clone());
        root
    }

    /// One exchange, retrying only a connection the boundary shed before any
    /// answer — the bounded-connection cap legitimately drops what it cannot
    /// schedule under parallel test load, which says nothing about the
    /// session behavior under test.
    fn exchange_or_retry(
        discovery: &ConnectorDiscovery,
        session: Option<&str>,
        body: &[u8],
        definition: &McpServerDefinition,
    ) -> (u16, Option<String>, Vec<u8>) {
        for _ in 0..4 {
            match connector_exchange(
                discovery,
                session,
                crate::application::PROTOCOL_REVISION,
                body,
                definition,
            ) {
                Ok(response) => return response,
                Err(_) => thread::sleep(Duration::from_millis(25)),
            }
        }
        connector_exchange(
            discovery,
            session,
            crate::application::PROTOCOL_REVISION,
            body,
            definition,
        )
        .expect("the exchange must eventually connect")
    }

    fn initialize_body(revision: &str) -> Vec<u8> {
        encode_http_body(
            &McpMessage::request(
                1_i64,
                "initialize",
                serde_json::json!({
                    "protocolVersion": revision,
                    "capabilities": {},
                    "clientInfo": {"name": "fixture", "version": "1"}
                })
                .as_object()
                .cloned(),
            )
            .unwrap(),
            crate::application::MAX_MCP_FRAME_BYTES,
        )
        .unwrap()
    }

    #[test]
    fn a_refused_initialize_frees_its_session_slot() {
        // A handshake the engine refuses is answered 200 with the JSON-RPC
        // error and frees the slot it minted: retrying bad handshakes can
        // never exhaust the bounded table.
        let _serial = crate::private_state::STATE_ROOT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let root = fixture_root("init-slot");
        let (service, definition) = fixture_service(InProcessBackend::new(|request| {
            Ok(serde_json::json!({"tool": request.tool}))
        }));
        let discovery = load_connector_discovery("fixture").unwrap();

        let refused = initialize_body("1999-01-01");
        for _ in 0..MAX_SESSIONS + 4 {
            let (status, session, _) = exchange_or_retry(&discovery, None, &refused, &definition);
            assert_eq!(status, 200, "the refusal is a protocol error, not 503");
            assert!(
                session.is_none(),
                "a refused initialize must not keep its slot"
            );
        }
        // The table never filled: a valid handshake still mints a session.
        let (status, session, _) = exchange_or_retry(
            &discovery,
            None,
            &initialize_body(crate::application::PROTOCOL_REVISION),
            &definition,
        );
        assert_eq!(status, 200);
        assert!(session.is_some(), "a valid initialize must still mint");

        drop(service);
        crate::private_state::clear_state_root_override();
        fs::remove_dir_all(&root).unwrap();
    }

    /// The other half of the fix: a request still executing past the idle
    /// TTL pins its session row, so the reaper never deletes a session whose
    /// answer is still owed.
    #[test]
    fn an_in_flight_call_pins_its_session_against_the_reaper() {
        let _serial = crate::private_state::STATE_ROOT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let root = fixture_root("in-flight");
        let (started, started_rx) = std::sync::mpsc::channel::<()>();
        let release = Arc::new(std::sync::Barrier::new(2));
        let (service, definition) = fixture_service(InProcessBackend::new({
            let release = Arc::clone(&release);
            move |_| {
                let _ = started.send(());
                release.wait();
                Ok(serde_json::json!({}))
            }
        }));
        let discovery = load_connector_discovery("fixture").unwrap();

        // Mint the session, then expire the TTL for anything idle.
        let (_, session, _) = exchange_or_retry(
            &discovery,
            None,
            &initialize_body(crate::application::PROTOCOL_REVISION),
            &definition,
        );
        let session = session.expect("initialize mints a session");
        service.set_session_idle_ttl(Duration::from_millis(60));

        // A session-bound call that runs well past the TTL.
        let call_body = encode_http_body(
            &McpMessage::request(
                2_i64,
                "tools/call",
                serde_json::json!({"name":"fixture_tool","arguments":{}})
                    .as_object()
                    .cloned(),
            )
            .unwrap(),
            crate::application::MAX_MCP_FRAME_BYTES,
        )
        .unwrap();
        let call = std::thread::spawn({
            let discovery = discovery.clone();
            let session = session.clone();
            let definition = definition.clone();
            move || exchange_or_retry(&discovery, Some(&session), &call_body, &definition)
        });
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the call is in-flight");
        thread::sleep(Duration::from_millis(100));

        // The reap this admission runs must keep the pinned row: a second
        // session-bound frame is still served, never 404.
        let list_body = encode_http_body(
            &McpMessage::request(3_i64, "tools/list", Some(Map::new())).unwrap(),
            crate::application::MAX_MCP_FRAME_BYTES,
        )
        .unwrap();
        let (status, _, _) = exchange_or_retry(&discovery, Some(&session), &list_body, &definition);
        assert_eq!(status, 200, "an in-flight session must survive the reaper");

        release.wait();
        let (status, _, _) = call.join().unwrap();
        assert_eq!(status, 200, "the pinned call completes normally");

        // Released and idle again: the next table access collects the row.
        thread::sleep(Duration::from_millis(100));
        let (status, _, _) = exchange_or_retry(&discovery, Some(&session), &list_body, &definition);
        assert_eq!(status, 404, "an idle unpinned session is reaped");

        drop(service);
        crate::private_state::clear_state_root_override();
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn tokens_compare_without_prefix_acceptance() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abc0"));
    }

    #[test]
    fn caller_context_headers_are_closed_identifiers() {
        assert!(valid_context_header("membership:fixture-1"));
        assert!(!valid_context_header("../private"));
        assert!(!valid_context_header("line\nbreak"));
    }

    #[test]
    fn later_request_reuses_session_caller_binding() {
        let fields = context_fields();
        let mut initial = SessionCallerBinding::new();
        initial.insert("conversationId".to_owned(), "conversation:one".to_owned());
        initial.insert("membershipId".to_owned(), "membership:caller".to_owned());
        let reused = merge_session_caller_binding(&initial, &HashMap::new(), &fields);
        assert_eq!(
            reused.get("conversationId").map(String::as_str),
            Some("conversation:one")
        );
        assert_eq!(
            reused.get("membershipId").map(String::as_str),
            Some("membership:caller")
        );
        let mut headers = HashMap::new();
        headers.insert(
            "x-fixture-conversation".to_owned(),
            "conversation:two".to_owned(),
        );
        let overridden = merge_session_caller_binding(&initial, &headers, &fields);
        assert_eq!(
            overridden.get("conversationId").map(String::as_str),
            Some("conversation:two")
        );
        assert_eq!(
            overridden.get("membershipId").map(String::as_str),
            Some("membership:caller")
        );
        // Undeclared headers never enter the caller scope.
        let mut foreign = HashMap::new();
        foreign.insert("x-other".to_owned(), "value".to_owned());
        let merged = merge_session_caller_binding(&initial, &foreign, &fields);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn discovery_admission_is_exact_and_lowercase() {
        let valid = || DiscoveryDocument {
            control_token: "c".repeat(64),
            schema_version: DISCOVERY_SCHEMA.to_owned(),
            endpoint: "http://127.0.0.1:34567/mcp".to_owned(),
            generation: "a".repeat(32),
            tokens: HashMap::from([
                ("antigravity".to_owned(), "b".repeat(64)),
                ("claude-code".to_owned(), "a".repeat(64)),
                ("codex".to_owned(), "c".repeat(64)),
                ("cursor".to_owned(), "d".repeat(64)),
            ]),
            context_headers: context_fields(),
            effect_tools: vec!["delegate".to_owned()],
        };
        assert!(valid_discovery_document(&valid()));
        for endpoint in [
            "http://localhost:34567/mcp",
            "http://127.0.0.1:034567/mcp",
            "http://127.0.0.1:34567/mcp?extra=1",
            "http://127.0.0.1:34567/other",
        ] {
            let mut document = valid();
            document.endpoint = endpoint.to_owned();
            assert!(!valid_discovery_document(&document));
        }
        let mut uppercase = valid();
        uppercase.tokens.insert("codex".to_owned(), "A".repeat(64));
        assert!(!valid_discovery_document(&uppercase));
        let mut truncated = valid();
        truncated.tokens.insert("codex".to_owned(), "c".repeat(63));
        assert!(!valid_discovery_document(&truncated));
        let mut empty = valid();
        empty.tokens.clear();
        assert!(!valid_discovery_document(&empty));
        let mut malformed_id = valid();
        malformed_id
            .tokens
            .insert("Codex CLI".to_owned(), "e".repeat(64));
        assert!(!valid_discovery_document(&malformed_id));
        let mut bad_header = valid();
        bad_header.context_headers.push(ContextField {
            attribute: "ok".to_owned(),
            header: "Not-A-Header".to_owned(),
            env: "OK_ENV".to_owned(),
        });
        assert!(!valid_discovery_document(&bad_header));
        let mut bad_tool = valid();
        bad_tool.effect_tools.push("not a tool".to_owned());
        assert!(!valid_discovery_document(&bad_tool));
    }

    /// The token map is the capability set, so a caller the reader does not
    /// recognise is admitted structurally. Deciding that a caller has no seat
    /// is the reader's job, not a reason to reject the whole document.
    #[test]
    fn discovery_admits_a_caller_set_the_reader_does_not_know() {
        let mut document = DiscoveryDocument {
            control_token: "c".repeat(64),
            schema_version: DISCOVERY_SCHEMA.to_owned(),
            endpoint: "http://127.0.0.1:34567/mcp".to_owned(),
            generation: "a".repeat(32),
            tokens: HashMap::from([("some-new-caller".to_owned(), "b".repeat(64))]),
            context_headers: Vec::new(),
            effect_tools: Vec::new(),
        };
        assert!(valid_discovery_document(&document));
        assert!(valid_caller_id("some-new-caller"));
        assert!(!valid_caller_id("Some New Caller"));
        assert!(!valid_caller_id(""));

        document.tokens.clear();
        assert!(!valid_discovery_document(&document));
    }
}
