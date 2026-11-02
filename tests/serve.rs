//! Process contract proof over a synthetic framed-protocol backend.
//!
//! The subject is the real `flr` binary serving declaratively-registered
//! services; every backend session is the deterministic `flr-stub-backend`
//! fixture — an echo responder with no tools, no network and no agent. The
//! proofs cover the union catalog, per-registration caller admission, the
//! reserved control channel, connector reload behaviour and lifecycle
//! custody, never a real Agent conversation.

#![cfg(unix)]

use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

const FLR: &str = env!("CARGO_BIN_EXE_flr");
const STUB: &str = env!("CARGO_BIN_EXE_flr-stub-backend");

struct Fixture {
    root: PathBuf,
    home: PathBuf,
    stub_dir: PathBuf,
    config: PathBuf,
}

fn service_block(name: &str, callers: &str, control: &str, tools: &str) -> String {
    format!(
        r#"[[service]]
name = "{name}"
callers = [{callers}]
{control}
[service.backend]
program = "{STUB}"
args = []
protocol = "flr.stdio.v1"
invoke = ["echo", "{{request}}"]
{tools}
"#
    )
}

fn tool(name: &str, effect: bool) -> String {
    format!(
        r#"[[service.tools]]
name = "{name}"
effect = {effect}
input_schema_json = '''
{{"type":"object","additionalProperties":false,
 "properties":{{"text":{{"type":"string","minLength":1,"maxLength":1024}}}},
 "required":[]}}
'''
"#
    )
}

impl Fixture {
    fn new() -> Self {
        Self::with_config(None)
    }

    fn with_config(config: Option<String>) -> Self {
        let root = std::env::temp_dir().join(format!("flr-contract-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        // Canonicalize so macOS /var→/private/var symlinks never confuse the
        // link-rejecting custody checks.
        let root = root.canonicalize().unwrap();
        let home = root.join("home");
        let stub_dir = root.join("stub");
        fs::create_dir_all(&stub_dir).unwrap();
        let text = config.unwrap_or_else(|| {
            format!(
                r#"[server]
name = "fixture-dispatcher"
version = "0.1.0"

[[context]]
attribute = "conversationId"
header = "x-fixture-conversation"
env = "FIXTURE_CONVERSATION"

{alpha}
{beta}
"#,
                alpha = service_block(
                    "alpha",
                    r#""fixture", "other-caller""#,
                    r#"control = ["fixture_cancel"]"#,
                    &[
                        tool("fixture_echo", true),
                        tool("fixture_hold", false),
                        tool("fixture_cancel", true),
                    ]
                    .concat(),
                ),
                beta = service_block("beta", r#""other-caller""#, "", &tool("beta_echo", false),),
            )
        });
        let config = root.join("services.toml");
        fs::write(&config, text).unwrap();
        Self {
            root,
            home,
            stub_dir,
            config,
        }
    }

    fn service(&self, args: &[&str]) -> std::process::Output {
        Command::new(FLR)
            .args(args)
            .env("FLR_HOME", &self.home)
            .env("FLR_STUB_DIR", &self.stub_dir)
            .env("FLR_STUB_MARKER", "env-alive")
            .output()
            .unwrap()
    }

    fn lifecycle(&self, action: &str) -> Value {
        let mut args = vec!["service", action];
        if matches!(action, "start" | "reload") {
            args.extend(["--config", self.config.to_str().unwrap()]);
        }
        let output = self.service(&args);
        assert!(
            output.status.success(),
            "service {action} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn connector(&self, caller: &str) -> Child {
        Command::new(FLR)
            .args(["connector", "--caller", caller])
            .env("FLR_HOME", &self.home)
            .env("FIXTURE_CONVERSATION", "conversation:one")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Command::new(FLR)
            .args(["service", "stop"])
            .env("FLR_HOME", &self.home)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn exchange(input: &mut impl Write, output: &mut impl BufRead, value: Value) -> Value {
    writeln!(input, "{value}").unwrap();
    input.flush().unwrap();
    let mut line = String::new();
    assert!(output.read_line(&mut line).unwrap() > 0);
    serde_json::from_str(&line).unwrap()
}

fn initialize(input: &mut impl Write, output: &mut impl BufRead) -> Value {
    exchange(
        input,
        output,
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}),
    )
}

fn wait_for(path: &Path, what: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !path.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "fixture did not produce {what}"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn union_catalog_per_caller_admission_and_caller_scope() {
    let fixture = Fixture::new();
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let mut connector = fixture.connector("fixture");
    let mut input = connector.stdin.take().unwrap();
    let mut output = BufReader::new(connector.stdout.take().unwrap());

    let initialized = initialize(&mut input, &mut output);
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        "fixture-dispatcher"
    );

    // The union catalog: both registrations, in declaration order.
    let list = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
    );
    assert_eq!(
        list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "fixture_echo",
            "fixture_hold",
            "fixture_cancel",
            "beta_echo"
        ]
    );

    // A routed call reaches the backend with the caller scope the context
    // channel declared: providerId plus the forwarded attribute, and the
    // child inherited the real environment.
    let called = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fixture_echo","arguments":{"text":"hi"}}}),
    );
    let request = &called["result"]["structuredContent"]["request"];
    assert_eq!(request["name"], "fixture_echo");
    assert_eq!(request["arguments"]["text"], "hi");
    assert_eq!(request["caller"]["providerId"], "fixture");
    assert_eq!(request["caller"]["conversationId"], "conversation:one");
    assert!(
        request["caller"].get("authenticated").is_none(),
        "the authenticated verdict never crosses the backend boundary"
    );
    assert_eq!(called["result"]["structuredContent"]["marker"], "env-alive");

    // Per-registration admission: 'fixture' holds a seat but not on beta.
    let refused = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"beta_echo","arguments":{}}}),
    );
    assert_eq!(refused["result"]["isError"], true);
    assert_eq!(
        refused["result"]["structuredContent"]["reasonCode"],
        "remote_operation_not_allowed"
    );
    assert_eq!(refused["result"]["structuredContent"]["retryable"], false);

    // An undeclared tool name is a protocol error, never an admission error.
    let unknown = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"missing","arguments":{}}}),
    );
    assert_eq!(unknown["error"]["code"], -32601);

    // Closed schemas: an undeclared argument fails validation.
    let rejected = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"fixture_echo","arguments":{"unexpected":1}}}),
    );
    assert_eq!(rejected["error"]["code"], -32602);

    drop(input);
    assert!(connector.wait().unwrap().success());
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
}

#[test]
fn a_caller_calls_only_the_registrations_that_admit_it() {
    // Per-registration seats: 'other-caller' holds a seat on beta only, so the
    // alpha tool refuses it even though both appear in the union catalog.
    let fixture = Fixture::with_config(Some(format!(
        r#"[server]
name = "seated"
version = "0.1.0"

{alpha}
{beta}
"#,
        alpha = service_block("alpha", r#""fixture""#, "", &tool("alpha_tool", false)),
        beta = service_block("beta", r#""other-caller""#, "", &tool("beta_tool", false)),
    )));
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let mut connector = fixture.connector("other-caller");
    let mut input = connector.stdin.take().unwrap();
    let mut output = BufReader::new(connector.stdout.take().unwrap());
    initialize(&mut input, &mut output);
    let list = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
    );
    assert_eq!(
        list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["alpha_tool", "beta_tool"]
    );
    let own = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"beta_tool","arguments":{}}}),
    );
    assert_eq!(
        own["result"]["structuredContent"]["request"]["name"],
        "beta_tool"
    );
    let refused = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"alpha_tool","arguments":{}}}),
    );
    assert_eq!(refused["result"]["isError"], true);
    assert_eq!(
        refused["result"]["structuredContent"]["reasonCode"],
        "remote_operation_not_allowed"
    );
    drop(input);
    assert!(connector.wait().unwrap().success());
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
}

#[test]
fn the_control_channel_bypasses_a_held_ordinary_call() {
    let fixture = Fixture::new();
    fixture.lifecycle("start");
    let mut connector = fixture.connector("fixture");
    let mut input = connector.stdin.take().unwrap();
    let mut output = BufReader::new(connector.stdout.take().unwrap());
    initialize(&mut input, &mut output);

    fs::write(fixture.stub_dir.join("hold"), "synthetic hold").unwrap();
    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fixture_hold","arguments":{}}})
    )
    .unwrap();
    input.flush().unwrap();
    wait_for(
        &fixture.stub_dir.join("hold-started"),
        "the held call to start",
    );

    writeln!(
        input,
        "{}",
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fixture_cancel","arguments":{}}})
    )
    .unwrap();
    input.flush().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let responses = (0..2)
            .map(|_| {
                let mut line = String::new();
                output.read_line(&mut line).unwrap();
                serde_json::from_str::<Value>(&line).unwrap()
            })
            .collect::<Vec<_>>();
        send.send(responses).unwrap();
    });
    let responses = receive
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the control call must pass the held ordinary call");
    let cancel = responses.iter().find(|v| v["id"] == 3).unwrap();
    assert_eq!(
        cancel["result"]["structuredContent"]["request"]["name"],
        "fixture_cancel"
    );
    assert!(responses.iter().any(|v| v["id"] == 2));
    drop(input);
    assert!(connector.wait().unwrap().success());
}

#[test]
fn reload_renews_an_existing_connector_session() {
    let fixture = Fixture::new();
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let mut connector = fixture.connector("fixture");
    let mut input = connector.stdin.take().unwrap();
    let mut output = BufReader::new(connector.stdout.take().unwrap());
    initialize(&mut input, &mut output);
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");

    // A clean stop withdrew discovery: no frame was sent, so the outcome is a
    // plain retryable module-unavailable — never an uncertain execution.
    let unavailable = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":30,"method":"tools/call","params":{"name":"fixture_echo","arguments":{"text":"x"}}}),
    );
    assert_eq!(unavailable["id"], 30);
    assert_eq!(
        unavailable["error"]["data"]["reasonCode"],
        "mcp_module_unavailable"
    );
    assert_eq!(
        unavailable["error"]["data"]["requestMayHaveExecuted"],
        false
    );
    assert_eq!(unavailable["error"]["data"]["retryable"], true);

    // The same connector survives a reload: discovery changed, so it renews
    // the handshake on the next frame instead of dying.
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    assert_eq!(fixture.lifecycle("reload")["state"], "running");
    let call = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"fixture_echo","arguments":{"text":"again"}}}),
    );
    assert_eq!(
        call["result"]["structuredContent"]["request"]["arguments"]["text"],
        "again"
    );
    drop(input);
    assert!(connector.wait().unwrap().success());
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
}

#[test]
fn stale_discovery_after_an_owned_service_crash_marks_outcomes_and_stops() {
    let fixture = Fixture::new();
    let mut service = Command::new(FLR)
        .args(["serve", "--config", fixture.config.to_str().unwrap()])
        .env("FLR_HOME", &fixture.home)
        .env("FLR_STUB_DIR", &fixture.stub_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if fixture.lifecycle("status")["state"] == "running" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "synthetic service did not start"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }

    // A hard kill leaves stale discovery: the connector still reads it, sends
    // the request, and the lost answer makes the outcome uncertain. Effect
    // tools forbid replay; the held non-effect call may be retried.
    let mut connector = fixture.connector("fixture");
    let mut input = connector.stdin.take().unwrap();
    let mut output = BufReader::new(connector.stdout.take().unwrap());
    initialize(&mut input, &mut output);
    service.kill().unwrap();
    service.wait().unwrap();

    let uncertain = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fixture_echo","arguments":{"text":"x"}}}),
    );
    assert_eq!(
        uncertain["error"]["data"]["reasonCode"],
        "mcp_outcome_unknown"
    );
    assert_eq!(uncertain["error"]["data"]["requestMayHaveExecuted"], true);
    assert_eq!(uncertain["error"]["data"]["retryable"], false);

    let retryable = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fixture_hold","arguments":{}}}),
    );
    assert_eq!(
        retryable["error"]["data"]["reasonCode"],
        "mcp_module_unavailable"
    );
    assert_eq!(retryable["error"]["data"]["retryable"], true);

    // The stale lease must not block lifecycle custody: stop recognizes the
    // dead owner and withdraws the publication.
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
    assert!(!fixture.home.join("discovery.json").exists());
    drop(input);
    let _ = connector.wait();
}

#[test]
fn a_colliding_catalog_refuses_to_serve() {
    let fixture = Fixture::with_config(Some(format!(
        "{}\n{}",
        service_block("alpha", r#""fixture""#, "", &tool("shared_tool", false)),
        service_block("beta", r#""fixture""#, "", &tool("shared_tool", false)),
    )));
    let output = fixture.service(&["serve", "--config", fixture.config.to_str().unwrap()]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("flr_tool_name_collision"),
        "unexpected refusal: {stderr}"
    );
}

#[test]
fn an_unadmitted_caller_is_refused_before_any_frame() {
    let fixture = Fixture::new();
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let output = Command::new(FLR)
        .args(["connector", "--caller", "outsider"])
        .env("FLR_HOME", &fixture.home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not an admitted caller"), "{stderr}");
    assert!(stderr.contains("other-caller"), "{stderr}");
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
}

#[test]
fn an_effect_call_lost_after_the_backend_read_reports_outcome_unknown() {
    // The stub backend reads the framed request and exits without replying:
    // the request line left the dispatcher, so the effect call's outcome is
    // unknown and must be reported without any retry invitation.
    let fixture = Fixture::with_config(Some(format!(
        r#"[server]
name = "dropping"
version = "0.1.0"

{alpha}
"#,
        alpha = service_block(
            "alpha",
            r#""fixture""#,
            "",
            &[tool("fixture_drop", true), tool("fixture_echo", false)].concat(),
        ),
    )));
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let mut connector = fixture.connector("fixture");
    let mut input = connector.stdin.take().unwrap();
    let mut output = BufReader::new(connector.stdout.take().unwrap());
    initialize(&mut input, &mut output);

    let lost = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fixture_drop","arguments":{"text":"x"}}}),
    );
    // A successful JSON-RPC reply carrying the structured outcome-unknown
    // tool error: the backend read the request line, then died.
    assert_eq!(lost["result"]["isError"], true);
    let payload = &lost["result"]["structuredContent"];
    assert_eq!(payload["reasonCode"], "mcp_outcome_unknown");
    assert_eq!(payload["requestMayHaveExecuted"], true);
    assert_eq!(payload["retryable"], false);
    assert_eq!(payload["recovery"], "reconcile_before_retry");

    // The service stays healthy: the desynced session was dropped and the
    // next call spawns a fresh child session.
    let again = exchange(
        &mut input,
        &mut output,
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fixture_echo","arguments":{"text":"still-here"}}}),
    );
    assert_eq!(
        again["result"]["structuredContent"]["request"]["arguments"]["text"],
        "still-here"
    );
    drop(input);
    assert!(connector.wait().unwrap().success());
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
}

/// One raw HTTP request against the published endpoint — the loopback
/// boundary answers and closes (`connection: close`).
fn raw_post(
    address: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, BTreeMap<String, String>) {
    let mut request = String::from("POST /mcp HTTP/1.1\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8(raw).unwrap();
    let (head, _) = text.split_once("\r\n\r\n").unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .expect("a status line");
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    (status, headers)
}

#[test]
fn the_loopback_boundary_enforces_its_admission_contract() {
    // The published endpoint's admission contract, exercised with raw HTTP:
    // every check the boundary promises runs before session or tool state.
    let fixture = Fixture::new();
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let document: Value =
        serde_json::from_str(&fs::read_to_string(fixture.home.join("discovery.json")).unwrap())
            .unwrap();
    let endpoint = document["endpoint"].as_str().unwrap();
    let address = endpoint
        .strip_prefix("http://")
        .and_then(|rest| rest.strip_suffix("/mcp"))
        .expect("a numeric loopback endpoint");
    let token = document["tokens"]["fixture"].as_str().unwrap();
    let other_token = document["tokens"]["other-caller"].as_str().unwrap();
    let bearer = |token: &str| format!("Bearer {token}");
    let initialize_body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"raw\",\"version\":\"1\"}}}";
    let list_body = "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}";
    let json = ("content-type", "application/json");
    let revision = ("mcp-protocol-version", "2025-06-18");
    let bearer_header = bearer(token);
    let auth = ("authorization", bearer_header.as_str());

    // Exact Host: absent, or anything but the numeric loopback address.
    let (status, _) = raw_post(address, &[auth, json, revision], initialize_body);
    assert_eq!(status, 400, "a missing Host must be refused");
    for wrong in ["localhost", "127.0.0.1", "example.com"] {
        let host = ("host", wrong);
        let (status, _) = raw_post(address, &[host, auth, json, revision], initialize_body);
        assert_eq!(status, 400, "Host '{wrong}' must be refused");
    }
    let host = ("host", address);

    // Any Origin header is a browser request — refused even with a bearer.
    let (status, _) = raw_post(
        address,
        &[
            host,
            ("origin", "http://attacker.example"),
            auth,
            json,
            revision,
        ],
        initialize_body,
    );
    assert_eq!(status, 400, "an Origin must be refused");

    // Bearer identity: absent and wrong both fail before any route logic.
    let (status, _) = raw_post(address, &[host, json, revision], initialize_body);
    assert_eq!(status, 401, "a missing bearer must be refused");
    let (status, _) = raw_post(
        address,
        &[host, ("authorization", "Bearer deadbeef"), json, revision],
        initialize_body,
    );
    assert_eq!(status, 401, "a wrong bearer must be refused");

    // An unsupported mcp-protocol-version is refused before session logic.
    let (status, _) = raw_post(
        address,
        &[host, auth, json, ("mcp-protocol-version", "1999-01-01")],
        initialize_body,
    );
    assert_eq!(status, 400, "an unsupported revision must be refused");

    // The well-formed handshake is admitted and mints a session.
    let (status, headers) = raw_post(address, &[host, auth, json, revision], initialize_body);
    assert_eq!(status, 200, "the handshake must succeed");
    let session = headers["mcp-session-id"].as_str();

    // A session id is caller-bound: another admitted caller holding it is
    // still a 404, and an invented id is equally foreign.
    let other_bearer = bearer(other_token);
    let other_auth = ("authorization", other_bearer.as_str());
    let session_header = ("mcp-session-id", session);
    let (status, _) = raw_post(
        address,
        &[host, other_auth, json, revision, session_header],
        list_body,
    );
    assert_eq!(status, 404, "a foreign session id must be refused");
    let (status, _) = raw_post(
        address,
        &[
            host,
            auth,
            json,
            revision,
            ("mcp-session-id", "f".repeat(32).as_str()),
        ],
        list_body,
    );
    assert_eq!(status, 404, "an unknown session id must be refused");

    // A well-formed POST on the admitted session is served.
    let (status, _) = raw_post(
        address,
        &[host, auth, json, revision, session_header],
        list_body,
    );
    assert_eq!(status, 200, "a session-bound call must succeed");
    assert_eq!(fixture.lifecycle("stop")["state"], "stopped");
}
