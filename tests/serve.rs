//! Process contract proof over a synthetic framed-protocol backend.
//!
//! The subject is the real `mcpd` binary serving declaratively-registered
//! services; every backend session is the deterministic `mcpd-stub-backend`
//! fixture — an echo responder with no tools, no network and no agent. The
//! proofs cover the union catalog, per-registration caller admission, the
//! reserved control channel, connector reload behaviour and lifecycle
//! custody, never a real Agent conversation.

#![cfg(unix)]

use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

const MCPD: &str = env!("CARGO_BIN_EXE_mcpd");
const STUB: &str = env!("CARGO_BIN_EXE_mcpd-stub-backend");

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
protocol = "mcpd.stdio.v1"
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
        let root = std::env::temp_dir().join(format!("mcpd-contract-{}", uuid::Uuid::new_v4()));
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
        Command::new(MCPD)
            .args(args)
            .env("MCPD_HOME", &self.home)
            .env("MCPD_STUB_DIR", &self.stub_dir)
            .env("MCPD_STUB_MARKER", "env-alive")
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
        Command::new(MCPD)
            .args(["connector", "--caller", caller])
            .env("MCPD_HOME", &self.home)
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
        let _ = Command::new(MCPD)
            .args(["service", "stop"])
            .env("MCPD_HOME", &self.home)
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
    let mut service = Command::new(MCPD)
        .args(["serve", "--config", fixture.config.to_str().unwrap()])
        .env("MCPD_HOME", &fixture.home)
        .env("MCPD_STUB_DIR", &fixture.stub_dir)
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
        stderr.contains("mcpd_tool_name_collision"),
        "unexpected refusal: {stderr}"
    );
}

#[test]
fn an_unadmitted_caller_is_refused_before_any_frame() {
    let fixture = Fixture::new();
    assert_eq!(fixture.lifecycle("start")["state"], "running");
    let output = Command::new(MCPD)
        .args(["connector", "--caller", "outsider"])
        .env("MCPD_HOME", &fixture.home)
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
