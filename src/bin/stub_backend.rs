//! `mcpd-stub-backend` — a deterministic framed-protocol fixture.
//!
//! A test/support binary, not a service: it speaks the same line-framed
//! protocol `NativeBackend` drives — one JSON request per stdin line, one
//! `{protocol, id, workflowId, ok, result}` reply per stdout line — and
//! echoes the request back inside `result`. It owns no tools, touches no
//! network and performs no work of its own.
//!
//! Fixture vocabulary for integration proofs (all synthetic):
//! * request `"name": "fixture_hold"` — when `$MCPD_STUB_DIR/hold` exists,
//!   writes `hold-started` and answers only once `released` appears, so a
//!   proof can hold an ordinary call open;
//! * request `"name": "fixture_cancel"` — writes `released`, ending the
//!   hold, so a proof can show the control channel bypasses held work;
//! * request `"name": "fixture_drop"` — the child exits immediately without
//!   replying, so the parent sees EOF mid-call (a post-write transport
//!   failure whose outcome is unknown);
//! * every reply echoes `marker` = `$MCPD_STUB_MARKER`, proving the child
//!   inherited the real environment rather than an empty one.

use serde_json::{Value, json};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

fn request_envelope(args: &[Value]) -> Option<Value> {
    args.iter().rev().find_map(|arg| {
        arg.as_str()
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .filter(|value| value.get("name").and_then(Value::as_str).is_some())
    })
}

fn work_dir() -> Option<PathBuf> {
    env::var_os("MCPD_STUB_DIR").map(PathBuf::from)
}

fn wait_for_release() {
    let Some(root) = work_dir() else {
        return;
    };
    if !root.join("hold").exists() {
        return;
    }
    let _ = fs::write(root.join("hold-started"), "yes");
    // The fixture that armed the hold owns releasing it; no internal
    // deadline — a held call ends when its control call lands.
    while !root.join("released").exists() {
        thread::sleep(Duration::from_millis(10));
    }
}

fn execute(request: &Value) -> Value {
    let args = request["args"].as_array().cloned().unwrap_or_default();
    let envelope = request_envelope(&args);
    if let Some(name) = envelope
        .as_ref()
        .and_then(|envelope| envelope.get("name"))
        .and_then(Value::as_str)
    {
        match name {
            "fixture_hold" => wait_for_release(),
            "fixture_cancel" => {
                if let Some(root) = work_dir() {
                    let _ = fs::write(root.join("released"), "yes");
                }
            }
            // The request was read and the child dies before answering:
            // exactly the post-write failure an effect call must never
            // replay after.
            "fixture_drop" => std::process::exit(0),
            _ => {}
        }
    }
    json!({
        "args": args,
        "request": envelope.unwrap_or(Value::Null),
        "marker": env::var("MCPD_STUB_MARKER").unwrap_or_default(),
    })
}

fn main() -> ExitCode {
    let stdin = std::io::stdin();
    let mut output = std::io::stdout();
    for line in BufReader::new(stdin.lock()).lines() {
        let Ok(line) = line else {
            return ExitCode::FAILURE;
        };
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Value>(&line) {
            Ok(request) => json!({
                "protocol": request["protocol"],
                "id": request["id"],
                "workflowId": request["workflowId"],
                "ok": true,
                "result": execute(&request),
            }),
            Err(_) => json!({
                "protocol": "mcpd.stdio.v1",
                "id": Value::Null,
                "workflowId": Value::Null,
                "ok": false,
            }),
        };
        if writeln!(output, "{reply}")
            .and_then(|()| output.flush())
            .is_err()
        {
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}
