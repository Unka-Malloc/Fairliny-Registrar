//! Native handler backend: a bounded pool of reusable child sessions.
//!
//! Each pooled session is one long-lived child process speaking a framed
//! protocol — one JSON request object per stdin line, one JSON reply object
//! per stdout line, bounded to `max_frame_bytes`:
//!
//! ```json
//! {"protocol":"<spec.protocol>","id":"<seq>","workflowId":"<spec.workflow_id>",
//!  "method":"execute","args":["<invoke argv, {request} substituted>"]}
//! ```
//!
//! The reply must echo the same `protocol`, `id` and `workflowId` and carry
//! `ok`; a successful call returns the reply's `result` verbatim.
//!
//! Environment fidelity is the hard rule: the child inherits this process's
//! real environment, and `spec.env` applies explicit overrides on top. A
//! backend is never spawned with an empty environment — a "clean" child loses
//! the user's configuration, credentials, providers and tools, which is a
//! defect, not hardening.
//!
//! Accepted work carries no internal deadline: once a request is written, the
//! call waits for its reply for as long as the child takes. EOF mid-call is a
//! transport failure, never an implicit cancel.
//!
//! Failures split at the write boundary: a request that never fully left the
//! pipe cannot have executed, so it stays retryable; once the request line
//! was written the child may have run it — a missing, malformed or refused
//! reply then leaves the outcome unknown, and an effect-bearing call is
//! reported `mcp_outcome_unknown` (`requestMayHaveExecuted`, never retryable)
//! instead of inviting a replay that could double a real-world effect.

use crate::McpApplicationError;
use crate::registry::{Channel, ToolHandler, ToolRequest};
use anyhow::{Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// The invoke-argv element replaced by the serialized call envelope.
pub const REQUEST_PLACEHOLDER: &str = "{request}";

const DEFAULT_WORKFLOW_ID: &str = "flr-service";
const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// The grace a dropped session gives its child to notice stdin EOF and exit
/// before teardown escalates to `kill`. Teardown is bounded so a wedged
/// child can never hold a dropping thread — or the worker permit behind it —
/// forever; the unbounded wait stays on the reply path, where it belongs.
const CHILD_EXIT_GRACE: Duration = Duration::from_secs(2);

/// How far a session exchange got before it failed — the distinction an
/// effect-bearing call needs for its retry verdict.
enum SessionFailure {
    /// The request never fully left the pipe: the child cannot have
    /// dispatched it, so the call is safe to retry.
    Unsent,
    /// The request line was written and flushed — whether the child ran it
    /// is unknown, so an effect-bearing call must not be replayed blindly.
    Sent,
}

/// How one native child speaks: the program and argv that start it, the frame
/// protocol name, the workflow label every frame carries, and the argv
/// template each tool call renders. `invoke` must hold at least one
/// `{request}` element; that element is substituted with the serialized
/// `{"name","arguments","caller"}` envelope.
#[derive(Clone, Debug)]
pub struct NativeSpec {
    pub program: String,
    pub args: Vec<String>,
    pub protocol: String,
    pub invoke: Vec<String>,
    pub workflow_id: String,
    /// Explicit environment overrides layered on the inherited environment —
    /// never a replacement for it.
    pub env: BTreeMap<String, String>,
    pub max_frame_bytes: usize,
}

impl NativeSpec {
    pub fn new(
        program: impl Into<String>,
        args: Vec<String>,
        protocol: impl Into<String>,
        invoke: Vec<String>,
    ) -> Result<Self> {
        let spec = Self {
            program: program.into(),
            args,
            protocol: protocol.into(),
            invoke,
            workflow_id: DEFAULT_WORKFLOW_ID.to_owned(),
            env: BTreeMap::new(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.program.is_empty()
                && !self.protocol.is_empty()
                && !self.workflow_id.is_empty()
                && !self.invoke.is_empty()
                && self.invoke.iter().any(|arg| arg == REQUEST_PLACEHOLDER)
                && self.max_frame_bytes > 0,
            "flr_native_spec_invalid"
        );
        Ok(())
    }
}

/// One reusable native session. The pool bounds concurrency at the admission
/// boundary (the HTTP worker permits), so sessions stay cheap to reuse while
/// independent calls progress.
struct ChildSession {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    sequence: u64,
}

impl ChildSession {
    fn start(spec: &NativeSpec) -> Result<Self> {
        // Environment fidelity: the child inherits the user's real
        // environment; spec.env applies only declared overrides.
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (name, value) in &spec.env {
            command.env(name, value);
        }
        let mut child = command
            .spawn()
            .map_err(|_| anyhow!("flr_backend_unavailable"))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("flr_backend_unavailable"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("flr_backend_unavailable"))?;
        Ok(Self {
            child,
            input: Some(input),
            output: BufReader::new(output),
            sequence: 0,
        })
    }

    fn execute(
        &mut self,
        spec: &NativeSpec,
        args: Vec<String>,
    ) -> std::result::Result<Value, SessionFailure> {
        self.sequence += 1;
        let id = self.sequence.to_string();
        let input = self.input.as_mut().ok_or(SessionFailure::Unsent)?;
        serde_json::to_writer(
            &mut *input,
            &json!({
                "protocol": spec.protocol,
                "id": id,
                "workflowId": spec.workflow_id,
                "method": "execute",
                "args": args,
            }),
        )
        .map_err(|_| SessionFailure::Unsent)?;
        input.write_all(b"\n").map_err(|_| SessionFailure::Unsent)?;
        // The newline completes the request line inside the pipe: from here
        // the child may have read and run the call, so every later failure —
        // EOF, a desynced frame, a refused `ok` — leaves its outcome unknown.
        input.flush().map_err(|_| SessionFailure::Sent)?;
        // No task deadline. EOF is a transport failure, never an implicit
        // cancel — a viable call runs to completion.
        let mut bytes = Vec::new();
        let count = (&mut self.output)
            .take(spec.max_frame_bytes as u64 + 1)
            .read_until(b'\n', &mut bytes)
            .map_err(|_| SessionFailure::Sent)?;
        if count == 0 || bytes.last() != Some(&b'\n') || bytes.len() > spec.max_frame_bytes {
            return Err(SessionFailure::Sent);
        }
        let frame: Value = serde_json::from_slice(&bytes).map_err(|_| SessionFailure::Sent)?;
        if frame["protocol"] != spec.protocol
            || frame["id"] != id
            || frame["workflowId"] != spec.workflow_id
        {
            return Err(SessionFailure::Sent);
        }
        if frame["ok"] != true {
            return Err(SessionFailure::Sent);
        }
        frame.get("result").cloned().ok_or(SessionFailure::Sent)
    }
}

impl Drop for ChildSession {
    fn drop(&mut self) {
        drop(self.input.take());
        // Closing this session never kills peer sessions or the work the
        // child already finished; reap it so it cannot zombie — but bound
        // the teardown: a child that ignores stdin EOF gets a short grace
        // to exit, then is killed and reaped, so a wedged child can never
        // block the dropping thread (or the permit behind it) forever.
        let deadline = Instant::now() + CHILD_EXIT_GRACE;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                // The grace expired or the child can no longer be observed:
                // kill it and collect — reaping after a kill is bounded, so
                // teardown never waits on a wedged child.
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break;
                }
            }
        }
    }
}

/// A pooled framed child backend. Ordinary calls take `sessions`; a
/// registration that reserved a control channel routes control-class tools to
/// `control_sessions`, so a cancellation never queues behind a held ordinary
/// call.
pub struct NativeBackend {
    spec: NativeSpec,
    sessions: Mutex<Vec<ChildSession>>,
    control_sessions: Mutex<Vec<ChildSession>>,
}

impl NativeBackend {
    pub fn new(spec: NativeSpec) -> Result<Self> {
        spec.validate()?;
        Ok(Self {
            spec,
            sessions: Mutex::new(Vec::new()),
            control_sessions: Mutex::new(Vec::new()),
        })
    }
}

impl ToolHandler for NativeBackend {
    fn call(&self, request: &ToolRequest<'_>) -> std::result::Result<Value, McpApplicationError> {
        let envelope = call_envelope(request);
        let args = render_invoke(&self.spec.invoke, &envelope);
        let pool = match request.channel {
            Channel::General => &self.sessions,
            Channel::Control => &self.control_sessions,
        };
        let mut session = pool
            .lock()
            .map_err(|_| McpApplicationError::retryable("native_backend_unavailable", "backend"))?
            .pop()
            .map(Ok)
            .unwrap_or_else(|| ChildSession::start(&self.spec))
            .map_err(|_| McpApplicationError::retryable("native_backend_unavailable", "backend"))?;
        let response = session.execute(&self.spec, args);
        // A session that answered cleanly returns to its pool; a failed one is
        // dropped — never reuse a stream whose framing may have desynchronized.
        if response.is_ok() {
            pool.lock()
                .map_err(|_| {
                    McpApplicationError::retryable("native_backend_unavailable", "backend")
                })?
                .push(session);
        }
        // The retry verdict splits at the write boundary: a request the
        // child could never have seen stays retryable (`native_backend_unavailable`),
        // while anything after the request line left leaves the outcome
        // unknown — an effect-bearing call is then reported
        // `mcp_outcome_unknown` and never replayed.
        response.map_err(|failure| match failure {
            SessionFailure::Sent if request.effect => {
                McpApplicationError::outcome_unknown("backend")
            }
            _ => McpApplicationError::retryable("native_backend_unavailable", "backend"),
        })
    }
}

/// The call envelope handed to the child:
/// `{"name": tool, "arguments": {...}, "caller": {"providerId": ..., ...attrs}}`.
///
/// Declared context attributes flatten into the caller scope, keeping the
/// distinction the lifted contract draws: an attribute the caller never sent
/// is absent, while an empty one cannot arrive at all — the header admission
/// already rejected it.
fn call_envelope(request: &ToolRequest<'_>) -> Value {
    let mut scope = Map::new();
    scope.insert("providerId".to_owned(), json!(request.caller.provider_id));
    for (attribute, value) in &request.caller.attributes {
        scope.insert(attribute.clone(), json!(value));
    }
    json!({"name": request.tool, "arguments": request.arguments, "caller": scope})
}

fn render_invoke(invoke: &[String], envelope: &Value) -> Vec<String> {
    let rendered = envelope.to_string();
    invoke
        .iter()
        .map(|arg| {
            if arg == REQUEST_PLACEHOLDER {
                rendered.clone()
            } else {
                arg.clone()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::CallerContext;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    fn request<'a>(
        caller: &'a CallerContext,
        arguments: &'a Map<String, Value>,
        channel: Channel,
    ) -> ToolRequest<'a> {
        ToolRequest {
            tool: "fixture_tool",
            arguments,
            caller,
            channel,
            effect: false,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn caller(provider: &str, attributes: &[(&str, &str)]) -> CallerContext {
        CallerContext {
            provider_id: provider.to_owned(),
            attributes: attributes
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            authenticated: true,
        }
    }

    #[test]
    fn the_envelope_flattens_declared_scope_and_omits_absent_attributes() {
        let arguments = Map::from_iter([("prompt".to_owned(), json!("ordinary text"))]);
        let unscoped = caller("codex", &[]);
        let envelope = call_envelope(&request(&unscoped, &arguments, Channel::General));
        assert_eq!(
            envelope,
            json!({
                "name": "fixture_tool",
                "arguments": {"prompt": "ordinary text"},
                "caller": {"providerId": "codex"},
            })
        );

        let scoped = caller(
            "cursor",
            &[
                ("conversationId", "conversation:one"),
                ("membershipId", "membership:worker"),
                ("parentDispatchId", "dispatch:parent"),
            ],
        );
        let envelope = call_envelope(&request(&scoped, &arguments, Channel::General));
        assert_eq!(
            envelope["caller"],
            json!({
                "providerId": "cursor",
                "conversationId": "conversation:one",
                "membershipId": "membership:worker",
                "parentDispatchId": "dispatch:parent",
            })
        );
        // The authenticated flag never crosses the backend boundary.
        assert!(envelope["caller"].get("authenticated").is_none());
    }

    #[test]
    fn invoke_substitution_replaces_only_the_request_placeholder() {
        let envelope = json!({"name":"t","arguments":{},"caller":{"providerId":"codex"}});
        let invoke = vec![
            "subagents".to_owned(),
            "execute".to_owned(),
            "--stdin-json".to_owned(),
            "{request}".to_owned(),
        ];
        let rendered = render_invoke(&invoke, &envelope);
        assert_eq!(
            rendered,
            vec![
                "subagents".to_owned(),
                "execute".to_owned(),
                "--stdin-json".to_owned(),
                envelope.to_string(),
            ]
        );
    }

    #[test]
    fn a_native_spec_requires_the_request_placeholder() {
        let spec = NativeSpec::new(
            "prog",
            vec!["arg".into()],
            "flr.stdio.v1",
            vec!["no-placeholder".into()],
        );
        assert!(spec.is_err());
        let spec = NativeSpec::new("prog", vec![], "flr.stdio.v1", vec!["{request}".into()]);
        assert!(spec.is_ok());
        let spec = NativeSpec::new("", vec![], "flr.stdio.v1", vec!["{request}".into()]);
        assert!(spec.is_err());
    }
}
