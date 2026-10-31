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

use crate::McpApplicationError;
use crate::registry::{Channel, ToolHandler, ToolRequest};
use anyhow::{Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Mutex;

/// The invoke-argv element replaced by the serialized call envelope.
pub const REQUEST_PLACEHOLDER: &str = "{request}";

const DEFAULT_WORKFLOW_ID: &str = "mcpd-service";
const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

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
            "mcpd_native_spec_invalid"
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
            .map_err(|_| anyhow!("mcpd_backend_unavailable"))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("mcpd_backend_unavailable"))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("mcpd_backend_unavailable"))?;
        Ok(Self {
            child,
            input: Some(input),
            output: BufReader::new(output),
            sequence: 0,
        })
    }

    fn execute(&mut self, spec: &NativeSpec, args: Vec<String>) -> Result<Value> {
        self.sequence += 1;
        let id = self.sequence.to_string();
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| anyhow!("mcpd_backend_unavailable"))?;
        serde_json::to_writer(
            &mut *input,
            &json!({
                "protocol": spec.protocol,
                "id": id,
                "workflowId": spec.workflow_id,
                "method": "execute",
                "args": args,
            }),
        )?;
        input.write_all(b"\n")?;
        input.flush()?;
        // No task deadline. EOF is a transport failure, never an implicit
        // cancel — a viable call runs to completion.
        let mut bytes = Vec::new();
        let count = (&mut self.output)
            .take(spec.max_frame_bytes as u64 + 1)
            .read_until(b'\n', &mut bytes)?;
        if count == 0 || bytes.last() != Some(&b'\n') || bytes.len() > spec.max_frame_bytes {
            return Err(anyhow!("mcpd_backend_response_invalid"));
        }
        let frame: Value = serde_json::from_slice(&bytes)?;
        if frame["protocol"] != spec.protocol
            || frame["id"] != id
            || frame["workflowId"] != spec.workflow_id
        {
            return Err(anyhow!("mcpd_backend_response_invalid"));
        }
        if frame["ok"] != true {
            return Err(anyhow!("mcpd_backend_request_failed"));
        }
        frame
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("mcpd_backend_response_invalid"))
    }
}

impl Drop for ChildSession {
    fn drop(&mut self) {
        drop(self.input.take());
        // Closing this session never kills peer sessions or the work the
        // child already finished; reap it so it cannot zombie.
        let _ = self.child.wait();
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
        // Backend failures surface as retryable: the child may be restarted
        // and the call retried without changing its meaning.
        response
            .map_err(|_| McpApplicationError::retryable("native_backend_unavailable", "backend"))
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
            "mcpd.stdio.v1",
            vec!["no-placeholder".into()],
        );
        assert!(spec.is_err());
        let spec = NativeSpec::new("prog", vec![], "mcpd.stdio.v1", vec!["{request}".into()]);
        assert!(spec.is_ok());
        let spec = NativeSpec::new("", vec![], "mcpd.stdio.v1", vec!["{request}".into()]);
        assert!(spec.is_err());
    }
}
