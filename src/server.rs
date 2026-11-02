use super::{McpMessage, McpRequestId, decode_stdio_line, encode_stdio_line};
use anyhow::{Result, anyhow};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct McpServerDefinition {
    pub protocol_revision: &'static str,
    pub compatible_protocol_revisions: &'static [&'static str],
    pub server_name: &'static str,
    pub server_version: &'static str,
    pub max_message_bytes: usize,
}

impl McpServerDefinition {
    pub fn supports_protocol_revision(&self, revision: &str) -> bool {
        revision == self.protocol_revision || self.compatible_protocol_revisions.contains(&revision)
    }

    fn negotiated_protocol_revision(&self, requested: &str) -> Option<&'static str> {
        if requested == self.protocol_revision {
            return Some(self.protocol_revision);
        }
        self.compatible_protocol_revisions
            .iter()
            .copied()
            .find(|revision| *revision == requested)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpApplicationError {
    pub code: &'static str,
    pub stage: &'static str,
    pub retryable: bool,
    pub recovery: &'static str,
    /// Whether the failed request may already have executed at the backend —
    /// the verdict a caller needs before it may safely replay an effect call.
    pub request_may_have_executed: bool,
}

impl McpApplicationError {
    pub const fn permanent(code: &'static str, stage: &'static str) -> Self {
        Self {
            code,
            stage,
            retryable: false,
            recovery: "correct_request_and_retry",
            request_may_have_executed: false,
        }
    }

    pub const fn retryable(code: &'static str, stage: &'static str) -> Self {
        Self {
            code,
            stage,
            retryable: true,
            recovery: "retry_after_recovery",
            request_may_have_executed: false,
        }
    }

    /// The request left the boundary and its answer was lost: whether the
    /// call executed is unknown, so an effect-bearing call must never be
    /// replayed blindly — the caller reconciles before deciding.
    pub const fn outcome_unknown(stage: &'static str) -> Self {
        Self {
            code: "mcp_outcome_unknown",
            stage,
            retryable: false,
            recovery: "reconcile_before_retry",
            request_may_have_executed: true,
        }
    }
}

pub struct McpToolCallContext<'a, C> {
    pub caller: &'a C,
    pub cancelled: Arc<AtomicBool>,
}

pub trait McpApplication: Send + Sync {
    type CallerContext: Send + Sync;

    fn tool_catalog(&self) -> Vec<Value>;
    /// Whether the union catalog routes `name` — the membership check
    /// `tools/call` needs before validation. The default walks the served
    /// catalog, so an application needs no extra surface to be correct; an
    /// application holding a routing table (the `Dispatcher`) overrides it
    /// so the hot path never clones the catalog.
    fn has_tool(&self, name: &str) -> bool {
        self.tool_catalog()
            .iter()
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
    }
    fn validate_tool_arguments(&self, name: &str, arguments: &Map<String, Value>) -> bool;
    fn call_tool(
        &self,
        context: McpToolCallContext<'_, Self::CallerContext>,
        name: &str,
        arguments: &Map<String, Value>,
    ) -> std::result::Result<Value, McpApplicationError>;
}

#[derive(Default)]
pub struct McpSessionState {
    initialized: AtomicBool,
    protocol_revision: Mutex<Option<&'static str>>,
    cancellations: Mutex<HashMap<McpRequestId, Arc<AtomicBool>>>,
}

impl McpSessionState {
    pub fn initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub fn protocol_revision(&self) -> Option<&'static str> {
        *self
            .protocol_revision
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn initialize(&self, protocol_revision: &'static str) {
        *self
            .protocol_revision
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(protocol_revision);
        self.initialized.store(true, Ordering::Release);
    }

    fn cancellation(&self, id: &McpRequestId) -> Arc<AtomicBool> {
        let flag = Arc::new(AtomicBool::new(false));
        self.cancellations
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(id.clone(), Arc::clone(&flag));
        flag
    }

    fn finish(&self, id: &McpRequestId) {
        self.cancellations
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(id);
    }

    fn cancel(&self, id: &McpRequestId) {
        if let Some(flag) = self
            .cancellations
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(id)
        {
            flag.store(true, Ordering::Release);
        }
    }
}

pub struct McpServerEngine<A> {
    definition: McpServerDefinition,
    application: A,
}

impl<A> McpServerEngine<A>
where
    A: McpApplication,
{
    pub fn new(definition: McpServerDefinition, application: A) -> Result<Self> {
        if definition.protocol_revision.len() != 10
            || definition
                .compatible_protocol_revisions
                .iter()
                .enumerate()
                .any(|(index, revision)| {
                    revision.len() != 10
                        || *revision == definition.protocol_revision
                        || definition.compatible_protocol_revisions[index + 1..].contains(revision)
                })
            || definition.server_name.is_empty()
            || definition.server_version.is_empty()
            || definition.max_message_bytes == 0
        {
            return Err(anyhow!("mcp_server_definition_invalid"));
        }
        Ok(Self {
            definition,
            application,
        })
    }

    pub fn definition(&self) -> &McpServerDefinition {
        &self.definition
    }

    pub fn handle(
        &self,
        session: &McpSessionState,
        caller: &A::CallerContext,
        message: McpMessage,
    ) -> Option<McpMessage> {
        match message {
            McpMessage::Notification { method, params } => {
                if method == "notifications/cancelled"
                    && let Some(id) = params
                        .as_ref()
                        .and_then(|params| params.get("requestId"))
                        .and_then(request_id_from_value)
                {
                    session.cancel(&id);
                }
                None
            }
            McpMessage::Request { id, method, params } => {
                Some(self.handle_request(session, caller, id, &method, params))
            }
            McpMessage::Response { id, .. } => Some(rpc_error(
                id,
                -32600,
                "Invalid Request",
                Some(json!({"reasonCode": "mcp_response_not_accepted"})),
            )),
        }
    }

    fn handle_request(
        &self,
        session: &McpSessionState,
        caller: &A::CallerContext,
        id: McpRequestId,
        method: &str,
        params: Option<Map<String, Value>>,
    ) -> McpMessage {
        match method {
            "initialize" => self.initialize(session, id, params),
            "ping" if session.initialized() && params.as_ref().is_none_or(metadata_only_params) => {
                McpMessage::success(id, Map::new())
            }
            "tools/list" if !session.initialized() => {
                rpc_error(Some(id), -32002, "Server not initialized", None)
            }
            "tools/list" if params.as_ref().is_none_or(list_tools_params) => {
                let mut result = Map::new();
                result.insert(
                    "tools".to_owned(),
                    Value::Array(self.application.tool_catalog()),
                );
                McpMessage::success(id, result)
            }
            "tools/list" => rpc_error(Some(id), -32602, "Invalid params", None),
            "tools/call" if !session.initialized() => {
                rpc_error(Some(id), -32002, "Server not initialized", None)
            }
            "tools/call" => self.call_tool(session, caller, id, params),
            "ping" => rpc_error(Some(id), -32602, "Invalid params", None),
            _ => rpc_error(Some(id), -32601, "Method not found", None),
        }
    }

    fn initialize(
        &self,
        session: &McpSessionState,
        id: McpRequestId,
        params: Option<Map<String, Value>>,
    ) -> McpMessage {
        let negotiated_revision = params
            .as_ref()
            .and_then(|params| params.get("protocolVersion"))
            .and_then(Value::as_str)
            .and_then(|revision| self.definition.negotiated_protocol_revision(revision));
        let valid = params.as_ref().is_some_and(|params| {
            negotiated_revision.is_some()
                && params.get("capabilities").is_some_and(Value::is_object)
                && params.get("clientInfo").is_some_and(Value::is_object)
                && params.keys().all(|key| {
                    matches!(
                        key.as_str(),
                        "protocolVersion" | "capabilities" | "clientInfo" | "_meta"
                    )
                })
                && valid_metadata(params)
        });
        if !valid {
            return rpc_error(Some(id), -32602, "Invalid params", None);
        }
        let negotiated_revision = negotiated_revision.expect("validated protocol revision");
        session.initialize(negotiated_revision);
        let result = json!({
            "protocolVersion": negotiated_revision,
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": {
                "name": self.definition.server_name,
                "version": self.definition.server_version,
            }
        });
        McpMessage::success(id, result.as_object().cloned().unwrap_or_default())
    }

    fn call_tool(
        &self,
        session: &McpSessionState,
        caller: &A::CallerContext,
        id: McpRequestId,
        params: Option<Map<String, Value>>,
    ) -> McpMessage {
        let Some(params) = params else {
            return rpc_error(Some(id), -32602, "Invalid params", None);
        };
        if !(2..=3).contains(&params.len())
            || !params
                .keys()
                .all(|key| matches!(key.as_str(), "name" | "arguments" | "_meta"))
            || !valid_metadata(&params)
        {
            return rpc_error(Some(id), -32602, "Invalid params", None);
        }
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return rpc_error(Some(id), -32602, "Invalid params", None);
        };
        let Some(arguments) = params.get("arguments").and_then(Value::as_object) else {
            return rpc_error(Some(id), -32602, "Invalid params", None);
        };
        if !self.application.has_tool(name) {
            return rpc_error(Some(id), -32601, "Method not found", None);
        }
        if !self.application.validate_tool_arguments(name, arguments) {
            return rpc_error(Some(id), -32602, "Invalid params", None);
        }

        let cancelled = session.cancellation(&id);
        let result = self.application.call_tool(
            McpToolCallContext {
                caller,
                cancelled: Arc::clone(&cancelled),
            },
            name,
            arguments,
        );
        session.finish(&id);
        if cancelled.load(Ordering::Acquire) {
            return rpc_error(Some(id), -32800, "Request cancelled", None);
        }
        let payload = match result {
            Ok(value) => {
                let is_error = value.get("isError").and_then(Value::as_bool) == Some(true);
                tool_result(value, is_error)
            }
            Err(error) => tool_result(
                json!({
                    "schemaVersion": "flr.error.v1",
                    "reasonCode": error.code,
                    "stage": error.stage,
                    "retryable": error.retryable,
                    "recovery": error.recovery,
                    "requestMayHaveExecuted": error.request_may_have_executed,
                }),
                true,
            ),
        };
        McpMessage::success(id, payload.as_object().cloned().unwrap_or_default())
    }
}

fn valid_metadata(params: &Map<String, Value>) -> bool {
    params.get("_meta").is_none_or(Value::is_object)
}

fn metadata_only_params(params: &Map<String, Value>) -> bool {
    params.keys().all(|key| key == "_meta") && valid_metadata(params)
}

fn list_tools_params(params: &Map<String, Value>) -> bool {
    params
        .keys()
        .all(|key| matches!(key.as_str(), "cursor" | "_meta"))
        && params
            .get("cursor")
            .is_none_or(|cursor| cursor.is_null() || cursor.is_string())
        && valid_metadata(params)
}

fn tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_owned());
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
        "isError": is_error,
    })
}

fn request_id_from_value(value: &Value) -> Option<McpRequestId> {
    match value {
        Value::String(value) => Some(McpRequestId::from(value.clone())),
        Value::Number(value) => value.as_i64().map(McpRequestId::from),
        _ => None,
    }
}

fn rpc_error(
    id: Option<McpRequestId>,
    code: i64,
    message: &'static str,
    data: Option<Value>,
) -> McpMessage {
    McpMessage::error(id, code, message, data).expect("static MCP error is valid")
}

pub enum McpStdioFrame {
    Eof,
    Message(McpMessage),
    Invalid(McpMessage),
}

pub fn read_stdio_frame(reader: &mut impl BufRead, max_bytes: usize) -> McpStdioFrame {
    let mut bytes = Vec::with_capacity(1024);
    let mut oversized = false;
    loop {
        let available = match reader.fill_buf() {
            Ok(value) => value,
            Err(_) => return McpStdioFrame::Eof,
        };
        if available.is_empty() {
            if bytes.is_empty() && !oversized {
                return McpStdioFrame::Eof;
            }
            break;
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        let slice = &available[..consumed];
        if !oversized {
            let remaining = max_bytes.saturating_add(1).saturating_sub(bytes.len());
            bytes.extend_from_slice(&slice[..slice.len().min(remaining)]);
            oversized = bytes.len() > max_bytes || slice.len() > remaining;
        }
        let newline = slice.last() == Some(&b'\n');
        reader.consume(consumed);
        if newline {
            break;
        }
    }
    if oversized {
        return McpStdioFrame::Invalid(rpc_error(None, -32600, "Invalid Request", None));
    }
    match decode_stdio_line(&bytes, max_bytes) {
        Ok(message) => McpStdioFrame::Message(message),
        Err(_) => McpStdioFrame::Invalid(rpc_error(None, -32700, "Parse error", None)),
    }
}

pub fn serve_stdio<A, R, W>(
    engine: &McpServerEngine<A>,
    caller: &A::CallerContext,
    mut reader: R,
    mut writer: W,
) -> Result<W>
where
    A: McpApplication,
    R: BufRead,
    W: Write,
{
    let session = McpSessionState::default();
    loop {
        let response = match read_stdio_frame(&mut reader, engine.definition.max_message_bytes) {
            McpStdioFrame::Eof => break,
            McpStdioFrame::Invalid(response) => Some(response),
            McpStdioFrame::Message(message) => engine.handle(&session, caller, message),
        };
        if let Some(response) = response {
            let encoded = encode_stdio_line(&response, engine.definition.max_message_bytes)?;
            writer.write_all(&encoded)?;
            writer.flush()?;
        }
    }
    Ok(writer)
}
