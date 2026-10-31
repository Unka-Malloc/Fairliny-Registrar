use anyhow::{Result, anyhow, ensure};
use serde_json::{Map, Number, Value};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum McpRequestId {
    Text(String),
    Number(Number),
}

impl McpRequestId {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::String(value) => Ok(Self::Text(value.clone())),
            Value::Number(value) => Ok(Self::Number(value.clone())),
            _ => Err(anyhow!("mcp_request_id_invalid")),
        }
    }

    fn to_value(&self) -> Value {
        match self {
            Self::Text(value) => Value::String(value.clone()),
            Self::Number(value) => Value::Number(value.clone()),
        }
    }
}

impl From<&str> for McpRequestId {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for McpRequestId {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for McpRequestId {
    fn from(value: i64) -> Self {
        Self::Number(Number::from(value))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct McpError {
    pub code: Number,
    pub message: String,
    pub data: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum McpResponse {
    Success(Map<String, Value>),
    Error(McpError),
}

#[derive(Clone, Debug, PartialEq)]
pub enum McpMessage {
    Request {
        id: McpRequestId,
        method: String,
        params: Option<Map<String, Value>>,
    },
    Notification {
        method: String,
        params: Option<Map<String, Value>>,
    },
    Response {
        id: Option<McpRequestId>,
        response: McpResponse,
    },
}

impl McpMessage {
    pub fn request(
        id: impl Into<McpRequestId>,
        method: impl Into<String>,
        params: Option<Map<String, Value>>,
    ) -> Result<Self> {
        let method = normalized_method(method.into())?;
        Ok(Self::Request {
            id: id.into(),
            method,
            params,
        })
    }

    pub fn notification(
        method: impl Into<String>,
        params: Option<Map<String, Value>>,
    ) -> Result<Self> {
        Ok(Self::Notification {
            method: normalized_method(method.into())?,
            params,
        })
    }

    pub fn success(id: impl Into<McpRequestId>, result: Map<String, Value>) -> Self {
        Self::Response {
            id: Some(id.into()),
            response: McpResponse::Success(result),
        }
    }

    pub fn error(
        id: Option<McpRequestId>,
        code: i64,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Result<Self> {
        let message = message.into();
        ensure!(!message.trim().is_empty(), "mcp_error_message_empty");
        Ok(Self::Response {
            id,
            response: McpResponse::Error(McpError {
                code: Number::from(code),
                message,
                data,
            }),
        })
    }

    pub fn request_id(&self) -> Option<&McpRequestId> {
        match self {
            Self::Request { id, .. } => Some(id),
            Self::Notification { .. } => None,
            Self::Response { id, .. } => id.as_ref(),
        }
    }

    pub fn from_value(value: Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| {
            if value.is_array() {
                anyhow!("mcp_batch_unsupported")
            } else {
                anyhow!("mcp_message_must_be_object")
            }
        })?;
        ensure!(
            object.get("jsonrpc").and_then(Value::as_str) == Some("2.0"),
            "mcp_jsonrpc_version_invalid"
        );

        if object.contains_key("method") {
            ensure_allowed_fields(object, &["jsonrpc", "id", "method", "params"])?;
            let method = object
                .get("method")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("mcp_method_invalid"))?;
            let method = normalized_method(method.to_owned())?;
            let params = optional_object(object.get("params"), "mcp_params_invalid")?;
            return match object.get("id") {
                Some(id) => Ok(Self::Request {
                    id: McpRequestId::from_value(id)?,
                    method,
                    params,
                }),
                None => Ok(Self::Notification { method, params }),
            };
        }

        ensure_allowed_fields(object, &["jsonrpc", "id", "result", "error"])?;
        let has_result = object.contains_key("result");
        let has_error = object.contains_key("error");
        ensure!(has_result ^ has_error, "mcp_response_outcome_invalid");
        let id = object.get("id").map(McpRequestId::from_value).transpose()?;

        if let Some(result) = object.get("result") {
            ensure!(id.is_some(), "mcp_success_response_id_missing");
            let result = result
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!("mcp_result_invalid"))?;
            return Ok(Self::Response {
                id,
                response: McpResponse::Success(result),
            });
        }

        let error = object
            .get("error")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("mcp_error_invalid"))?;
        ensure_allowed_fields(error, &["code", "message", "data"])?;
        let code = error
            .get("code")
            .and_then(Value::as_number)
            .cloned()
            .ok_or_else(|| anyhow!("mcp_error_code_invalid"))?;
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .ok_or_else(|| anyhow!("mcp_error_message_invalid"))?
            .to_owned();
        Ok(Self::Response {
            id,
            response: McpResponse::Error(McpError {
                code,
                message,
                data: error.get("data").cloned(),
            }),
        })
    }

    pub fn to_value(&self) -> Value {
        let mut object = Map::new();
        object.insert("jsonrpc".into(), Value::String("2.0".into()));
        match self {
            Self::Request { id, method, params } => {
                object.insert("id".into(), id.to_value());
                object.insert("method".into(), Value::String(method.clone()));
                if let Some(params) = params {
                    object.insert("params".into(), Value::Object(params.clone()));
                }
            }
            Self::Notification { method, params } => {
                object.insert("method".into(), Value::String(method.clone()));
                if let Some(params) = params {
                    object.insert("params".into(), Value::Object(params.clone()));
                }
            }
            Self::Response { id, response } => {
                if let Some(id) = id {
                    object.insert("id".into(), id.to_value());
                }
                match response {
                    McpResponse::Success(result) => {
                        object.insert("result".into(), Value::Object(result.clone()));
                    }
                    McpResponse::Error(error) => {
                        let mut value = Map::new();
                        value.insert("code".into(), Value::Number(error.code.clone()));
                        value.insert("message".into(), Value::String(error.message.clone()));
                        if let Some(data) = &error.data {
                            value.insert("data".into(), data.clone());
                        }
                        object.insert("error".into(), Value::Object(value));
                    }
                }
            }
        }
        Value::Object(object)
    }
}

pub fn decode_http_body(body: &[u8], max_bytes: usize) -> Result<McpMessage> {
    decode_json(body, max_bytes)
}

pub fn encode_http_body(message: &McpMessage, max_bytes: usize) -> Result<Vec<u8>> {
    ensure!(max_bytes > 0, "mcp_message_limit_invalid");
    let encoded =
        serde_json::to_vec(&message.to_value()).map_err(|_| anyhow!("mcp_encode_failed"))?;
    ensure!(encoded.len() <= max_bytes, "mcp_message_too_large");
    Ok(encoded)
}

pub fn decode_stdio_line(line: &[u8], max_bytes: usize) -> Result<McpMessage> {
    ensure!(
        line.len() <= max_bytes.saturating_add(2),
        "mcp_message_too_large"
    );
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    ensure!(
        !line.iter().any(|byte| matches!(byte, b'\n' | b'\r')),
        "mcp_stdio_embedded_newline"
    );
    decode_json(line, max_bytes)
}

pub fn encode_stdio_line(message: &McpMessage, max_bytes: usize) -> Result<Vec<u8>> {
    let mut encoded = encode_http_body(message, max_bytes)?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn decode_json(bytes: &[u8], max_bytes: usize) -> Result<McpMessage> {
    ensure!(max_bytes > 0, "mcp_message_limit_invalid");
    ensure!(bytes.len() <= max_bytes, "mcp_message_too_large");
    let value = serde_json::from_slice(bytes).map_err(|_| anyhow!("mcp_invalid_json"))?;
    McpMessage::from_value(value)
}

fn normalized_method(method: String) -> Result<String> {
    let normalized = method.trim();
    ensure!(!normalized.is_empty(), "mcp_method_empty");
    ensure!(normalized == method, "mcp_method_whitespace_invalid");
    Ok(method)
}

fn optional_object(
    value: Option<&Value>,
    code: &'static str,
) -> Result<Option<Map<String, Value>>> {
    value
        .map(|value| value.as_object().cloned().ok_or_else(|| anyhow!(code)))
        .transpose()
}

fn ensure_allowed_fields(object: &Map<String, Value>, allowed: &[&str]) -> Result<()> {
    ensure!(
        object.keys().all(|key| allowed.contains(&key.as_str())),
        "mcp_envelope_field_invalid"
    );
    Ok(())
}
