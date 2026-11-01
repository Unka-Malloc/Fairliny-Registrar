//! Declarative registrations: `mcpd serve --config services.toml`.
//!
//! The file describes the same things a host embedding the crate builds in
//! Rust: per-`[[service]]` name, optional version, caller allowlist,
//! control-channel tool names, a native backend spec and an ordered tool
//! catalog of closed input schemas. Service-wide `[[context]]` tables declare
//! the caller-context channel (environment variable → loopback header →
//! caller-scope attribute).

use crate::application::{
    COMPATIBLE_PROTOCOL_REVISIONS, Dispatcher, MAX_MCP_FRAME_BYTES, PROTOCOL_REVISION,
};
use crate::backend::{NativeBackend, NativeSpec, REQUEST_PLACEHOLDER};
use crate::registry::{ContextField, Registration, ToolSpec};
use crate::{McpServerDefinition, Registry};
use anyhow::{Result, anyhow, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerTable {
    name: Option<String>,
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackendTable {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    protocol: String,
    workflow_id: Option<String>,
    /// The argv template each tool call renders; must hold at least one
    /// `{request}` element substituted with the serialized call envelope.
    invoke: Vec<String>,
    /// Explicit environment overrides layered on the inherited environment.
    #[serde(default)]
    env: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolTable {
    name: String,
    description: Option<String>,
    /// Effect-bearing: a transport failure mid-call leaves the outcome
    /// uncertain, so connectors must never replay it.
    #[serde(default)]
    effect: bool,
    /// The closed input schema as a TOML table.
    input_schema: Option<toml::Value>,
    /// The same schema as a JSON string, for hosts that keep schemas in JSON.
    input_schema_json: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceTable {
    name: String,
    version: Option<String>,
    /// The caller allowlist for this registration's tools.
    callers: Vec<String>,
    /// Tool names reserved to the control channel.
    #[serde(default)]
    control: Vec<String>,
    backend: BackendTable,
    /// The ordered tool catalog; each entry declares `name`, optional
    /// `description`, optional `effect` and exactly one input schema form.
    #[serde(rename = "tools")]
    tools: Vec<ToolTable>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServicesFile {
    server: Option<ServerTable>,
    /// Service-wide caller-context fields.
    #[serde(default)]
    context: Vec<ContextField>,
    /// The declared registrations; at least one.
    #[serde(rename = "service")]
    service: Vec<ServiceTable>,
}

/// Load the registration file and build the dispatcher plus the serving
/// identity. This is the `mcpd` composition root; embedding hosts build the
/// same `(Dispatcher, McpServerDefinition, Vec<ContextField>)` tuple in Rust
/// and never touch the file.
pub fn load_dispatcher(
    path: &Path,
) -> Result<(Dispatcher, McpServerDefinition, Vec<ContextField>)> {
    let text = fs::read_to_string(path).map_err(|_| anyhow!("mcpd_config_unavailable"))?;
    let file: ServicesFile = toml::from_str(&text).map_err(|_| anyhow!("mcpd_config_invalid"))?;
    build(&file)
}

fn build(file: &ServicesFile) -> Result<(Dispatcher, McpServerDefinition, Vec<ContextField>)> {
    ensure!(!file.service.is_empty(), "mcpd_config_services_empty");
    let mut attributes = BTreeSet::new();
    let mut headers = BTreeSet::new();
    let mut envs = BTreeSet::new();
    for field in &file.context {
        field.validate()?;
        ensure!(
            attributes.insert(&field.attribute)
                && headers.insert(&field.header)
                && envs.insert(&field.env),
            "mcpd_context_field_duplicate"
        );
    }

    let mut registrations = Vec::with_capacity(file.service.len());
    for service in &file.service {
        let backend = BackendTable::spec(&service.backend)?;
        let control: BTreeSet<&str> = service.control.iter().map(String::as_str).collect();
        let declared: BTreeSet<&str> = service
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        ensure!(
            control.iter().all(|name| declared.contains(name)),
            "mcpd_control_tool_undeclared"
        );
        let mut tools = Vec::with_capacity(service.tools.len());
        for tool in &service.tools {
            tools.push(ToolSpec::declared(
                &tool.name,
                tool.description.as_deref(),
                tool.input_schema()?,
                control.contains(tool.name.as_str()),
                tool.effect,
            )?);
        }
        registrations.push(Registration::new(
            service.name.clone(),
            service.version.clone().unwrap_or_else(|| "1".to_owned()),
            tools,
            service.callers.clone(),
            NativeBackend::new(backend)?,
        )?);
    }
    let dispatcher = Dispatcher::new(Registry::new(registrations)?);

    // The server definition borrows 'static strings by design; the deployed
    // identity lives for the rest of this process, so it is interned once
    // rather than carried as owned storage through the engine.
    let name = file
        .server
        .as_ref()
        .and_then(|server| server.name.clone())
        .unwrap_or_else(|| "mcpd".to_owned());
    let version = file
        .server
        .as_ref()
        .and_then(|server| server.version.clone())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
    ensure!(
        !name.is_empty() && !version.is_empty(),
        "mcpd_server_identity_invalid"
    );
    let definition = McpServerDefinition {
        protocol_revision: PROTOCOL_REVISION,
        compatible_protocol_revisions: COMPATIBLE_PROTOCOL_REVISIONS,
        server_name: Box::leak(name.into_boxed_str()),
        server_version: Box::leak(version.into_boxed_str()),
        max_message_bytes: MAX_MCP_FRAME_BYTES,
    };
    Ok((dispatcher, definition, file.context.clone()))
}

impl BackendTable {
    fn spec(&self) -> Result<NativeSpec> {
        let mut spec = NativeSpec::new(
            self.program.clone(),
            self.args.clone(),
            self.protocol.clone(),
            self.invoke.clone(),
        )?;
        if let Some(workflow_id) = &self.workflow_id {
            spec.workflow_id = workflow_id.clone();
        }
        spec.env = self.env.clone();
        spec.validate()?;
        Ok(spec)
    }
}

impl ToolTable {
    fn input_schema(&self) -> Result<Value> {
        match (&self.input_schema, &self.input_schema_json) {
            (Some(schema), None) => toml_to_json(schema),
            (None, Some(text)) => {
                serde_json::from_str(text).map_err(|_| anyhow!("mcpd_tool_schema_invalid"))
            }
            _ => Err(anyhow!("mcpd_tool_schema_invalid")),
        }
    }
}

/// Deterministic TOML→JSON conversion for declared schemas. TOML datetimes
/// become their RFC 3339 text form; every other scalar maps one-to-one.
fn toml_to_json(value: &toml::Value) -> Result<Value> {
    Ok(match value {
        toml::Value::String(text) => Value::String(text.clone()),
        toml::Value::Integer(number) => Value::Number((*number).into()),
        toml::Value::Float(number) => Value::Number(
            serde_json::Number::from_f64(*number)
                .ok_or_else(|| anyhow!("mcpd_tool_schema_invalid"))?,
        ),
        toml::Value::Boolean(flag) => Value::Bool(*flag),
        toml::Value::Datetime(moment) => Value::String(moment.to_string()),
        toml::Value::Array(items) => {
            Value::Array(items.iter().map(toml_to_json).collect::<Result<_>>()?)
        }
        toml::Value::Table(table) => Value::Object(
            table
                .iter()
                .map(|(key, value)| Ok((key.clone(), toml_to_json(value)?)))
                .collect::<Result<_>>()?,
        ),
    })
}

/// The `{request}` literal a backend `invoke` template substitutes — re-exported
/// so config validation and documentation share one spelling.
pub const fn request_placeholder() -> &'static str {
    REQUEST_PLACEHOLDER
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(text: &str) -> ServicesFile {
        toml::from_str(text).expect("fixture parses")
    }

    #[test]
    fn a_minimal_service_builds_a_native_registration() {
        let file = file(
            r##"
[[service]]
name = "alpha"
callers = ["codex"]
[service.backend]
program = "backend-bin"
args = ["serve"]
protocol = "mcpd.stdio.v1"
invoke = ["call", "{request}"]
[[service.tools]]
name = "alpha_tool"
input_schema = {type = "object", additionalProperties = false, properties = {}, required = []}
"##,
        );
        let (dispatcher, definition, context) = build(&file).unwrap();
        assert_eq!(dispatcher.caller_providers(), vec!["codex"]);
        assert_eq!(dispatcher.catalog_names(), vec!["alpha_tool"]);
        assert_eq!(definition.server_name, "mcpd");
        assert_eq!(definition.protocol_revision, "2025-06-18");
        assert!(context.is_empty());
    }

    #[test]
    fn two_services_serving_one_tool_name_is_refused() {
        let service = r##"
[[service]]
name = "alpha"
callers = ["codex"]
[service.backend]
program = "a"
protocol = "mcpd.stdio.v1"
invoke = ["{request}"]
[[service.tools]]
name = "shared"
input_schema_json = "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{}}"
[[service]]
name = "beta"
callers = ["cursor"]
[service.backend]
program = "b"
protocol = "mcpd.stdio.v1"
invoke = ["{request}"]
[[service.tools]]
name = "shared"
input_schema_json = "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{}}"
"##;
        let file = file(service);
        let error = build(&file)
            .map(|_| ())
            .expect_err("colliding catalogs cannot serve");
        assert_eq!(error.to_string(), "mcpd_tool_name_collision");
    }

    #[test]
    fn control_must_name_a_declared_tool() {
        let file = file(
            r##"
[[service]]
name = "alpha"
callers = ["codex"]
control = ["not_a_tool"]
[service.backend]
program = "a"
protocol = "mcpd.stdio.v1"
invoke = ["{request}"]
[[service.tools]]
name = "real_tool"
input_schema_json = "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{}}"
"##,
        );
        let error = build(&file)
            .map(|_| ())
            .expect_err("an undeclared control tool is a config defect");
        assert_eq!(error.to_string(), "mcpd_control_tool_undeclared");
    }

    #[test]
    fn exactly_one_schema_form_is_accepted() {
        let tool = |line: &str| format!("name = \"t\"\n{line}\n");
        let table = tool("input_schema = {type = \"object\", properties = {}}");
        let tool_table: ToolTable = toml::from_str(&table).unwrap();
        assert_eq!(tool_table.input_schema().unwrap()["type"], "object");
        let text = tool("input_schema_json = \"{\\\"type\\\":\\\"object\\\"}\"");
        let tool_table: ToolTable = toml::from_str(&text).unwrap();
        assert_eq!(tool_table.input_schema().unwrap()["type"], "object");
        let both = tool("input_schema = {type = \"object\"}\ninput_schema_json = \"{}\"");
        let tool_table: ToolTable = toml::from_str(&both).unwrap();
        assert!(tool_table.input_schema().is_err());
        let neither = tool("");
        let tool_table: ToolTable = toml::from_str(&neither).unwrap();
        assert!(tool_table.input_schema().is_err());
    }

    #[test]
    fn context_fields_must_not_alias_each_other() {
        let file = file(
            r##"
[[context]]
attribute = "conversationId"
header = "x-a-one"
env = "A_ONE"
[[context]]
attribute = "conversationId"
header = "x-a-two"
env = "A_TWO"
[[service]]
name = "alpha"
callers = ["codex"]
[service.backend]
program = "a"
protocol = "mcpd.stdio.v1"
invoke = ["{request}"]
[[service.tools]]
name = "t"
input_schema_json = "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{}}"
"##,
        );
        let error = build(&file)
            .map(|_| ())
            .expect_err("aliased attributes are ambiguous");
        assert_eq!(error.to_string(), "mcpd_context_field_duplicate");
    }

    #[test]
    fn toml_schemas_convert_without_loss() {
        let schema = toml_to_json(
            &toml::from_str::<toml::Value>(
                "type = \"object\"\nminimum = 1\nflag = true\nratio = 1.5\nnames = [\"a\"]",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["minimum"], 1);
        assert_eq!(schema["flag"], true);
        assert_eq!(schema["ratio"], 1.5);
        assert_eq!(schema["names"][0], "a");
    }
}
