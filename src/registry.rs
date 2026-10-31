//! The registration seam: hosts declare tool services once; the dispatcher
//! serves the union catalog and routes `tools/call` by tool name to the owning
//! registration.
//!
//! A [`Registration`] binds a service name and version, an ordered tool
//! catalog (closed JSON input schemas), an argument validator, one handler
//! backend, a caller allowlist and an optional control-channel reservation.
//! Tool-name collisions across registrations are a startup error — never
//! first-come-wins — because a silently shadowed tool routes real effects to
//! the wrong owner.

use crate::{McpApplicationError, McpToolCallContext};
use anyhow::{Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// The MCP tool-name grammar the registry enforces on declared tools.
pub(crate) fn valid_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// A published caller id is an opaque caller identifier. The union of all
/// registration allowlists is the capability set the service mints bearer
/// tokens for; the reader must be able to tell "no seat for this caller" from
/// "unreadable discovery", so the shape is structural, not a known-name list.
pub(crate) fn valid_caller_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn valid_context_attribute(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_context_header_name(value: &str) -> bool {
    value.len() >= 2
        && value.len() <= 128
        && value.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

fn valid_context_env_name(value: &str) -> bool {
    value.len() >= 2
        && value.len() <= 64
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_uppercase() || b == b'_')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

/// One caller-context channel between the connector and the service: the
/// environment variable the connector reads, the loopback header it forwards,
/// and the caller-scope attribute the backend receives.
///
/// The mapping is declared once per service deployment — the LicoUp preset
/// maps its conversation, membership and dispatch ids through it — and is
/// published inside the discovery document so the connector learns it from
/// the service rather than carrying a compiled-in copy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextField {
    /// The caller-scope attribute name handed to the backend.
    pub attribute: String,
    /// The HTTP header name forwarded on the loopback boundary.
    pub header: String,
    /// The environment variable the connector reads.
    pub env: String,
}

impl ContextField {
    pub fn new(
        attribute: impl Into<String>,
        header: impl Into<String>,
        env: impl Into<String>,
    ) -> Result<Self> {
        let field = Self {
            attribute: attribute.into(),
            header: header.into(),
            env: env.into(),
        };
        field.validate()?;
        Ok(field)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            valid_context_attribute(&self.attribute)
                && valid_context_header_name(&self.header)
                && valid_context_env_name(&self.env),
            "mcpd_context_field_invalid"
        );
        Ok(())
    }
}

/// Who is calling. The loopback transport mints this only after the bearer
/// token matched, so `authenticated` is the admission verdict, not a claim the
/// caller makes about itself. `attributes` carries the declared context
/// fields forwarded for this session; an absent attribute means the caller
/// never declared it — distinct from an empty one, which the header
/// validation rejects outright.
#[derive(Clone, Debug)]
pub struct CallerContext {
    pub provider_id: String,
    pub attributes: BTreeMap<String, String>,
    pub authenticated: bool,
}

/// Which pool a call takes. `Control` calls ride the registration's reserved
/// control session so cancellation-class tools never queue behind slow
/// ordinary work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Channel {
    General,
    Control,
}

/// One routed `tools/call`, handed to the owning registration's backend.
pub struct ToolRequest<'a> {
    pub tool: &'a str,
    pub arguments: &'a Map<String, Value>,
    pub caller: &'a CallerContext,
    pub channel: Channel,
    pub cancelled: Arc<AtomicBool>,
}

/// The handler backend one registration owns. `NativeBackend` bridges a
/// pooled child process; `InProcessBackend` is the embedding host's own
/// handler. `Ok` payloads become the tool result verbatim; `Err` becomes the
/// structured `mcpd.error.v1` payload — a backend never invents the transport
/// error shape itself.
pub trait ToolHandler: Send + Sync {
    fn call(&self, request: &ToolRequest<'_>) -> std::result::Result<Value, McpApplicationError>;
}

/// One declared tool: the catalog entry served verbatim by `tools/list`, plus
/// the admission metadata the dispatcher itself consumes. `control` reserves
/// the registration's control channel; `effect` marks the call as
/// effect-bearing so a caller never replays it after an uncertain transport
/// failure.
#[derive(Clone, Debug)]
pub struct ToolSpec {
    name: String,
    catalog_entry: Value,
    input_schema: Value,
    control: bool,
    effect: bool,
}

impl ToolSpec {
    /// `catalog_entry` is served as-is by `tools/list`; it must carry `name`
    /// (valid tool grammar) and an object `inputSchema`.
    pub fn new(catalog_entry: Value, control: bool, effect: bool) -> Result<Self> {
        let name = catalog_entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("mcpd_tool_spec_invalid"))?;
        ensure!(valid_tool_name(name), "mcpd_tool_spec_invalid");
        let input_schema = catalog_entry
            .get("inputSchema")
            .filter(|schema| schema.is_object())
            .cloned()
            .ok_or_else(|| anyhow!("mcpd_tool_spec_invalid"))?;
        Ok(Self {
            name: name.to_owned(),
            catalog_entry,
            input_schema,
            control,
            effect,
        })
    }

    /// Build the plain `{name, description, inputSchema}` entry; description
    /// defaults to the tool name when the registration does not declare one.
    pub fn declared(
        name: &str,
        description: Option<&str>,
        input_schema: Value,
        control: bool,
        effect: bool,
    ) -> Result<Self> {
        let entry = serde_json::json!({
            "name": name,
            "description": description.unwrap_or(name),
            "inputSchema": input_schema,
        });
        Self::new(entry, control, effect)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn catalog_entry(&self) -> &Value {
        &self.catalog_entry
    }
    pub fn control(&self) -> bool {
        self.control
    }
    pub fn effect(&self) -> bool {
        self.effect
    }
    fn validate_arguments(&self, arguments: &Map<String, Value>) -> bool {
        valid_arguments(&self.input_schema, arguments)
    }
}

/// One registered tool service.
pub struct Registration {
    name: String,
    version: String,
    tools: Vec<ToolSpec>,
    callers: Vec<String>,
    handler: Arc<dyn ToolHandler>,
}

impl Registration {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        tools: Vec<ToolSpec>,
        callers: Vec<String>,
        handler: impl ToolHandler + 'static,
    ) -> Result<Self> {
        let name = name.into();
        let version = version.into();
        ensure!(
            !name.is_empty() && !version.is_empty(),
            "mcpd_service_invalid"
        );
        ensure!(!tools.is_empty(), "mcpd_service_tools_empty");
        let mut seen = BTreeSet::new();
        ensure!(
            tools.iter().all(|tool| seen.insert(tool.name())),
            "mcpd_tool_name_collision"
        );
        ensure!(!callers.is_empty(), "mcpd_service_callers_empty");
        ensure!(
            callers.iter().all(|caller| valid_caller_id(caller)),
            "mcpd_caller_invalid"
        );
        Ok(Self {
            name,
            version,
            tools,
            callers,
            handler: Arc::new(handler),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    /// The caller allowlist: only these provider ids may invoke this
    /// registration's tools, even though the token map is the union of every
    /// registration's callers.
    pub fn callers(&self) -> &[String] {
        &self.callers
    }
    fn admits(&self, provider_id: &str) -> bool {
        self.callers.iter().any(|caller| caller == provider_id)
    }
    fn control_session_reserved(&self) -> bool {
        self.tools.iter().any(|tool| tool.control)
    }
}

/// The built registration set: union catalog, routing table, capability set.
pub struct Registry {
    registrations: Vec<Registration>,
    catalog: Vec<Value>,
    routes: HashMap<String, (usize, usize)>,
    callers: BTreeSet<String>,
    control_limit: usize,
    effect_tools: BTreeSet<String>,
}

impl Registry {
    /// Order is catalog order: tools appear in `tools/list` in the order the
    /// registrations declared them. Any duplicate tool name across the whole
    /// set fails startup — a collision is a configuration defect, never a
    /// first-come-wins merge.
    pub fn new(registrations: Vec<Registration>) -> Result<Self> {
        ensure!(!registrations.is_empty(), "mcpd_registry_empty");
        let mut catalog = Vec::new();
        let mut routes = HashMap::new();
        let mut callers = BTreeSet::new();
        let mut control_limit = 0_usize;
        let mut effect_tools = BTreeSet::new();
        for (service, registration) in registrations.iter().enumerate() {
            callers.extend(registration.callers.iter().cloned());
            if registration.control_session_reserved() {
                control_limit += 1;
            }
            for (tool, spec) in registration.tools.iter().enumerate() {
                ensure!(
                    routes
                        .insert(spec.name().to_owned(), (service, tool))
                        .is_none(),
                    "mcpd_tool_name_collision"
                );
                if spec.effect {
                    effect_tools.insert(spec.name().to_owned());
                }
                catalog.push(spec.catalog_entry.clone());
            }
        }
        Ok(Self {
            registrations,
            catalog,
            routes,
            callers,
            control_limit: control_limit.max(1),
            effect_tools,
        })
    }

    /// The admitted caller set — the union of every registration's allowlist,
    /// ordered for deterministic token publication.
    pub fn callers(&self) -> Vec<String> {
        self.callers.iter().cloned().collect()
    }

    /// The union catalog served by `tools/list`, in declaration order.
    pub fn catalog(&self) -> Vec<Value> {
        self.catalog.clone()
    }

    /// The union tool names in catalog order — the health probe's contract.
    pub fn catalog_names(&self) -> Vec<String> {
        self.catalog
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_owned))
            .collect()
    }

    /// Effect-bearing tools, published in discovery so a connector can mark a
    /// call whose outcome is uncertain after a transport failure. No replay.
    pub fn effect_tools(&self) -> Vec<String> {
        self.effect_tools.iter().cloned().collect()
    }

    /// How many control permits exist: one per registration that reserved a
    /// control channel, at least one.
    pub fn control_limit(&self) -> usize {
        self.control_limit
    }

    pub fn is_control_tool(&self, name: &str) -> bool {
        self.route(name).is_some_and(|(_, spec)| spec.control())
    }

    fn route(&self, name: &str) -> Option<(&Registration, &ToolSpec)> {
        let (service, tool) = self.routes.get(name)?;
        Some((
            &self.registrations[*service],
            &self.registrations[*service].tools[*tool],
        ))
    }

    /// Closed-schema validation for `tools/call` — the same admission the
    /// engine applies before the call reaches a handler. The registration's
    /// authoritative validation still happens inside the handler backend;
    /// this check only refuses calls the declared schema cannot describe.
    pub fn validate_tool_arguments(&self, name: &str, arguments: &Map<String, Value>) -> bool {
        self.route(name)
            .is_some_and(|(_, spec)| spec.validate_arguments(arguments))
    }

    /// Whether this caller may invoke this tool: the caller must be on the
    /// owning registration's allowlist, not merely hold some service token.
    pub fn admits(&self, name: &str, provider_id: &str) -> bool {
        self.route(name)
            .is_some_and(|(registration, _)| registration.admits(provider_id))
    }

    /// Route the call to the owning registration's backend. Caller admission
    /// is checked here — a caller outside the owning registration's allowlist
    /// is refused before the handler is touched.
    pub fn call_tool(
        &self,
        context: McpToolCallContext<'_, CallerContext>,
        name: &str,
        arguments: &Map<String, Value>,
    ) -> std::result::Result<Value, McpApplicationError> {
        let Some((registration, spec)) = self.route(name) else {
            return Err(McpApplicationError::permanent(
                "remote_operation_not_allowed",
                "admission",
            ));
        };
        if !context.caller.authenticated || !registration.admits(&context.caller.provider_id) {
            return Err(McpApplicationError::permanent(
                "remote_operation_not_allowed",
                "admission",
            ));
        }
        registration.handler.call(&ToolRequest {
            tool: spec.name(),
            arguments,
            caller: context.caller,
            channel: if spec.control() {
                Channel::Control
            } else {
                Channel::General
            },
            cancelled: context.cancelled,
        })
    }
}

// Validate the closed scalar schemas registrations declare, without carrying
// a second domain schema. The owning handler still performs its authoritative
// validation; this layer only refuses what the declared schema cannot mean.
fn valid_arguments(schema: &Value, arguments: &Map<String, Value>) -> bool {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return false;
    };
    if arguments.keys().any(|key| !properties.contains_key(key)) {
        return false;
    }
    if schema["required"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|name| !arguments.contains_key(name))
    {
        return false;
    }
    arguments.iter().all(|(key, value)| {
        let property = &properties[key];
        if property["enum"]
            .as_array()
            .is_some_and(|choices| !choices.contains(value))
        {
            return false;
        }
        match property["type"].as_str() {
            Some("string") => value.as_str().is_some_and(|text| {
                !text.trim().is_empty()
                    && !text.contains('\0')
                    && text.len() as u64 >= property["minLength"].as_u64().unwrap_or(0)
                    && text.len() as u64 <= property["maxLength"].as_u64().unwrap_or(u64::MAX)
            }),
            Some("integer") => value.as_u64().is_some_and(|number| {
                number >= property["minimum"].as_u64().unwrap_or(0)
                    && number <= property["maximum"].as_u64().unwrap_or(u64::MAX)
            }),
            Some("boolean") => value.is_boolean(),
            Some("object") => value.is_object(),
            Some("array") => value.as_array().is_some_and(|items| {
                items.len() as u64 <= property["maxItems"].as_u64().unwrap_or(u64::MAX)
            }),
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::Ordering;

    struct Echo;
    impl ToolHandler for Echo {
        fn call(
            &self,
            request: &ToolRequest<'_>,
        ) -> std::result::Result<Value, McpApplicationError> {
            Ok(json!({
                "tool": request.tool,
                "arguments": request.arguments,
                "caller": request.caller.provider_id,
                "channel": match request.channel {
                    Channel::General => "general",
                    Channel::Control => "control",
                },
            }))
        }
    }

    fn tool(name: &str, control: bool, effect: bool) -> ToolSpec {
        ToolSpec::declared(
            name,
            None,
            json!({"type":"object","additionalProperties":false,"properties":{},"required":[]}),
            control,
            effect,
        )
        .unwrap()
    }

    fn registration(name: &str, tools: Vec<ToolSpec>, callers: &[&str]) -> Registration {
        Registration::new(
            name,
            "1",
            tools,
            callers.iter().map(|caller| (*caller).to_owned()).collect(),
            Echo,
        )
        .unwrap()
    }

    fn caller(provider: &str) -> CallerContext {
        CallerContext {
            provider_id: provider.to_owned(),
            attributes: BTreeMap::new(),
            authenticated: true,
        }
    }

    #[test]
    fn tool_name_collision_across_registrations_is_a_startup_error() {
        let first = registration("alpha", vec![tool("shared_tool", false, false)], &["a"]);
        let second = registration("beta", vec![tool("shared_tool", false, false)], &["b"]);
        let error = Registry::new(vec![first, second])
            .map(|_| ())
            .expect_err("the same tool name must never merge silently");
        assert_eq!(error.to_string(), "mcpd_tool_name_collision");
    }

    #[test]
    fn duplicate_tool_inside_one_registration_is_a_startup_error() {
        let tools = vec![tool("same", false, false), tool("same", false, false)];
        let error = Registration::new("alpha", "1", tools, vec!["a".to_owned()], Echo)
            .map(|_| ())
            .expect_err("a service may not shadow itself");
        assert_eq!(error.to_string(), "mcpd_tool_name_collision");
    }

    #[test]
    fn union_catalog_preserves_declaration_order_and_callers() {
        let alpha = registration(
            "alpha",
            vec![tool("a_one", false, false), tool("a_two", true, true)],
            &["codex", "cursor"],
        );
        let beta = registration("beta", vec![tool("b_one", false, true)], &["cursor"]);
        let registry = Registry::new(vec![alpha, beta]).unwrap();
        assert_eq!(registry.catalog_names(), vec!["a_one", "a_two", "b_one"]);
        assert_eq!(registry.callers(), vec!["codex", "cursor"]);
        assert_eq!(registry.effect_tools(), vec!["a_two", "b_one"]);
        assert!(registry.is_control_tool("a_two"));
        assert!(!registry.is_control_tool("a_one"));
        assert_eq!(registry.control_limit(), 1);
    }

    #[test]
    fn per_registration_allowlists_admit_only_their_own_callers() {
        let alpha = registration("alpha", vec![tool("a_one", false, false)], &["codex"]);
        let beta = registration("beta", vec![tool("b_one", false, false)], &["cursor"]);
        let registry = Registry::new(vec![alpha, beta]).unwrap();
        assert!(registry.admits("a_one", "codex"));
        assert!(!registry.admits("a_one", "cursor"));
        assert!(registry.admits("b_one", "cursor"));
        assert!(!registry.admits("b_one", "codex"));
        assert!(!registry.admits("missing", "codex"));
    }

    #[test]
    fn routing_reaches_the_owning_backend_with_its_channel() {
        let alpha = registration(
            "alpha",
            vec![
                tool("ordinary", false, false),
                tool("control_ping", true, true),
            ],
            &["codex"],
        );
        let registry = Registry::new(vec![alpha]).unwrap();
        let admitted = caller("codex");
        let cancelled = Arc::new(AtomicBool::new(false));
        let called = registry
            .call_tool(
                McpToolCallContext {
                    caller: &admitted,
                    cancelled: Arc::clone(&cancelled),
                },
                "control_ping",
                &Map::new(),
            )
            .unwrap();
        assert_eq!(called["tool"], "control_ping");
        assert_eq!(called["channel"], "control");
        assert!(!cancelled.load(Ordering::Acquire));

        // A caller without a seat on this registration is refused before the
        // handler — same admission surface as the lifted implementation.
        let outsider = caller("cursor");
        let refused = registry
            .call_tool(
                McpToolCallContext {
                    caller: &outsider,
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                "ordinary",
                &Map::new(),
            )
            .map(|_| ())
            .expect_err("cursor holds no seat on alpha");
        assert_eq!(refused.code, "remote_operation_not_allowed");
        assert!(!refused.retryable);
    }

    #[test]
    fn closed_schemas_validate_exactly_like_the_lifted_engine() {
        let spec = ToolSpec::declared(
            "typed",
            None,
            json!({
                "type":"object",
                "additionalProperties":false,
                "properties":{
                    "prompt":{"type":"string","minLength":1,"maxLength":8},
                    "mode":{"type":"string","enum":["fast","slow"]},
                    "count":{"type":"integer","minimum":1,"maximum":4},
                    "flag":{"type":"boolean"},
                    "bag":{"type":"object"},
                    "items":{"type":"array","maxItems":2}
                },
                "required":["prompt"]
            }),
            false,
            false,
        )
        .unwrap();
        let registration = registration("alpha", vec![spec], &["codex"]);
        let registry = Registry::new(vec![registration]).unwrap();
        let valid = |value: Value| {
            registry.validate_tool_arguments("typed", value.as_object().expect("object arguments"))
        };
        assert!(valid(
            json!({"prompt":"hello","mode":"fast","count":2,"flag":true,"bag":{},"items":[1,2]})
        ));
        // A missing required member, an undeclared member, a blank string, an
        // out-of-range scalar and a wrong type all fail the same closed shape.
        assert!(!valid(json!({})));
        assert!(!valid(json!({"prompt":"hello","extra":1})));
        assert!(!valid(json!({"prompt":"  "})));
        assert!(!valid(json!({"prompt":"hello","count":0})));
        assert!(!valid(json!({"prompt":"hello","count":5})));
        assert!(!valid(json!({"prompt":"hello","mode":"other"})));
        assert!(!valid(json!({"prompt":"hello","items":[1,2,3]})));
        assert!(!valid(json!({"prompt":"hello","flag":"yes"})));
        assert!(!registry.validate_tool_arguments("missing", &Map::new()));
    }

    #[test]
    fn tool_specs_require_a_valid_name_and_object_schema() {
        assert!(
            ToolSpec::new(
                json!({"name":"ok_tool","inputSchema":{"type":"object"}}),
                false,
                false
            )
            .is_ok()
        );
        for entry in [
            json!({"name":"has space","inputSchema":{"type":"object"}}),
            json!({"name":"","inputSchema":{"type":"object"}}),
            json!({"name":"ok","inputSchema":"object"}),
            json!({"name":"ok"}),
            json!({"inputSchema":{"type":"object"}}),
        ] {
            assert!(ToolSpec::new(entry, false, false).is_err());
        }
    }

    #[test]
    fn context_fields_validate_their_three_names() {
        assert!(
            ContextField::new("conversationId", "x-host-conversation", "HOST_CONVERSATION").is_ok()
        );
        assert!(ContextField::new("bad attr", "x-host-a", "HOST_A").is_err());
        assert!(ContextField::new("attr", "Not-Lower", "HOST_A").is_err());
        assert!(ContextField::new("attr", "x-host-a", "lowercase").is_err());
    }
}
