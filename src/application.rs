//! The dispatcher: one [`McpApplication`] assembled from a [`Registry`].
//!
//! The engine asks for the union catalog, validates arguments against the
//! declared closed schemas, and hands admitted calls to the owning
//! registration's backend. Per-registration allowlists decide who may call
//! what — the published token set is the union of every allowlist, while a
//! call is admitted only against the *owning* registration's list.

use crate::registry::{CallerContext, Registry};
use crate::{McpApplication, McpApplicationError, McpToolCallContext};
use anyhow::Result;
use serde_json::{Map, Value};
use std::sync::Arc;

/// The protocol revisions this dispatcher negotiates, fixed for every
/// deployment: the current revision plus its documented compatible one.
pub const PROTOCOL_REVISION: &str = "2025-06-18";
pub const COMPATIBLE_PROTOCOL_REVISIONS: &[&str] = &["2025-11-25"];

/// The default frame bound for this service's MCP messages.
pub const MAX_MCP_FRAME_BYTES: usize = 64 * 1024;

/// Whether a protocol revision is one this dispatcher can negotiate. Shared
/// by the serving side and the connector, so the two never drift on what a
/// peer may ask for.
pub fn supports_protocol_revision(revision: &str) -> bool {
    revision == PROTOCOL_REVISION || COMPATIBLE_PROTOCOL_REVISIONS.contains(&revision)
}

/// The registration-driven application the engine and transport share.
#[derive(Clone)]
pub struct Dispatcher {
    registry: Arc<Registry>,
}

impl Dispatcher {
    pub fn new(registry: Registry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    /// Assemble the dispatcher from already-built registrations.
    pub fn from_registrations(registrations: Vec<crate::registry::Registration>) -> Result<Self> {
        Ok(Self::new(Registry::new(registrations)?))
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The admitted caller set — the union of every registration's allowlist.
    /// The supervisor mints exactly these bearer tokens; neither side keeps a
    /// private copy of the list.
    pub fn caller_providers(&self) -> Vec<String> {
        self.registry.callers()
    }

    pub fn is_control_tool(&self, name: &str) -> bool {
        self.registry.is_control_tool(name)
    }

    pub fn control_limit(&self) -> usize {
        self.registry.control_limit()
    }

    pub fn catalog_names(&self) -> Vec<String> {
        self.registry.catalog_names()
    }

    pub fn effect_tools(&self) -> Vec<String> {
        self.registry.effect_tools()
    }
}

impl McpApplication for Dispatcher {
    type CallerContext = CallerContext;

    fn tool_catalog(&self) -> Vec<Value> {
        self.registry.catalog()
    }

    fn has_tool(&self, name: &str) -> bool {
        self.registry.has_tool(name)
    }

    fn validate_tool_arguments(&self, name: &str, arguments: &Map<String, Value>) -> bool {
        self.registry.validate_tool_arguments(name, arguments)
    }

    fn call_tool(
        &self,
        context: McpToolCallContext<'_, CallerContext>,
        name: &str,
        arguments: &Map<String, Value>,
    ) -> std::result::Result<Value, McpApplicationError> {
        self.registry.call_tool(context, name, arguments)
    }
}
