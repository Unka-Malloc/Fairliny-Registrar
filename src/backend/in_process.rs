//! In-process handler backend for hosts embedding the crate directly.
//!
//! The host hands over one Rust call per registration — a closure or any
//! value implementing [`ToolHandler`] — and the dispatcher routes to it. No
//! child process, no frame boundary: the handler sees the same `ToolRequest`
//! a native backend would, including the reserved-channel classification and
//! the cooperative cancellation flag.

use crate::McpApplicationError;
use crate::registry::{ToolHandler, ToolRequest};
use serde_json::Value;

type HostHandler =
    dyn Fn(&ToolRequest<'_>) -> std::result::Result<Value, McpApplicationError> + Send + Sync;

/// A host-owned handler invoked in the dispatcher's process.
pub struct InProcessBackend {
    handler: Box<HostHandler>,
}

impl InProcessBackend {
    pub fn new(
        handler: impl Fn(&ToolRequest<'_>) -> std::result::Result<Value, McpApplicationError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            handler: Box::new(handler),
        }
    }
}

impl ToolHandler for InProcessBackend {
    fn call(&self, request: &ToolRequest<'_>) -> std::result::Result<Value, McpApplicationError> {
        (self.handler)(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{CallerContext, Channel};
    use serde_json::{Map, json};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn the_host_closure_sees_the_full_request() {
        let backend = InProcessBackend::new(|request| {
            Ok(json!({
                "tool": request.tool,
                "channel": request.channel == Channel::Control,
                "caller": request.caller.provider_id,
            }))
        });
        let caller = CallerContext {
            provider_id: "codex".to_owned(),
            attributes: BTreeMap::new(),
            authenticated: true,
        };
        let request = ToolRequest {
            tool: "probe",
            arguments: &Map::new(),
            caller: &caller,
            channel: Channel::Control,
            effect: false,
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let result = backend.call(&request).unwrap();
        assert_eq!(result["tool"], "probe");
        assert_eq!(result["channel"], true);
        assert_eq!(result["caller"], "codex");
    }
}
