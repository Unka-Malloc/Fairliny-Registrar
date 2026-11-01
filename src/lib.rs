//! MCP-Dispatcher: the registration-based MCP service dispatcher.
//!
//! The crate is three layers over one engine:
//!
//! * `wire` + `server` — the service-neutral MCP engine: JSON-RPC framing,
//!   initialization and protocol negotiation (2025-06-18 with 2025-11-25
//!   compat), tool listing, cancellation and calls, bounded frames, the stdio
//!   serve loop and JSON-RPC error discipline.
//! * `registry` — the registration seam: each [`Registration`] binds a
//!   service name and version, an ordered tool catalog of closed schemas, an
//!   argument validator, one handler [`crate::registry::ToolHandler`]
//!   backend, a caller allowlist and an optional control-channel reservation.
//!   Tool-name collisions across registrations are a startup error. The
//!   [`Dispatcher`] implements [`McpApplication`] over the registry.
//! * `transport` + `connector` + `lifecycle` + `private_state` — the
//!   authenticated numeric-loopback HTTP service (exact-Host check, Origin
//!   omission, per-caller ephemeral bearer tokens in owner-only discovery),
//!   the thin stdio connector bridging a caller process to that service, the
//!   local service lifecycle and the credential custody primitives.
//!
//! `config` is the `mcpd serve --config services.toml` composition root; the
//! same tuple is built in Rust by embedding hosts.

pub mod application;
pub mod backend;
pub mod config;
pub mod connector;
pub mod lifecycle;
pub mod private_state;
pub mod registry;
mod server;
pub mod transport;
#[cfg(any(windows, test))]
mod windows_private_state;
pub mod wire;

/// The documented integration modes, included so the advertised SDK surface
/// is compiled as a doctest and can never silently rot.
#[doc(hidden)]
#[doc = include_str!("../docs/integration-modes.md")]
mod integration_modes {}

pub use application::{
    COMPATIBLE_PROTOCOL_REVISIONS, Dispatcher, MAX_MCP_FRAME_BYTES, PROTOCOL_REVISION,
    supports_protocol_revision,
};
pub use backend::{InProcessBackend, NativeBackend, NativeSpec, REQUEST_PLACEHOLDER};
pub use registry::{CallerContext, ContextField, Registration, Registry, ToolSpec};
pub use server::*;
pub use wire::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    mod server;
    fn object(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }
}
