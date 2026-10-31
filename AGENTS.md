# Agent rules for MCP-Dispatcher

## Boundary (hard rule)

This crate is generic infrastructure: it owns the MCP engine, the
registration seam, the authenticated loopback boundary, credential custody
and service lifecycle. It must not grow agent semantics — no host names,
no tool meanings, no conversation model. Host-specific surfaces (LicoUp's
admitted tools, its callers, its child protocol) live exclusively in
declarative config such as `presets/`, never in the engine, registry or
transport.

## Loopback-only authenticated boundary (hard rule)

The HTTP service binds numeric loopback only, checks `Host` exactly and
admits no `Origin`. Authentication is a per-caller ephemeral bearer token
published through owner-only private discovery. Tokens and any credential
material never appear in logs, diagnostics or serialized debug output.
Stale discovery is reclaimed through OS-lease liveness, never trusted.

## No replay of uncertain tool calls (hard rule)

A call to an `effect` tool whose request left the connector has an unknown
outcome after a transport failure. It is reported `mcp_outcome_unknown`
with `requestMayHaveExecuted` and `retryable=false`; nothing may resend it.

## Environment fidelity (hard rule)

Backend child processes receive the user's real inherited environment plus
the service's explicit `env` overrides. A clean or minimal child
environment is a defect: it silently strips the configuration, credentials
and tools the user installed. Explicit config may override values; it may
not discard the inherited set.

## Collisions fail closed (hard rule)

Two registrations declaring the same tool name are a startup error before
anything serves. Unknown control-tool references, malformed schemas,
non-closed schemas, unadmitted callers and ambiguous config likewise fail
before the boundary opens — never after, never first-come-wins.

## Verify

`scripts/verify.sh` — fmt + clippy -D warnings + tests.
