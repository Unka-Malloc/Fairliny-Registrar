---
name: mcpd-sdk
description: Embed mcp-dispatcher as a Rust library — build registrations in code and serve MCP over stdio or the authenticated loopback transport.
---

# Embedding MCP-Dispatcher as an SDK

Use this when the host is Rust and wants to own the process: no TOML, no
`mcpd` binary, registrations built in code.

## The shape

```rust
Registration::new(
    name,        // service name, e.g. "my-tools"
    version,     // service version string
    tools,       // Vec<ToolSpec> — ordered catalog
    callers,     // Vec<String> — provider ids this registration admits
    handler,     // impl ToolHandler — InProcessBackend or NativeBackend
)
```

Compose the registrations into a `Registry` (or let `Dispatcher` do it),
build an `McpServerEngine` over a `McpServerDefinition`, and serve.

- `ToolSpec::declared(name, description, input_schema, control, effect)` —
  the `input_schema` must be an object schema; closed schemas
  (`additionalProperties: false`) are the admission discipline: undeclared
  arguments are rejected before the call reaches your handler.
- `ToolSpec::new(catalog_entry, control, effect)` — when you already hold a
  full `{name, description, inputSchema, ...}` catalog JSON value; it is
  served verbatim by `tools/list`.
- `control: true` reserves the call for the registration's control channel
  (cancellation-class tools that must not queue behind slow work).
- `effect: true` marks the call effect-bearing: connectors report
  `mcp_outcome_unknown` and forbid replay after a mid-flight transport
  failure. Set it on anything whose second execution would double an effect.
- A duplicate tool name across registrations is a startup `Err`, never a
  silent merge.

## Backends

- `InProcessBackend::new(|request: &ToolRequest| -> Result<Value, McpApplicationError>)`
  — your handler in-process. `request` carries `tool`, `arguments`,
  `caller` (`provider_id` + declared `attributes`; `authenticated` is the
  transport's verdict, never forwarded to a backend scope), `channel`
  (`General`/`Control`) and `cancelled` (cooperative flag set by
  `notifications/cancelled`).
- `NativeBackend::new(NativeSpec::new(program, args, protocol, invoke)?)`
  — a pooled child process speaking newline-framed JSON. `invoke` is the
  argv template; the literal `{request}` element is replaced by the JSON
  tool request. `workflow_id`, `env` overrides and `max_frame_bytes` are
  public fields set after construction. The child inherits your process
  environment plus `env` overrides — never a stripped environment.
  `protocol` is the handshake name every frame carries.

## Transports

- `serve_stdio(&engine, &caller, reader, writer)` — one stdio session for
  one already-trusted caller (an embedding host vouches for identity).
- `transport::HttpService::start(dispatcher, definition, context_fields)`
  — binds numeric loopback, publishes owner-only discovery with per-caller
  bearer tokens, enforces exact `Host`/no `Origin`, bounds connections,
  sessions and tool workers. `ContextField::new(attribute, header, env)`
  declares which caller-scope attributes cross the boundary.

Protocol negotiation: primary revision `2025-06-18`, compatible
`2025-11-25`, via `PROTOCOL_REVISION` / `COMPATIBLE_PROTOCOL_REVISIONS`.

## Rules

- The engine owns JSON-RPC error shape and `mcpd.error.v1` payloads;
  handlers return `Ok(value)` or `Err(McpApplicationError)` — never raw
  transport errors.
- Do not give a handler its own view of authentication; trust
  `caller.authenticated` as delivered.
- `scripts/verify.sh` is the gate: fmt, clippy `-D warnings`, tests.
