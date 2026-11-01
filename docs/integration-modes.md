# Integration modes

MCP-Dispatcher serves one union catalog three ways. All modes share the
same engine, the same admission rules and the same collision-fails-closed
registration semantics; they differ only in who builds the registrations
and which transport faces the caller.

## 1. Embedded Rust SDK

A Rust host links `mcp-dispatcher`, builds `Registration`s in code and
serves them directly — no TOML, no child config parsing.

```rust,no_run
use mcp_dispatcher::{
    CallerContext, COMPATIBLE_PROTOCOL_REVISIONS, Dispatcher, InProcessBackend,
    MAX_MCP_FRAME_BYTES, McpServerDefinition, McpServerEngine, PROTOCOL_REVISION,
    Registration, ToolSpec, serve_stdio,
};

fn main() -> anyhow::Result<()> {
    let tools = vec![ToolSpec::declared(
        "my_tool",
        Some("does one thing"),
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {"text": {"type": "string"}},
            "required": ["text"]
        }),
        false, // control-channel reservation
        false, // effect-bearing
    )?];

    let registration = Registration::new(
        "my-service",
        "1.0.0",
        tools,
        vec!["my-caller".to_owned()],
        InProcessBackend::new(|request| {
            // request.tool, request.arguments, request.caller, request.channel,
            // request.effect, request.cancelled (cooperative cancellation flag)
            Ok(serde_json::json!({"echo": request.arguments}))
        }),
    )?;

    let dispatcher = Dispatcher::from_registrations(vec![registration])?;
    let engine = McpServerEngine::new(
        McpServerDefinition {
            protocol_revision: PROTOCOL_REVISION,
            compatible_protocol_revisions: COMPATIBLE_PROTOCOL_REVISIONS,
            server_name: "my-dispatcher",
            server_version: env!("CARGO_PKG_VERSION"),
            max_message_bytes: MAX_MCP_FRAME_BYTES,
        },
        dispatcher,
    )?;
    let caller = CallerContext {
        provider_id: "my-caller".to_owned(),
        attributes: Default::default(),
        authenticated: true,
    };
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    serve_stdio(&engine, &caller, stdin.lock(), stdout.lock())?;
    Ok(())
}
```

- Use `InProcessBackend` when the handler is Rust code in the same process.
- Use `NativeBackend` with a `NativeSpec` when the handler is a child
  program speaking the bounded framed protocol over its own stdio.
- `serve_stdio` is the raw engine loop. For the authenticated loopback HTTP
  boundary, hand the same `(McpServerDefinition, Dispatcher)` pair to
  `transport::HttpService::start`.

## 2. `mcpd serve` sidecar

A host that is not Rust — or that wants process isolation — declares its
services in TOML and runs the binary:

```toml
[server]
name = "my-dispatcher"
version = "1.0.0"

# Optional caller-context fields: session header -> caller attribute.
[[context]]
attribute = "conversationId"
header = "x-my-conversation-id"
env = "MY_MCP_CONVERSATION_ID"

[[service]]
name = "alpha"
callers = ["caller-a", "caller-b"]
control = ["alpha_cancel"]            # must name declared tools

[service.backend]
program = "/usr/local/bin/my-backend"
args = ["--serve"]
protocol = "myhost.stdio.v1"
invoke = ["tool", "run", "{request}"]  # {request} = the JSON request
env = { MY_FLAG = "1" }               # overrides on the inherited env

[[service.tools]]
name = "alpha_ping"
input_schema_json = '''
{"type":"object","additionalProperties":false,"properties":{},"required":[]}
'''

[[service.tools]]
name = "alpha_act"
effect = true                          # never replayed on uncertain outcome
input_schema_json = '''
{"type":"object","additionalProperties":false,
 "properties":{"id":{"type":"string","minLength":1}},
 "required":["id"]}
'''

[[service.tools]]
name = "alpha_cancel"
effect = true
input_schema_json = '''
{"type":"object","additionalProperties":false,
 "properties":{"id":{"type":"string"}},"required":[]}
'''
```

Then:

```sh
MCPD_HOME=<state dir> mcpd service start --config services.toml
MCPD_HOME=<state dir> mcpd service status    # {"service":"mcpd","state":"running"}
MCPD_HOME=<state dir> mcpd service reload --config services.toml
MCPD_HOME=<state dir> mcpd service stop
```

Each admitted caller connects through its own connector process:

```sh
MCPD_HOME=<state dir> mcpd connector --caller caller-a
```

The caller's MCP client talks ordinary stdio JSON-RPC to the connector;
the connector authenticates to the loopback service with that caller's
published bearer token. A caller absent from every `callers` list is
refused before the first frame.

Config is validated before anything serves: unknown control tools,
duplicate tool names across `[[service]]` blocks, non-object or
non-closed schemas, empty caller lists and malformed backend specs all
fail startup.

## 3. Presets

`presets/licoup-subagents.toml` is a worked example: LicoUp's five
admitted subagent tools routed to `licoup subagents execute --stdin-json`
over `licoup.stdio.v1`, `lico_subagent_cancel` on the control channel, and
the adapter-registry caller set (`antigravity`, `claude-code`, `codex`,
`cursor`).

Read-only tools (`lico_subagents_list`, `lico_subagent_probe`) keep no
`effect` flag and no channel reservation. Effect-bearing tools
(`lico_subagent_delegate`, `lico_subagent_continue`, `lico_subagent_cancel`)
are marked `effect = true` so a mid-flight transport failure reports an
uncertain outcome instead of a retry — the semantics live in the
dispatcher; the *which-tools* decision lives in the preset.

Extending a host's remote surface is additive: add `[[service]]` blocks or
more `[[service.tools]]`, keep each registration's `callers` honest, and
mark `effect = true` on anything whose re-execution would double a real
world effect.

## Choosing

| You are...                          | Mode |
|-------------------------------------|------|
| A Rust host owning the process      | Embedded SDK (`InProcessBackend` or `NativeBackend`) |
| Any host wanting a managed sidecar  | `mcpd` + `services.toml` |
| Reusing an existing admitted surface | Copy a preset, adjust callers/backends |
