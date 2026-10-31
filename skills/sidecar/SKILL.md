---
name: mcpd-sidecar
description: Run mcpd as a managed sidecar — declarative services.toml, lifecycle control, and connector wiring for MCP callers.
---

# Running `mcpd` as a sidecar

Use this when the host is not Rust, or wants the dispatcher isolated in its
own process serving declaratively-registered tool services.

## Files and state

- `services.toml` — the registration file (`[server]`, `[[context]]`,
  `[[service]]` with `callers`, `control`, `[service.backend]`,
  `[[service.tools]]`). Validated before anything binds: duplicate tool
  names across services, unknown `control` entries, non-closed schemas and
  malformed backends all fail startup.
- State root — resolves as `--home`, then `MCPD_HOME`, then the platform
  user-data dir. Holds owner-only `discovery.json` (endpoint, generation,
  per-caller bearer tokens). Every command touching a running sidecar needs
  the same state root.

## Lifecycle

```sh
mcpd serve --config services.toml            # foreground (supervisors)
mcpd service start --config services.toml    # detached
mcpd service status                          # {"service":"mcpd","state":"running"}
mcpd service reload --config services.toml   # new generation, live swap
mcpd service stop                            # withdraws discovery
```

`status` is a real health probe — authenticated initialize plus the exact
ordered union catalog — not just process liveness. A crashed service leaves
stale discovery; `stop` recognizes the dead owner via the OS lease and
withdraws it.

## Wiring a caller

Each admitted caller runs its own bridge:

```sh
mcpd connector --caller <provider>
```

The caller's MCP client speaks ordinary stdio JSON-RPC to the connector.
The connector reads the caller's bearer token from discovery, forwards
frames to `http://127.0.0.1:<port>/mcp`, and renews the session handshake
whenever the service publishes a new generation (e.g. after `reload`).

- A `--caller` absent from every registration's `callers` is refused before
  the first frame, with stderr naming the admitted set.
- `[[context]]` fields become env vars the connector forwards as headers:
  set `<env>` (e.g. `LICOUP_MCP_CONVERSATION_ID`) on the connector process
  to bind that attribute into the caller scope for every call.
- An `effect` tool whose request was sent but whose answer is lost reports
  `mcp_outcome_unknown` (`requestMayHaveExecuted`, `retryable=false`); a
  call that never left reports `mcp_module_unavailable` (`retryable=true`).
  Never re-send an uncertain call.

## Backend contract (`mcpd.stdio.v1`-style)

`[service.backend]` spawns `program args`, then sends `invoke` argv with
the literal `{request}` replaced by the JSON tool request on the child's
stdin — one bounded JSON line per call, one bounded JSON line back. The
child inherits the real environment plus `[service.backend.env]` overrides;
do not strip PATH or host config.

A `control = [...]` tool gets its own reserved session in the backend pool,
so a cancel reaches the backend while an ordinary call is still running.

## Presets

`presets/licoup-subagents.toml` is the reference registration: LicoUp's
five admitted tools, its caller set, its child protocol and its control
reservation — copy the shape, keep your own `callers` honest, mark
`effect = true` on effect-bearing tools.
