---
name: flr-sidecar
description: Run flr as a managed sidecar — declarative services.toml, lifecycle control, and connector wiring for MCP callers.
---

# Running `flr` as a sidecar

Use this when the host is not Rust, or wants the dispatcher isolated in its
own process serving declaratively-registered tool services.

## Files and state

- `services.toml` — the registration file (`[server]`, `[[context]]`,
  `[[service]]` with `callers`, `control`, `[service.backend]`,
  `[[service.tools]]`). Validated before anything binds: duplicate tool
  names across services, unknown `control` entries, non-closed schemas and
  malformed backends all fail startup.
- State root — resolves as `--home`, then `FLR_HOME`, then the platform
  user-data dir. Holds owner-only `discovery.json` (endpoint, generation,
  per-caller bearer tokens). Every command touching a running sidecar needs
  the same state root.

## Lifecycle

```sh
flr serve --config services.toml            # foreground (supervisors)
flr service start --config services.toml    # detached
flr service status                          # {"service":"flr","state":"running"}
flr service reload --config services.toml   # new generation, live swap
flr service stop                            # withdraws discovery
```

`status` is a real health probe — authenticated initialize plus the exact
ordered union catalog — not just process liveness. A crashed service leaves
stale discovery; `stop` recognizes the dead owner via the OS lease and
withdraws it.

## Wiring a caller

Each admitted caller runs its own bridge:

```sh
flr connector --caller <provider>
```

The caller's MCP client speaks ordinary stdio JSON-RPC to the connector.
The connector reads the caller's bearer token from discovery, forwards
frames to `http://127.0.0.1:<port>/mcp`, and renews the session handshake
whenever the service publishes a new generation (e.g. after `reload`) or
the session was reaped for idleness — sessions idle an hour are collected,
which is never an error: the connector replays the client's own
`initialize` and retries the frame once, invisibly to the caller. The
connector therefore lives as long as its caller process; idle gaps of any
length are normal. One exception: an `effect` frame that met a dead session
is never resent — it reports `mcp_outcome_unknown` (below).

- A `--caller` absent from every registration's `callers` is refused before
  the first frame, with stderr naming the admitted set.
- `[[context]]` fields become env vars the connector forwards as headers:
  set `<env>` (e.g. `LICOUP_MCP_CONVERSATION_ID`) on the connector process
  to bind that attribute into the caller scope for every call.
- An `effect` tool whose request was sent but whose answer is lost reports
  `mcp_outcome_unknown` (`requestMayHaveExecuted`, `retryable=false`); a
  call that never left reports `mcp_module_unavailable` (`retryable=true`).
  Never re-send an uncertain call.

## Backend contract (`flr.stdio.v1`-style)

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
