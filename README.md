# MCP-Dispatcher

A generic, registration-based MCP service dispatcher. Any host registers
named tool services — an ordered catalog of closed input schemas plus a
handler backend — and the dispatcher serves their union catalog over
authenticated MCP: stdio for embedding, numeric-loopback HTTP for local
callers behind per-caller ephemeral bearer tokens.

The machinery is the service-neutral core extracted from LicoUp's
`licoup-public-gateway`, generalized so the dispatcher carries no host or
agent semantics of its own. `mcpd` is the standalone binary that serves
declaratively-registered services from a TOML file.

```text
mcpd serve --config services.toml [--home <dir>]   # foreground
mcpd service start --config services.toml          # detached
mcpd service status|stop|reload
mcpd connector --caller <provider>                 # stdio bridge for a caller
```

## The model

- **Registration** — a service name + version, an ordered tool catalog, a
  caller allowlist, a handler backend, and an optional control-channel
  reservation for cancellation-class tools.
- **Union catalog** — `tools/list` returns every registration's tools in
  declaration order. `tools/call` routes to the owning registration and
  enforces that registration's caller allowlist.
- **Collision = startup error** — two registrations declaring the same tool
  name refuse the whole config before anything serves. Never first-come-wins.
- **Backends** — `NativeBackend` pools child processes speaking a bounded
  newline-framed JSON protocol (`NativeSpec.program + args`, `{request}`
  placeholder in `invoke`, separate control pool); `InProcessBackend` takes a
  Rust trait-object handler for embedding hosts.
- **Callers** — each admitted caller gets a fresh bearer token per service
  generation, published through owner-only private discovery; the connector
  reads it and bridges the caller's stdio frames to the loopback service.

## Security boundary

- HTTP binds numeric loopback only (`127.0.0.1`, random port); exact `Host`
  check, `Origin` must be absent, bounded headers/connections/sessions.
- Bearer tokens are per-caller, per-generation, never logged.
- Discovery lives under the state root (`--home`, `MCPD_HOME`, else the
  platform user-data dir) with owner-only permissions, atomic writes and
  OS-lease liveness cleanup of stale publications.
- Tool input schemas are closed (`additionalProperties: false`, object
  schemas only) and validated before the call reaches a backend.
- Sessions idle for an hour are reaped — never mid-request, since an
  in-flight call pins its session. The TTL bounds only how long a dead
  session holds its slot: a live connector replays the client's own
  `initialize` to mint a replacement and retries the frame once,
  invisibly to the caller, so a connector may idle as long as its caller
  process lives (an overnight IDE or agent session is the normal case).
- An effect-bearing call (`effect = true`) whose transport fails mid-flight
  is reported `mcp_outcome_unknown` and is never replayed by a connector.
- Backend children inherit the real environment plus explicit `env`
  overrides — never a stripped environment.

## Repository layout

```text
src/wire.rs            JSON-RPC/MCP framing, stdio lines, HTTP bodies
src/server.rs          the service-neutral engine: initialize, tools/list,
                       tools/call, cancellation, bounded frames
src/registry.rs        Registration/Registry/ToolSpec/CallerContext/Channel
src/application.rs     Dispatcher = McpApplication over the Registry
src/backend/           NativeBackend (pooled child) + InProcessBackend
src/transport.rs       authenticated loopback HTTP + discovery + health
src/private_state.rs   owner-only state root custody (Unix)
src/windows_private_state.rs  Windows ACL policy (cfg-gated)
src/lifecycle.rs       serve/service start|stop|status|reload, OS lease
src/connector.rs       stdio→HTTP caller bridge
src/config.rs          services.toml composition root
src/bin/mcpd.rs        the binary
src/bin/stub_backend.rs  synthetic framed-protocol fixture for tests
presets/               declarative service files (incl. licoup-subagents)
docs/integration-modes.md
skills/                sdk + sidecar host skills
tests/serve.rs         end-to-end contract proof over the stub backend
```

## Verify

```sh
scripts/verify.sh   # fmt --check, clippy -D warnings, workspace tests
```

License: AGPL-3.0-or-later.
