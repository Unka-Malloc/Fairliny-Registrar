# Composition: one registry, two serving shapes

Fairliny-Registrar is an embedded registration engine, not a server
directory. A host registers its own named services — CLI-backed tools,
catalogs, control surfaces — and the engine answers for them: a union
`tools/list`, ownership-routed `tools/call`, caller allowlists and the
authenticated loopback boundary.

## Two serving shapes, one registry

- **Standalone** — the `flr` binary or the library serving a host's
  own boundary: stdio, or loopback HTTP with per-caller ephemeral
  bearer tokens.
- **Composed** — inside a host face such as Fairliny-Gateway, where the
  registry is one more surface beside model forwarding.

Both shapes must see the same registrations. The composition root
constructs the `Registry` once and injects it into every face that
serves MCP — two faces never build two registries, because that would
mean two auth domains, two catalogs and two token issuers over one
logical surface.

## What stays generic here

The engine knows registration, catalog union, ownership routing and the
loopback boundary. It does not know what a tool *means* — host-specific
tools, admitted callers and child-protocol choices live in declarative
presets (`presets/`), never in the engine, registry or transport. The
`Dispatcher` type name survives on purpose: it describes the internal
ownership routing, not the component's identity.
