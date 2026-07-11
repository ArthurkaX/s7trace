# s7trace concept

This document defines the load-bearing ideas behind `s7trace`. For commands and
the field workflow, start with the project README. For implementation details,
see `architecture.md`.

## Problem

Intermittent PLC faults usually disappear before an engineer can observe them.
The useful evidence is the ordered history of a deliberately selected set of
signals, collected continuously and safely on production equipment.

In one line, `s7trace` is a **read-only scope and black box for Siemens
S7-300/400, operated through a remote-friendly CLI and backed by SQLite**.

## Three components and one invariant

1. The **server** owns every PLC connection, polling plan, worker, and write to
   the live database.
2. **SQLite** stores configuration, samples, poll cycles, events, datasets, and
   the durable command queue.
3. The **CLI** is the interface for engineers, scripts, and LLM agents, locally
   or from another city.

The invariant is that one server process owns PLC communication and live
database writes. A remote CLI never opens a second PLC connection. Offline
`sql` may read a copied or stopped database; `admin-sql` is explicit local
maintenance and must not be run against a database owned by a running server.

## On-site collection, remote investigation

The server is meant to remain in the controls network near the PLC. Remote CLI
clients reach it through a VPN or another protected private network:

```text
S7-300/400 ← on-site server ← protected network → CLI / LLM agent
                    ↓
                 SQLite
```

`-r HOST` or `S7TRACE_SERVER` selects the server. Port `7707` is the default.
The HTTP API is an internal CLI transport, not the primary operator interface;
operators should use `s7trace`, not hand-written `curl` requests. Optional
bearer authentication is enabled by setting `S7TRACE_TOKEN` on both sides.

## Questions and actions

The interface separates operations by their nature:

- **Questions** use bounded read-only SQL or status endpoints. They are safe to
  repeat and suitable for incremental LLM analysis.
- **Actions** use typed commands. The server serializes them through a durable,
  lease-based queue, executes them, and stores their result.

Remote action commands currently print the queued command ID and then wait for
the final result. Queue inspection commands remain available for diagnostics
and recovery.

This split allows an agent to explore recorded evidence without gaining a raw
write channel to the database or PLC.

## Focused recordings

Tags belong to poll groups, which define cadence, and may belong to metric sets,
which define a diagnostic scope. A dataset identifies a recording session.
Configuration changes are applied live without restarting the server.

The intended workflow is hypothesis-driven: choose the smallest set of signals
that can distinguish likely causes, record until the event occurs, and analyse
the resulting timeline. Recording every available address increases PLC load
and usually makes the evidence harder to interpret.

## Failure containment

- Bad addresses are recorded as bad tags and kept visible instead of silently
  poisoning the polling plan.
- One connection-owning actor per PLC serializes reads and gives interactive or
  faster requests priority over slower polling groups.
- Multi-read batches obey negotiated PDU limits and adapt their item cap when a
  PLC truncates responses.
- Pollers use reconnect backoff and skip missed ticks by default instead of
  building an unbounded backlog.
- Queue leases allow abandoned commands to be reclaimed after a server crash.
- History deletion is explicit and bounded.
- The native Windows window displays stale poll-cycle age even when the server
  remains otherwise responsive.

## LLM boundary

LLMs are useful for reading exported STEP 7 source, selecting signals,
constructing SQL, comparing transitions, and calculating statistics. They do
not replace engineering judgement and do not receive PLC write access.

The preferred companion workflow is:

```text
STEP 7 → awl-text-sync → reviewed tag set → s7trace → SQLite → SQL/LLM analysis
```

Large raw time-series should not be pasted into a model context. Let the agent
inspect the schema and issue narrow aggregate or event-window queries instead.

## Scope boundaries

Core scope includes read-only PLC access, live polling configuration, metric
sets, datasets, event notes, SQLite history, remote CLI operation, command queue
recovery, and status diagnostics.

The native Windows window is intentionally small: it controls sets and polling
periods and reports freshness, but does not chart history or edit tag addresses.
PLC writes, SCADA functions, alarm management, and web visualization are out of
scope today. A possible future analysis layer is a CLI-requested chart for a
selected time window and tag set, rendered in a local web view; it must remain
separate from the collection path and must not turn the server into a SCADA UI.
