# CLI ergonomics: declarative diagnostic sets

Status: **design note / proposal**, reviewed 2026-07-11. Commands marked as
proposals do not exist yet.

## Motivation

Building a diagnostic set from many existing tags currently requires several
independent facts to be reconciled:

- membership belongs to the metric set;
- cadence belongs to each tag through `poll_group_id`;
- a tag has its own `enabled` flag;
- `set-add-tag` changes membership but does not enable the tag;
- imported structures still require address and type expansion before they can
  become tags.

This is manageable for a small investigation but produces many round trips and
opportunities for drift in a large reused set.

## Current capabilities

- `tag-add` can create a tag and attach it to a set and poll group in one call.
- `tag-import` imports the `signals:` section of a YAML-shaped file and can
  validate every address.
- `set-add-tag` accepts a name, ID, comma-separated values, a file, or IDs from
  a `SELECT` query.
- `tag-enable` and `tag-disable` are available explicitly.
- `poll-group-set-period` changes cadence without recreating the group.
- Bare remote hosts and full URLs are normalized consistently.

## Proposal 1: declarative `set-apply`

One version-controlled file should describe the desired state of a diagnostic
set. The tool would diff it against live state, show the plan, and reconcile
missing tags, membership, enabled state, and poll-group assignment. Re-running
the same file should be a no-op.

```yaml
plc: line1
set: intermittent_fault
poll_group: diagnostic
period_ms: 150

signals:
  - name: Sequence.State
    address: DB10.DBW20
    type: i16
  - name: Handshake.Ready
    address: DB10.DBX30.0
    type: bool
```

Possible interface:

```text
s7trace -r recorder-host set-apply --file intermittent_fault.yaml --dry-run
s7trace -r recorder-host set-apply --file intermittent_fault.yaml
```

The important property is atomic intent: a complete set should not appear
healthy while half its members remain disabled or attached to an old cadence.

## Proposal 2: set-level cadence

Today cadence is stored on the tag. A set-level default poll group, with an
optional per-tag override, would make “record this investigation at 150 ms” one
fact rather than a series of updates.

This requires a schema and planner decision because one tag may belong to
multiple sets. The implementation must define whether the fastest enabled set
wins, duplicate sampling is allowed, or membership is constrained.

## Proposal 3: structure-aware expansion

The most error-prone part of S7 diagnostics is manually deriving packed UDT
offsets. A future importer could consume type information exported by
`awl-text-sync`, expand selected leaves from a DB base address, and emit a
reviewable signal list.

```yaml
- expand_udt:
    db: 10
    base: 200
    type: DiagnosticHandshake
    prefix: Handshake
    include: [Ready, Request, Acknowledge, FaultCode]
```

Address generation must remain reviewable before anything reaches a production
PLC. Validation should use normal read-only `s7trace` reads.

## Proposal 4: stable machine output (implemented)

`--json` (or `S7TRACE_JSON=1`) makes every command print one compact envelope
with the result or a stable error code, a retry hint and the queued
`command_id`; `command-list`, `command-wait` and `command-result` work through
`-r`, so a command that timed out can be followed remotely. `--format` is a
closed list (`text`, `json`, `jsonl`), and logs go to stderr. Human output
remains the default. See [`machine-output.md`](machine-output.md).

## Priority

1. Declarative `set-apply` provides the largest reduction in round trips and
   configuration drift.
2. Structure-aware expansion removes the most S7-specific source of human
   address errors.
3. Set-level cadence should follow only after its multi-set semantics are
   explicit.

See `fix.md` for resolved remote-address and `tag-enable` issues, plus the
remaining membership/enabled-state caveat. See `server-ui.md` for the current
local operator window.
