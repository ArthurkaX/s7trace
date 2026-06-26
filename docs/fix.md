# Fixes / Known Issues

## `-r` bare-host normalization is inconsistent across subcommands

**Observed 2026-06-26** while configuring `plc2` against server `192.0.2.56`.

### Symptom

Read-path subcommands accept a **bare IP/host** for `-r`:

```
s7trace -r 192.0.2.56 status          # OK
s7trace -r 192.0.2.56 sql "..."       # OK
```

…but mutating subcommands **reject the same bare host** and demand a full URL:

```
s7trace -r 192.0.2.56 set-disable example_set
  -> Error: URL must start with http:// or https://: 192.0.2.56

s7trace -r http://192.0.2.56:7707 set-disable example_set
  -> OK (queued command_id=...)
```

Confirmed affected (need full `http://host:port`): `set-disable` (and by extension the other
mutating verbs that route through the same client builder — `set-enable`, `tag-add`,
`tag-update`, `set-add-tag`, `set-create`, `poll-group-add`).

The `--help` text for `-r` advertises that a bare IP is fine ("Accepts a bare IP/host (e.g.
192.168.0.50) or a full URL"), so the mutating path contradicts the documented contract.

### Likely cause

Two different code paths turn `-r` into a base URL. The read/query client path applies the
bare-host → `http://host:<S7TRACE_PORT>` defaulting; the action/command-queue client path parses
`-r` straight into a `Url` (or equivalent) **without** the same prefix/port normalization, so a
bare host fails URL parsing.

### Fix

Normalize `-r` once, centrally, before either client is built: if the value has no scheme,
prepend `http://` and append `:` + the effective port (default 7707 / `S7TRACE_PORT` /
`--server-port`). Both read and mutating paths should consume the already-normalized base URL so
behavior matches the `--help` contract. Add a regression test covering bare-host input to a
mutating command.

### Workaround (until fixed)

For any mutating command, pass the full URL:

```
-r http://192.0.2.56:7707
```

## No `tag-enable` CLI verb; `set-add-tag` does not enable members

**Observed 2026-06-26** while assembling the `tomo2_split` set (88 tags) by reusing tags that
already existed in a *disabled* set.

### Symptom

`set-add-tag` adds set membership but leaves the tag's own `enabled` flag untouched. Tags that
were `enabled = 0` (because they came from a disabled set) stay `enabled = 0` after being added to
a now-enabled set, so they are **not polled**. In this case 44 of 88 tags silently sat disabled —
including critical signals (`Run_State`, `HBS_RTS`, `HBS_RTR`, the FHO2PLT monitor block). The set
looked complete in `set-show`, but only half of it collected.

There is no CLI verb to re-enable them:

- No `tag-enable` subcommand exists (the CLI exposes `tag-add`, `tag-update`, `tag-disable`,
  `tag-revalidate`).
- `tag-update` revalidates but does **not** flip `enabled` (confirmed: `tag-update --id 196` left
  `enabled = 0`).
- `admin-sql` is local-DB only and does not route to the server over `-r`, so a bulk
  `UPDATE tag SET enabled = 1 ...` against the live server is not reachable from a client.

### Good news: the capability already exists server-side

`engine/server.rs` already implements a `tag_enable` control action (around line 988, mirroring
`tag_disable` at line 958). It is simply **not surfaced as a CLI subcommand**. So the fix is small.

### Fix

1. Add a `tag-enable` CLI subcommand that submits the existing `tag_enable` control action — a
   direct mirror of `tag-disable`. Support the same selectors (`--id`, or `--plc` + `--name`) and
   ideally a bulk form (`--from-sql` / comma-separated) to undo a whole reused batch in one call.
2. Decide `set-add-tag` semantics: either enable members on add (probably the least-surprising
   default for "add these to a set I intend to poll"), or at minimum **warn** when adding tags that
   are currently `enabled = 0` so the half-disabled set is not silent.

### Workaround (until fixed)

Re-`tag-add` the affected addresses (tag-add defaults `enabled = 1` and takes `--set`), or submit
the `tag_enable` control action directly via `POST /v1/command` (transport-level, not a supported
operator path).
