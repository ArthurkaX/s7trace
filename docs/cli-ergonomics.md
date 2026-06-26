# CLI Ergonomics: simplifying metric-set configuration

Status: design note / proposal. Written 2026-06-26 after building the `tomo2_split` metric set
(88 tags) against a live server. That build took ~40 mutating CLI calls and ~8.5 min of
round-trips. Almost all of that cost is structural, not incidental — this note traces it to one
root cause and proposes fixes in priority order.

## What happened (the motivating case)

Goal: one comprehensive diagnostic set, all tags polled at a single 150 ms cadence. The set was
assembled from 16 brand-new tags plus 72 tags reused from existing (disabled) sets. Observed cost:

- New tags were cheap: `tag-add` already does create + `--set` + `--poll-group` in one call.
- **Reuse was expensive.** Each reused tag carried its own `poll_group_id`, so unifying the
  cadence meant `set-add-tag` *then* a per-tag `tag-update --poll-group`. ~70 of those.
- Offsets for the machine-side ISD telegram (`UDT_RTT_ISD01_Com` at DB1291 base 2022) had to be
  hand-derived field by field — the most error-prone step.
- Output parsing fought us: log lines and a banner on stdout, and `sql` injecting `LIMIT`.

## Root cause: poll cadence is bound to the tag, not the set

`tag.poll_group_id` lives on the tag. There is no set→group binding, so "this whole set polls at
150 ms" is not expressible as a single fact — it must be stamped onto every member tag. Every
downstream pain (the `tag-update` storm, "can't say a set's rate", regrouping requiring an id
lookup) flows from this one modeling decision.

## Proposals, by leverage

### 1. Set-level poll group (biggest win)

Make the poll group a property of the metric set, with an optional per-tag override:

```
set-create --plc plc2 --name tomo2_split --poll-group diag_split --period-ms 150
```

Membership then determines cadence. This alone removes the ~70 `tag-update` calls from the
motivating case (40 calls → ~5). If per-tag flexibility must stay, model it as "set default group,
tag may override" rather than "tag is the only source of truth".

### 2. Declarative, idempotent set-apply

One file describing the desired set; the tool diffs against live state and reconciles
(create missing tags, attach, set group, drop extras). Re-running is a no-op.

```
s7trace set-apply -f tomo2_split.yaml
```

This collapses the whole 40-command session into one reviewable, version-controllable artifact,
and is inherently restart/resume-safe. `tag-import` already ingests a YAML signal file with
`--set`, `--poll-group`, and `--validate`, but it is append-only ("create"). The extension is
set-level *reconcile* (diff + prune), not just import. Pairs with #1.

### 3. UDT / struct-aware expansion (S7-specific superpower)

The single most error-prone step was deriving ~20 offsets from a packed UDT by hand. Let the
importer expand a struct from a base offset:

```yaml
- expand_udt:
    db: 1291
    base: 2022                 # byte offset of the struct instance in the DB
    udt: UDT_RTT_ISD01_Com     # parsed from exported AWL/SCL, or an inline field list
    prefix: ISD                # tag-name prefix -> ISD.Run_State, ISD.Screening_Mode, ...
    include: [Run_State, Screening_Mode, Fault_Status, Bag_In_Process, Decision_Sent_To_BHS]
    # omit `include` to take every leaf field
```

The tool walks the UDT layout, computes each leaf's offset/type/name, and emits tags. The machine
side of `tomo2_split` would have been ~3 lines instead of 20 hand-computed addresses. This is
uniquely valuable for S7 because everything is a packed struct and offset arithmetic is exactly
what humans get wrong.

### 4. Idempotent `tag-ensure` (or make `tag-add` an upsert)

One verb that is safe to re-run: given `address` + `set` + `group`, create-if-missing,
attach-if-not-member, set group. Natural dedupe key is `(plc, address)`. Removes the
find → add → update dance on the reuse path. (Largely subsumed by #2, but useful on its own for
quick interactive edits.)

### 5. Clean machine output + stop injecting `LIMIT`

Two concrete bugs that cost scripting time:

- **Logs and banner on stdout.** The `applying migration …` log line and the
  `(remote server: …)` banner are written to stdout, breaking `| jq` / `| python -c json.load`.
  Send logs + banner to stderr; keep stdout pure payload. Add `--json` to `status` and `sql`.
- **`sql` auto-appends `LIMIT 200`.** This breaks `PRAGMA` outright and corrupts any query with
  its own `LIMIT` / trailing `;`. Make it an explicit `--limit` (default off, or wrap only a bare
  top-level `SELECT`), and never touch `PRAGMA`. Today it silently rewrites valid SQL. See also the
  `pragma_table_info('tag')` workaround currently needed to introspect columns.

### 6. `-r` consistency

See `fix.md` for the bare-IP-vs-URL bug. Beyond that: one global `--server`, bare-host normalized
once, identical flag position across all subcommands, and `read-raw` should not require/parse the
server differently from the read path.

### 7. `tag-update` selection by address

Allow `--address` as a *selector* (not only "new address"), so a tag can be re-pointed or
regrouped without first resolving its id/name.

## Priority

- **#1 + #2** deliver ~80% of the win: a declarative set whose cadence lives on the set.
- **#3** removes the main S7-specific error source (offset derivation).
- **#5** is cheap and unblocks all scripting/automation around the tool.

## Sketch: what `tomo2_split.yaml` could look like under #1–#3

```yaml
set: tomo2_split
plc: plc2
poll_group: diag_split
period_ms: 150
store_mode: on_change
description: "Tomo2 fault-split: machine ISD handshake vs BHS coupling/tracking, one timeline"

tags:
  # machine side — expanded from the ISD telegram UDT
  - expand_udt: { db: 1291, base: 2022, udt: UDT_RTT_ISD01_Com, prefix: ISD,
                  include: [Run_State, HBS_RTR, Bag_In_Process, HBS_RTS,
                            Exit_Bag_Data_Available, Decision_Sent_To_BHS,
                            Screening_Mode, Fault_Status] }
  - expand_udt: { db: 1291, base: 2066, udt: UDT_RTT_ISD01_ID, prefix: ISD,
                  include: [Bag_Decision, LIC, Barcode_Type] }

  # BHS coupling + tracking — explicit addresses (or a third expand_udt over the FB instance)
  - { address: DB1291.DBW3090, name: FHO2PLT.Rcv_Data_LE_Not_Found, type: i16 }
  - { address: DB1291.DBW3088, name: FHO2PLT.Data_Coupled_To_PLT,  type: i16 }
  - { address: DB1291.DBW3420, name: TRS5.UFO_Generated,           type: i16 }
  # ...

  # decision point
  - { address: DB1327.DBW32,  name: TDZ40.TimeOut_Lvl4, type: i16 }
  # ...
```

`s7trace set-apply -f tomo2_split.yaml` then reconciles the live set to exactly this — idempotent,
reviewable, diffable in version control.

## Related findings (2026-06-26, round 2)

- **`fix.md`** — two concrete bugs surfaced while building a set by reuse: the `-r` bare-host
  rejection on mutating commands, and `set-add-tag` not enabling reused members (no `tag-enable`
  CLI verb, though the `tag_enable` control action already exists server-side). The half-disabled
  set was silent — a strong argument for #2 (declarative apply would have caught the drift) and #5
  (clean output / visibility).
- **`server-ui.md`** — planned task for a native **Win32 control panel** (no web UI) to see and
  toggle metric sets, and to surface a stalled poll engine (which once ran ~8 h with zero poll
  cycles while still answering queries). The operate/observe counterpart to the config-as-code
  direction here.
