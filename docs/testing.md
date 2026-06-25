# Testing Instructions

This document defines how to set up and exercise s7trace in tests, manual
sessions, and diagnostics. **Read this before touching a test database.**

## The rule

All creation and mutation goes through the **CLI**. The CLI validates input and
routes mutations through the `command_request` queue — the same path the control
window and LLM tools use. Writing rows directly with `admin-sql` (or any raw
`INSERT`/`UPDATE`) bypasses validation, produces states the server would never
create itself, and tests nothing real.

> A model (or a human) editing the database directly is the single most common
> way to produce a "working" test that proves nothing. Don't do it.

`admin-sql` is a maintenance tool, **not** a test tool. It must not appear in
the test workflow. The only allowed database access is read-only timeseries and
state queries through the `sql` command.

```text
LLM / human operator
         |
         v
   s7trace CLI
         |
         +-- mutations ------> command_request queue --> server --> PLC
         |
         +-- read-only -------> sql / status -----------> timeseries & state
```

## Entity-to-command reference

Every entity has a CLI command. There is no exception.

| Entity                  | CLI command                                               | Notes                                       |
| ----------------------- | --------------------------------------------------------- | ------------------------------------------- |
| `plc`                   | `plc-add --name <N> --host <H:PORT> --rack <R> --slot <S>` |                                             |
| `poll_group`            | `poll-group-add --name <N> --period-ms <MS>`               |                                             |
| tag                     | `tag-add --plc <N> --name <N> --address <A> --type <T> --poll-group <G> [--set <S>]` | Single tag                          |
| bulk tag import         | `tag-import --plc <N> --file <YAML> --set <S> --poll-group <G> [--validate]` | Parses `signals:` from yaml, creates all tags, auto-creates set |
| tag revalidate          | `tag-revalidate --id <ID>`                                  | Re-reads a tag from PLC, updates good/bad   |
| metric set              | `set-create --plc <N> --name <N>`                          | Auto-created by `tag-import` if `--set` given |
| list sets               | `set-list [--plc <N>]`                                     | Lists metric sets for a PLC                 |
| show set                | `set-show <SET>` (or `--set <SET>`)                        | Shows one set's members                     |
| set membership          | `set-add-tag --set <S> --tag <TAGS>`                       | Comma-separated tag names                   |
| remove from set         | `set-remove-tag --set <S> --tag <TAGS>`                    | Removes a tag from a metric set             |
| enable a set            | `set-enable <SET>`                                         |                                             |
| start collection        | `collection-start --plc <N> --dataset <D> --set <S>`       |                                             |
| stop collection         | `collection-stop --plc <N>`                                 |                                             |
| vacuum history          | `history-vacuum [--force]`                                 | Compacts/prunes historical data             |
| **observe state**       | `status --format text`                                     | Read-only                                   |
| **query timeseries**    | `sql "SELECT ..."`                                         | Read-only; never `admin-sql`                |

## Address -> type convention

`rtt_diagnostic.yaml` lists signals as `name`/`address` pairs with no explicit
`type:`. `tag-import` infers the type from the address width using the table
below.

| Address form     | Inferred type | Notes                                     |
| ---------------- | ------------- | ----------------------------------------- |
| `DBxxxx.DBX a.b` | `bool`        | single bit                                |
| `DBxxxx.DBW n`   | `i16`         | signed word; use `u16` for counters        |
| `DBxxxx.DBD n`   | `u32`         | dword; use `f32`/`i32` where the block defines it |

When calling `tag-add` directly (one-off tags), pass `--type` explicitly.
`tag-import` applies the same inference automatically.

### Polling performance & realistic poll rates

Each S7 read span is one network round-trip. With 110 tags split into ~3 spans over
a high-latency link, a full cycle takes ~1.7 s regardless of the configured period,
so poll overrun is EXPECTED on remote PLCs. The `on_change` storage hook reduces
how much is STORED, not how long the READ takes. A 40 ms period is only realistic
with very few tags or a local low-latency network.

Consolidate tags into fewer/larger spans and choose a realistic period for remote links.

## Canonical recipe (using `temp/rtt_diagnostic.yaml`)

The yaml's `plc` block gives the real on-site target
(`192.168.0.50:102`, rack 0, slot 2, 40 ms). Use those values directly.

```bash
DB=/tmp/s7trace_rtt.db
BIN=./target/debug/s7trace.exe

# 0. Build, then initialise the schema (any command migrates the DB;
#    --dry-run also validates config without starting pollers).
cargo build
"$BIN" --db "$DB" server --dry-run

# 1. Create the PLC and poll group (CLI commands, no raw SQL).
"$BIN" --db "$DB" --local-db plc-add --name rtt --host 192.168.0.50:102 --rack 0 --slot 2
"$BIN" --db "$DB" --local-db poll-group-add --name fast --period-ms 40

# 2. Start the server.
"$BIN" --db "$DB" server --control-window &     # control window optional

# 3. Import all tags from the yaml in one command.
#    tag-import parses the signals: list, infers types from address widths,
#    creates all tags through the queue, and auto-creates the metric set.
#    Add --validate to immediately read each tag from the PLC.
"$BIN" --db "$DB" --local-db tag-import --plc rtt --file temp/rtt_diagnostic.yaml \
  --set rtt_diag --poll-group fast --validate

# 4. The set was already created by tag-import. If you need to add more tags
#    or create a set manually later:
#    "$BIN" --db "$DB" --local-db set-create  --plc rtt --name extra_set
#    "$BIN" --db "$DB" --local-db set-add-tag --set extra_set --tag DB5997.Entries_Used
#    "$BIN" --db "$DB" --local-db set-enable  extra_set

# 5. Start collection into a named dataset.
"$BIN" --db "$DB" --local-db collection-start --plc rtt --dataset rtt_test --set rtt_diag

# 6. Observe -- read-only only.
"$BIN" --db "$DB" --local-db status --format text
"$BIN" --db "$DB" --local-db sql "SELECT id, name, address, bad, COALESCE(bad_class,'-') AS bc
                       FROM tag ORDER BY id;"
"$BIN" --db "$DB" --local-db sql "SELECT state, COUNT(*) FROM command_request GROUP BY state;"

# 7. Tear down.
"$BIN" --db "$DB" --local-db collection-stop --plc rtt
taskkill //F //IM s7trace.exe
```

## Checklist before claiming a test passed

- [ ] Every entity was created via a CLI command — no `admin-sql`, no raw SQL writes.
- [ ] Tags were added with `tag-import` or `tag-add` (validated addresses/types).
- [ ] If `tag-import --validate` was used, each tag was confirmed good/bad against the real PLC.
- [ ] Results were observed with `sql`/`status`, never by reading raw tables or using `admin-sql`.
- [ ] Tags read cleanly against the real PLC (`bad=0`), or their `bad_class` is the expected one.
- [ ] Mutating commands show a completed entry in `command_request` / `command_result`.
