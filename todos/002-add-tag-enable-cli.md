# Task 002: add `tag-enable` CLI command

## Goal

Expose the existing server-side `tag_enable` control action through the CLI.

The command must mirror `tag-disable`.

## Files

- `src/cli/args.rs`
- `src/main.rs`
- `src/engine/server.rs`

## Required CLI

Add:

```powershell
s7trace tag-enable --id 123
s7trace tag-enable --plc plc2 --name Some.Tag.Name
s7trace -r 127.0.0.1 tag-enable --id 123
s7trace -r 127.0.0.1 tag-enable --plc plc2 --name Some.Tag.Name
```

Use the same selector rules as `tag-disable`:

- either `--id`
- or `--plc` plus `--name`

Do not add bulk enable in this task.

## Required implementation

1. Add a `TagEnable` command variant next to `TagDisable`.
2. Add `handle_tag_enable(...)` next to `handle_tag_disable(...)`.
3. Local mode must update `tag.enabled = 1`.
4. Remote mode must enqueue the existing control action:

```json
{"action":"tag_enable", "...selector fields..."}
```

5. Print a concise success message matching the style of `tag-disable`.

## Tests

Add unit tests mirroring existing `tag-disable` tests:

- enabling an existing disabled tag by id sets `enabled = 1`
- unknown id returns an error
- selector validation matches `tag-disable`

## Acceptance

- `cargo test` passes.
- `s7trace tag-enable --help` is present.
- A disabled tag can be re-enabled without direct SQL.
