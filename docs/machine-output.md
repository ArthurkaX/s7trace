# Machine-readable CLI output

Scripts and LLM agents should run every command with `--json` (or set
`S7TRACE_JSON=1` once). Each invocation then prints exactly one compact JSON
line on stdout. Logs, notes and warnings go to stderr.

```sh
export S7TRACE_JSON=1
s7trace -r s7trace-site.example tag-add --plc line1 --name motor.speed \
  --address DB5.DBW120 --type u16 --poll-group fast
```

## Envelope

Success:

```json
{"ok":true,"result":{"message":"apply_state=pending tag_id=7","data":{"tag_id":7,"value":"1450","apply_state":"pending","set":"default"}},"command_id":42}
```

Failure:

```json
{"ok":false,"error":{"code":"timeout","message":"command 42 still running after 30000 ms","retryable":true,"state":"running"},"command_id":42}
```

| Field | Meaning |
|---|---|
| `ok` | `true` on success, `false` on failure. Always present. |
| `result` | The command's result (success only). Its shape depends on the command; see below. |
| `error.code` | A stable error class (table below). Match on it, not on `message`. |
| `error.message` | Human-readable detail. Wording may change. |
| `error.retryable` | Whether the same request may succeed later without being changed. |
| `error.state` | Last known state of the queued command (`pending`, `running`, `done`, `bad`, `cancelled`), when there is one. |
| `error.details` | Structured detail, when there is any (for example `tag-import` per-signal failures or the `bad_class` of a failed validation). |
| `command_id` | The server-side queue id, for commands that went through the queue. Present on failures too, including timeouts and lost connections after the command was queued. |

`--json` takes precedence over a command's `--format`. Without `--json`,
`--format json` and `--format jsonl` keep their own documented shapes, but a
failure is still reported as the envelope above on stdout. `read`, `read-raw`,
`collection-start` and `collection-stop` always print the envelope.

A usage error (unknown flag, bad `--format` value) under `--json` is an
`invalid_argument` envelope with exit code 1. `--help` and `--version` print
text as usual.

## Error codes

| Code | Retryable | Exit | Meaning |
|---|---|---|---|
| `invalid_argument` | no | 1 | A flag or value in the request is wrong. |
| `invalid_address` | no | 1 | The S7 address does not parse. |
| `server_required` | no | 1 | The command needs `-r HOST` or `S7TRACE_SERVER`. |
| `not_found` | no | 1 | A named PLC, tag, set, poll group, dataset or command does not exist. |
| `conflict` | no | 1 | The target exists but its state forbids the change (already exists, dataset active, command no longer pending). |
| `unauthorized` | no | 1 | The server rejected `S7TRACE_TOKEN`. |
| `sql_error` | no | 1 | The SQL query is invalid or failed. |
| `query_timeout` | no | 1 | The SQL query ran past its deadline; narrow it or raise `--timeout-ms`. |
| `plc_error` | yes | 2 | Talking to the PLC failed (unreachable, read error). |
| `command_failed` | no | 2 | The operation ran and failed for another reason. |
| `cancelled` | no | 2 | The command was cancelled before it ran. |
| `server_unavailable` | yes | 3 | The server could not be reached. |
| `timeout` | yes | 3 | The command was queued but did not finish in time. It may still run. |
| `pending` | yes | 3 | `command-result`: the command has not finished yet. |
| `server_error` | no | 4 | The server answered with an internal error or an unreadable response. |
| `internal` | no | 4 | An unexpected failure inside s7trace. |

Exit codes: `0` success, `1` change the request, `2` the operation failed,
`3` wait or retry later, `4` internal error.

## Following a queued command

Mutating commands and PLC reads are queued on the server and the CLI waits up
to `--remote-timeout-ms` (default 30000) for them. A `timeout` does **not**
mean the command failed: it may still run. Do not resubmit it. Follow it by
id instead, through the same `-r`:

```sh
s7trace -r HOST --json command-wait 42 --timeout-ms 60000   # block until done
s7trace -r HOST --json command-result 42                    # one look; `pending` if not done
s7trace -r HOST --json command-list --state running
s7trace -r HOST --json command-cancel 42                    # pending commands only
```

`command-wait` and `command-result` return the same `result` the original
command would have returned, or its failure with the original `error.code`.
The same applies after `server_unavailable` with a `command_id`: the command
was queued before the connection was lost.

A `plc_error` from `tag-add` means the tag was created but could not be
validated against the PLC; `error.details.tag_id` names it. Retry the
validation with `tag-revalidate`, not with another `tag-add` (which would be a
`conflict`).

## Result shapes

| Command | `result` |
|---|---|
| `tag-*`, `set-create/-add-tag/-remove-tag/-enable/-disable/-delete`, `plc-add`, `poll-group-*`, remote `history-policy` | `{"message": "...", "data": {...} \| null}` |
| `read`, `read-raw`, `collection-start/-stop` | The server's read or control result object. |
| `sql` | `{"cols": [...], "rows": [[...]], "truncated": bool, "limit": N, "requested_limit": M}`. `truncated` means the row cap `limit` cut the result short. A server caps `limit` at 10000; when `limit < requested_limit`, raising `--limit` will not help: page the query, or use `sample-export`. |
| `sample-export` | Requires `--out FILE` under `--json`: the rows go to the file, stdout gets `{"rows", "tags": [{"plc", "tag", "tag_id", "rows"}], "since_ts", "until_ts", "out"}`. A failure after some rows were written has `error.details` `{"rows", "last_ts"}`: everything up to `last_ts` is in the file. |
| `status` | The status object (`plcs`, `tags`, `command_queue`, ...). |
| `set-list` | `[{"plc", "name", "enabled", "tags_total", "tags_healthy", "tags_bad"}]` |
| `set-show` | `{"id", "plc_id", "plc", "name", "enabled", "description", "members": [...]}` |
| `command-list` | `[{"id", "created_ts", "finished_ts", "state", "kind", "error"}]` |
| `command-wait`, `command-result` | The finished command's result. |
| `command-cancel` | `{"cancelled": true}` |
| `history-clean`, `history-delete-dataset` | `{"state": "pending"}`, with `command_id` to follow. |
| `history-policy` (local) | `{"enabled", "sample_days", "cycle_days", "failed_cycle_days", "keep_failed", "batch_rows"}` |
| `history-vacuum` | `{"vacuumed": true}` |
| `event add`, `note` | `{"id", "plc_id", "kind", "ts"}` |
| `tag-import` | `{"imported", "failed": [{"name", "reason"}], "set", "validated": [...]}` |
| `admin-sql` | `{"rows_affected": N}` |
