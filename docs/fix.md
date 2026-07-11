# Resolved issues and remaining caveats

This file records field issues that influenced the current interface. It is not
a list of unimplemented fixes.

## Resolved: bare hosts with `-r`

Earlier action commands required a full `http://host:port` URL even though read
commands accepted a bare host. Remote address handling is now centralized in
`cli::remote::resolve_url`, with regression tests for bare hosts, explicit
ports, full URLs, and custom default ports.

All of these forms are supported:

```text
s7trace -r recorder-host status
s7trace -r recorder-host:7708 status
s7trace -r http://recorder-host:7707 status
```

The default is port `7707`, overridable with `S7TRACE_PORT` or
`--server-port`.

## Resolved: missing `tag-enable`

The CLI now exposes `tag-enable` with the same selectors as `tag-disable`:

```text
s7trace -r recorder-host tag-enable --id 196
s7trace -r recorder-host tag-enable --plc line1 --name Motor.Speed
```

Both commands route through the typed operation core and the server's durable
command queue.

## Remaining caveat: set membership does not enable a tag

`set-add-tag` changes membership only. It intentionally leaves the tag's own
`enabled` flag unchanged. A disabled tag added to an enabled metric set is still
not polled, so `tags_active` may be lower than `tags_total`.

Check `set-show`, `status`, or the Windows control window after reusing tags from
an old disabled set, then enable the required tags explicitly. A future
declarative set-reconciliation command could make the desired membership,
enabled state, and poll group atomic; see `cli-ergonomics.md`.
