# Task 001: normalize `-r` bare host once for every remote command

## Goal

Make `s7trace -r HOST ...` behave the same for read commands and mutating commands.

Example that must work after this task:

```powershell
s7trace -r 192.0.2.56 set-disable example_set
```

It must resolve to:

```text
http://192.0.2.56:7707
```

unless `--server-port` provides another port.

## Files

- `src/main.rs`
- `src/cli/remote.rs`
- `src/cli/args.rs`

## Required implementation

1. Keep one central remote URL normalization function.
2. If `-r` has no `http://` or `https://` scheme, prepend `http://`.
3. If the resulting host has no explicit port, append the effective server port.
4. Use the normalized URL before any read or mutating remote client is called.
5. Do not make individual subcommands parse `-r` themselves.

## Tests

Add or update unit tests for:

- bare IP with default port: `192.0.2.56` -> `http://192.0.2.56:7707`
- bare host with custom port: `plc-host --server-port 7708` -> `http://plc-host:7708`
- full URL with port remains unchanged
- mutating command path accepts a bare host

## Acceptance

- `cargo test` passes.
- `s7trace -r 127.0.0.1 status` still works.
- `s7trace -r 127.0.0.1 set-disable SOME_SET` no longer fails with `URL must start with http:// or https://`.
