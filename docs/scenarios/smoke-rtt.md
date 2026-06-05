# Smoke test against a real S7-300 PLC

This scenario validates the s7trace end-to-end pipeline against a live
Siemens S7-300 over ISO-on-TCP, with the 5 representative signals from
`archive/rtt_exit/rtt_diagnostic.yaml`.

It covers:

1. PLC reachability (COTP / S7 connect, rack 0 / slot 2, port 102)
2. Address validity for the 5 chosen signals (sync read during `tag add`)
3. Polling pipeline (`poll_cycle`, `read_span`, `sample`)
4. DB write (WAL, timestamp sanity)
5. `plc.connectivity` flip (online after a successful cycle)

## Target

| Field     | Value                                |
|-----------|--------------------------------------|
| IP:port   | `192.168.0.50:102`                   |
| Model     | S7-300                               |
| Rack/slot | 0 / 2                                |
| Latency   | ~200 ms via Tailscale tunnel         |

## Signals under test

5 signals were picked to cover the 4 address shapes the parser supports
and the 4 `value_role` patterns the system cares about:

| Name                          | Address             | Type | Notes                  |
|-------------------------------|---------------------|------|------------------------|
| `cfg.TRS1.Conv_Speed_mm_s`    | `DB5999.DBW22`      | i16  | config INT             |
| `DB5997.Section_Run`          | `DB5997.DBX10.1`    | bool | bit-level BOOL         |
| `DB5997.Entries_Used`         | `DB5997.DBW0`       | i16  | counter (monotonic?)   |
| `TRS5.Statistical.Missing`    | `DB5999.DBW3486`    | i16  | counter statistic      |
| `RTT_ISD_ID_1.LIC`            | `DB5999.DBD2196`    | i32  | DWORD / bag id         |

All five reside in either DB5997 or DB5999 which are static data blocks
in the RTT1 program; their offsets are stable across reboots.

## How to run

```powershell
# from the s7trace project root
cargo build
.\scripts\smoke-rtt.ps1
```

The script will:
1. Create a fresh DB in `$Env:TEMP\s7trace_smoke.db`
2. Insert the PLC and 3 poll groups (fast/medium/slow)
3. Add 5 tags — each does a synchronous PLC read
4. Print the resulting `tag` table with `bad` / `last_value_*` fields
5. Start the server, wait 5 seconds, print:
   - `poll_cycle` counts per PLC
   - `sample` counts per tag
   - `read_span` counts
   - `plc.connectivity` and `last_ok_ts`
6. Stop the server and clean up

## Expected output

Step 3 should show **5 rows, all with `bad = 0`** — that's the address
validity check. The first three values should be in plausible ranges:

- `cfg.TRS1.Conv_Speed_mm_s`: typically 0..5000 (mm/s)
- `DB5997.Section_Run`: 0 or 1
- `DB5997.Entries_Used`: 0..20
- `TRS5.Statistical.Missing`: 0..(whatever running total)
- `RTT_ISD_ID_1.LIC`: 0 if no bag in handover, else a non-zero 32-bit id

Step 5 should show, after 5 seconds at 100 ms period:

- `poll_cycle.cycles` >= 40 per PLC (5 s / 100 ms - some jitter)
- `sample.samples` >= 1 per tag (depends on `on_change` storage hook —
  if a value never changes, only the initial sample is stored)
- `read_span.ok_spans` > 0
- `plc.connectivity` = `online`, `last_ok_ts` > 0

## Triage

| Symptom                                       | Likely cause |
|-----------------------------------------------|--------------|
| All `tag add` fail with `bad=1, bad_class=ConnectError` | PLC not reachable from this host; check Tailscale, firewall, PLC IP |
| `bad_class=AddressOutOfRange`                 | Offset is wrong or DB number not loaded on the PLC |
| `bad_class=TypeMismatch`                      | Data type disagrees with what the PLC actually has at that offset (e.g. user expected `i16` but it's a `REAL`) |
| `tag add` succeeds but `sample.samples = 0`   | Poll plan didn't get rebuilt; check `applied_version == desired_version` for the tags |
| `connectivity = offline` after server start   | Server can't reach the PLC even though CLI could (different routing? DNS?); check `plc.last_error` |
| `poll_cycle.cycles` low or 0                  | `apply_worker` didn't fire; verify with `SELECT * FROM config_apply` |

## Cleanup

The script removes its DB on exit. To preserve the DB for analysis:

```powershell
$env:S7TRACE_KEEP_DB = "1"  # not yet implemented
```

For now, comment out the `Remove-Item` lines in the script.
