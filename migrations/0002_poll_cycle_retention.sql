-- 0002: poll_cycle retention and the indexes it needs.
--
-- Field finding (Minsk, 2026-08): poll_cycle reached 12.7M rows against 1.4M
-- samples — a 9:1 ratio where the audit log, not the collected data, dominated
-- a 2 GB database. Two things were missing: any time-range index on poll_cycle
-- (so every status query and every retention pass was a full scan), and a
-- retention policy that runs while collection is active.
--
-- Indexed on finished_ts rather than started_ts: retention, the status
-- summaries and the per-PLC "last cycle" lookups all key off finished_ts, and
-- the two columns are within one cycle of each other. A second index on
-- started_ts would cost as much space as it saves in scans.
--
-- Wrapped in a transaction by the file itself: the migration runner cannot do
-- it, because 0001 opens with PRAGMAs that SQLite rejects inside one.
-- Not done here: dropping UNIQUE(dataset_id, poll_group_id, seq). Its automatic
-- index is real overhead on a table this size, but SQLite can only remove a
-- table constraint by rebuilding the table, which on the 2 GB field database
-- means a full copy — hours of downtime and twice the disk, during a migration
-- that runs on startup. The constraint is also the only thing rejecting a
-- duplicate cycle row. Retention bounds the table instead; if the index still
-- costs too much once retention has been running, it deserves its own
-- explicitly-scheduled maintenance command rather than a startup migration.

BEGIN;

-- Whole-table time range: retention sweeps and the last-hour status summary.
CREATE INDEX IF NOT EXISTS ix_poll_cycle_finished_ts
    ON poll_cycle(finished_ts);

-- Per-(PLC, poll group) time range: the per-group cycle aggregates `status`
-- reports, and the per-group MAX(finished_ts) it derives the PLC's last cycle
-- from. Deliberately NOT a second, shorter (plc_id, finished_ts) index: on the
-- 2 GB field database each poll_cycle index costs around 250 MB, and every
-- caller that used to want the per-PLC form now either reads liveness from
-- memory or aggregates per group, which this index serves as a prefix.
CREATE INDEX IF NOT EXISTS ix_poll_cycle_plc_group_finished
    ON poll_cycle(plc_id, poll_group_id, finished_ts);

-- Retention must clear poll_cycle references before deleting the parent rows.
-- sample and event_marker already have an index on cycle_id; plc_time_sample
-- did not, which made its reference check a full scan of that table.
CREATE INDEX IF NOT EXISTS ix_plc_time_sample_cycle
    ON plc_time_sample(cycle_id);

-- Retention policy for the poll_cycle audit log.
--
-- `cycle_cutoff_days` is separate from `cutoff_days` (which bounds sample
-- history) because the two have very different value per byte: cycle rows are
-- diagnostics worth days, samples are the product worth months.
-- `keep_failed_cycles` applies the separate failed-cycle cutoff, so the record
-- of an outage outlives routine cycles without making failure history endless.
ALTER TABLE history_cleanup_policy ADD COLUMN cycle_cutoff_days INTEGER;
ALTER TABLE history_cleanup_policy ADD COLUMN failed_cycle_cutoff_days INTEGER;
ALTER TABLE history_cleanup_policy ADD COLUMN keep_failed_cycles INTEGER NOT NULL DEFAULT 1;

-- Global default policy (poll_group_id NULL = applies to every group).
-- 14 days of cycle history at the observed 20-40 rows/s is ~25-50M rows, so
-- this is a ceiling rather than a target; operators tighten it per site.
INSERT INTO history_cleanup_policy (poll_group_id, cutoff_days, cycle_cutoff_days,
                                    failed_cycle_cutoff_days, delete_batch_rows,
                                    keep_failed_cycles, enabled)
SELECT NULL, NULL, 14, 30, 10000, 1, 1
WHERE NOT EXISTS (
    SELECT 1 FROM history_cleanup_policy WHERE poll_group_id IS NULL AND enabled = 1
);

UPDATE schema_meta SET value = '2' WHERE key = 'schema_version';

COMMIT;
