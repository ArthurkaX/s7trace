-- 0003: a daily record of polling that outlives poll_cycle retention, and
-- dataset start/stop times.
--
-- Once poll_cycle rows older than 14 days were thinned to the cycles that
-- samples still reference, old history could no longer tell a real polling
-- outage from quiet hours with nothing changing: both looked like long gaps. The retention worker now writes one
-- row per day, PLC and poll group here before those cycles are deleted.
--
-- Also: collection-start/-stop never filled dataset.started_ts/stopped_ts.
-- They do now; existing rows are backfilled from what the database still has.

BEGIN;

-- One row per UTC day, PLC and poll group. `day_ts` is midnight UTC in
-- microseconds. Gaps are measured between consecutive finished cycles; the
-- longest one that ends inside the day is reported, counting the end of the day
-- as an end point, so a day with no cycles at all reports the whole outage up
-- to midnight rather than nothing.
CREATE TABLE poll_cycle_daily (
    day_ts INTEGER NOT NULL,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    cycles INTEGER NOT NULL,
    failed INTEGER NOT NULL,
    overruns INTEGER NOT NULL,
    first_ts INTEGER,
    last_ts INTEGER,
    max_gap_ms REAL,
    max_gap_end_ts INTEGER,
    avg_actual_ms REAL,
    max_actual_ms REAL,
    computed_ts INTEGER NOT NULL,
    PRIMARY KEY (day_ts, plc_id, poll_group_id)
);

-- A dataset is created at collection-start, so its creation time is its start.
UPDATE dataset SET started_ts = created_ts WHERE started_ts IS NULL;

-- The best surviving evidence of when a stopped dataset stopped is its last
-- sample (an index lookup on ix_sample_dataset_ts); failing that, the start.
UPDATE dataset
SET stopped_ts = COALESCE(
        (SELECT MAX(s.ts) FROM sample s WHERE s.dataset_id = dataset.id),
        started_ts)
WHERE state = 'stopped' AND stopped_ts IS NULL;

UPDATE schema_meta SET value = '3' WHERE key = 'schema_version';

COMMIT;
