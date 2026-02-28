-- RAW QA 01 — Event integrity (duplicates, late-arriving, missing timestamps, tz issues)

-- Basic messy flags (should align ~ to config rates, not exact)
SELECT
  ROUND(100.0 * AVG(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END), 3) AS pct_missing_event_ts,
  ROUND(100.0 * AVG(CASE WHEN is_duplicate THEN 1 ELSE 0 END), 3) AS pct_duplicate_flag,
  ROUND(100.0 * AVG(CASE WHEN is_late_arriving THEN 1 ELSE 0 END), 3) AS pct_late_arriving_flag,
  ROUND(100.0 * AVG(CASE WHEN event_tz = 'INCONSISTENT' THEN 1 ELSE 0 END), 3) AS pct_tz_inconsistent
FROM raw.events_log;

-- Duplicate candidates by (case_id, status, event_ts) ignoring event_id
-- This catches "retry logging" patterns beyond explicit flags.
SELECT
  COUNT(*) AS duplicate_groups,
  SUM(cnt - 1) AS duplicate_extra_rows
FROM (
  SELECT case_id, status, event_ts, COUNT(*) AS cnt
  FROM raw.events_log
  WHERE event_ts IS NOT NULL
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
);

-- Ingestion after event sanity (should almost always be >=, but missing event_ts allowed)
SELECT
  ROUND(100.0 * AVG(CASE WHEN event_ts IS NOT NULL AND ingestion_ts < event_ts THEN 1 ELSE 0 END), 3) AS pct_ingestion_before_event_ts
FROM raw.events_log;
