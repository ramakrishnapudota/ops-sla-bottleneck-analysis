-- Step 4.2 — Canonicalize + add quality flags for downstream logic.
-- Notes:
-- - Keep raw timestamps for audit, but standardize analysis on event_ts_canonical.
-- - Add anomaly flags (ingestion < event_ts, tz inconsistent marker).

CREATE OR REPLACE TABLE staging.events_clean AS
SELECT
  event_id,
  case_id,
  status,

  -- raw timestamps (audit)
  event_ts,
  ingestion_ts,

  -- canonical timestamp (analysis)
  event_ts_canonical,

  -- timezone fields
  event_tz,
  CASE WHEN event_tz = 'INCONSISTENT' THEN TRUE ELSE FALSE END AS is_tz_inconsistent,

  -- raw flags
  is_duplicate,
  is_late_arriving,

  -- anomaly flags
  CASE
    WHEN event_ts IS NOT NULL AND ingestion_ts < event_ts THEN TRUE
    ELSE FALSE
  END AS is_ingestion_before_event_ts,

  CASE
    WHEN event_ts IS NULL THEN TRUE ELSE FALSE
  END AS is_event_ts_missing,

  intake_date

FROM staging.events_deduped;
