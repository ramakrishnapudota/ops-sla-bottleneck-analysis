-- Step 4.1 — Deduplicate raw events into a stable event grain.
-- Strategy:
-- - Define canonical timestamp: COALESCE(event_ts, ingestion_ts)
-- - Deduplicate on (case_id, status, event_ts_canonical) because retries often replay same logical event
-- - Prefer rows with non-null event_ts; then earliest ingestion_ts; then stable event_id ordering

CREATE OR REPLACE TABLE staging.events_deduped AS
WITH base AS (
  SELECT
    event_id,
    case_id,
    status,
    event_ts,
    ingestion_ts,
    COALESCE(event_ts, ingestion_ts) AS event_ts_canonical,
    event_tz,
    CAST(is_duplicate AS BOOLEAN) AS is_duplicate,
    CAST(is_late_arriving AS BOOLEAN) AS is_late_arriving,
    intake_date
  FROM raw.events_log
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY case_id, status, event_ts_canonical
      ORDER BY
        CASE WHEN event_ts IS NOT NULL THEN 0 ELSE 1 END,
        ingestion_ts ASC,
        event_id ASC
    ) AS rn
  FROM base
)
SELECT
  event_id,
  case_id,
  status,
  event_ts,
  ingestion_ts,
  event_ts_canonical,
  event_tz,
  is_duplicate,
  is_late_arriving,
  intake_date
FROM ranked
WHERE rn = 1;
