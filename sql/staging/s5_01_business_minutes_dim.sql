-- Step 5.1 — Business-minute dimension (Mon–Fri 08:00–18:00, excluding holidays)
-- DuckDB note: generate_series() can't take per-row parameters in this context.
-- Approach: create minute offsets (0..599) and add to each eligible business day.
-- Scale: ~180 days * 600 minutes/day ≈ 108k rows.

CREATE OR REPLACE TABLE staging.business_minutes_dim AS
WITH biz_days AS (
  SELECT
    cal_date::DATE AS cal_date
  FROM raw.calendar_dim
  WHERE NOT is_weekend
    AND NOT is_holiday
),
minute_offsets AS (
  SELECT
    gs AS minute_offset
  FROM generate_series(0, 599, 1) AS t(gs)
),
minutes AS (
  SELECT
    d.cal_date,
    (d.cal_date::TIMESTAMP + INTERVAL '8 hours' + (o.minute_offset * INTERVAL '1 minute')) AS minute_ts
  FROM biz_days d
  CROSS JOIN minute_offsets o
)
SELECT
  minute_ts,
  cal_date,
  ROW_NUMBER() OVER (ORDER BY minute_ts) - 1 AS minute_idx
FROM minutes
ORDER BY minute_ts;
