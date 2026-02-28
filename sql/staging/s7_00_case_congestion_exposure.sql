-- Step 7 Fix v3 — Case-level congestion exposure
-- Compute avg congestion_flow_index across business days from intake_date to resolved_date (cap to 10 biz days)
-- This aligns driver signal to the actual window that determines SLA A.

CREATE OR REPLACE TABLE staging.case_congestion_exposure AS
WITH resolved_cases AS (
  SELECT
    case_id,
    tier,
    case_type,
    intake_date,
    CAST(resolved_ts AS DATE) AS resolved_date
  FROM staging.case_sla_metrics
  WHERE resolved_ts IS NOT NULL
),
biz_days AS (
  SELECT cal_date
  FROM raw.calendar_dim
  WHERE NOT is_weekend
    AND NOT is_holiday
),
expanded AS (
  SELECT
    r.case_id,
    r.tier,
    r.case_type,
    r.intake_date,
    r.resolved_date,
    d.cal_date,
    ROW_NUMBER() OVER (PARTITION BY r.case_id ORDER BY d.cal_date) AS biz_day_n
  FROM resolved_cases r
  JOIN biz_days d
    ON d.cal_date BETWEEN r.intake_date AND r.resolved_date
),
windowed AS (
  SELECT *
  FROM expanded
  WHERE biz_day_n <= 10
),
joined AS (
  SELECT
    w.case_id,
    w.tier,
    w.case_type,
    w.intake_date,
    w.resolved_date,
    w.biz_day_n,
    c.congestion_flow_index
  FROM windowed w
  LEFT JOIN mart.congestion_daily_v2 c
    ON c.cal_date = w.cal_date
)
SELECT
  case_id,
  tier,
  case_type,
  intake_date,
  resolved_date,
  COUNT(*) AS biz_days_in_window,
  ROUND(AVG(congestion_flow_index), 4) AS avg_congestion_flow_index
FROM joined
WHERE congestion_flow_index IS NOT NULL
GROUP BY 1,2,3,4,5;
