-- Step 7.3 — Reopen impact on cycle time (business hours)
-- Grain: tier + reopened_flag

CREATE OR REPLACE TABLE mart.driver_reopen_impact AS
WITH reopened AS (
  SELECT
    case_id,
    tier,
    case_type,
    CASE WHEN reopened_first_ts IS NOT NULL THEN TRUE ELSE FALSE END AS is_reopened
  FROM staging.case_milestones
),
joined AS (
  SELECT
    r.tier,
    r.case_type,
    r.is_reopened,
    s.first_resolution_business_hours_including_cw AS fr_hours_inc_cw,
    s.first_resolution_business_hours_paused_cw AS fr_hours_pause_cw
  FROM reopened r
  JOIN staging.case_sla_metrics s USING(case_id)
  WHERE s.resolved_ts IS NOT NULL
)
SELECT
  tier,
  case_type,
  is_reopened,
  COUNT(*) AS cases,
  ROUND(AVG(fr_hours_inc_cw), 3) AS avg_fr_hours_inc_cw,
  ROUND(AVG(fr_hours_pause_cw), 3) AS avg_fr_hours_pause_cw,
  quantile_cont(fr_hours_inc_cw, 0.90) AS p90_fr_hours_inc_cw,
  quantile_cont(fr_hours_inc_cw, 0.95) AS p95_fr_hours_inc_cw
FROM joined
GROUP BY 1,2,3
ORDER BY 1,2,3;
