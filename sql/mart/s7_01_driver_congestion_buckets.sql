-- Step 7.2 (fixed v3) — Congestion exposure buckets vs breach rates
-- Grain: decile + tier
-- Uses case-level avg congestion during intake->resolved window (cap 10 biz days).

CREATE OR REPLACE TABLE mart.driver_congestion_buckets AS
WITH base AS (
  SELECT
    s.case_id,
    s.tier,
    s.case_type,
    s.sla_b_breached,
    s.sla_a_breached_including_cw,
    s.sla_a_breached_paused_cw,
    e.avg_congestion_flow_index AS congestion_exposure
  FROM staging.case_sla_metrics s
  JOIN staging.case_congestion_exposure e USING(case_id)
),
bucketed AS (
  SELECT
    *,
    NTILE(10) OVER (ORDER BY congestion_exposure) AS congestion_decile
  FROM base
)
SELECT
  congestion_decile,
  tier,
  COUNT(*) AS cases,
  ROUND(100.0 * AVG(CASE WHEN sla_b_breached THEN 1 ELSE 0 END), 3) AS sla_b_breach_pct,
  ROUND(100.0 * AVG(CASE WHEN sla_a_breached_including_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_inc_cw,
  ROUND(100.0 * AVG(CASE WHEN sla_a_breached_paused_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_pause_cw,
  ROUND(AVG(congestion_exposure), 4) AS avg_congestion_exposure
FROM bucketed
GROUP BY 1,2
ORDER BY 1,2;
