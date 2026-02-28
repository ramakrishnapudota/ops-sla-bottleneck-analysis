-- Step 7.5 (final) — Headline driver summary
-- Congestion exposure is expected to affect SLA B (first touch), not necessarily SLA A (first resolution).

CREATE OR REPLACE TABLE mart.driver_summary AS
WITH base AS (
  SELECT
    s.case_id,
    s.sla_b_breached,
    s.sla_a_breached_including_cw,
    e.avg_congestion_flow_index AS congestion_exposure
  FROM staging.case_sla_metrics s
  JOIN staging.case_congestion_exposure e USING(case_id)
  WHERE e.avg_congestion_flow_index IS NOT NULL
),
bucketed AS (
  SELECT
    *,
    NTILE(10) OVER (ORDER BY congestion_exposure) AS congestion_decile
  FROM base
),
decile_rates AS (
  SELECT
    congestion_decile,
    COUNT(*) AS cases,
    ROUND(100.0 * AVG(CASE WHEN sla_b_breached THEN 1 ELSE 0 END), 3) AS sla_b_breach_pct,
    ROUND(100.0 * AVG(CASE WHEN sla_a_breached_including_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_inc_cw
  FROM bucketed
  GROUP BY 1
),
pick AS (
  SELECT
    (SELECT sla_b_breach_pct FROM decile_rates WHERE congestion_decile = 5) AS sla_b_breach_p50_cong,
    (SELECT sla_b_breach_pct FROM decile_rates WHERE congestion_decile = 9) AS sla_b_breach_p90_cong,
    (SELECT sla_b_breach_pct FROM decile_rates WHERE congestion_decile = 10) AS sla_b_breach_p95plus_cong,

    (SELECT sla_a_breach_pct_inc_cw FROM decile_rates WHERE congestion_decile = 5) AS sla_a_breach_p50_cong,
    (SELECT sla_a_breach_pct_inc_cw FROM decile_rates WHERE congestion_decile = 9) AS sla_a_breach_p90_cong,
    (SELECT sla_a_breach_pct_inc_cw FROM decile_rates WHERE congestion_decile = 10) AS sla_a_breach_p95plus_cong
)
SELECT
  sla_b_breach_p50_cong,
  sla_b_breach_p90_cong,
  sla_b_breach_p95plus_cong,
  ROUND(sla_b_breach_p90_cong - sla_b_breach_p50_cong, 3) AS sla_b_breach_lift_p90_vs_p50,
  ROUND(sla_b_breach_p95plus_cong - sla_b_breach_p50_cong, 3) AS sla_b_breach_lift_p95_vs_p50,

  sla_a_breach_p50_cong,
  sla_a_breach_p90_cong,
  sla_a_breach_p95plus_cong,
  ROUND(sla_a_breach_p90_cong - sla_a_breach_p50_cong, 3) AS sla_a_breach_lift_p90_vs_p50,
  ROUND(sla_a_breach_p95plus_cong - sla_a_breach_p50_cong, 3) AS sla_a_breach_lift_p95_vs_p50
FROM pick;
