-- Mart 6.2 — SLA metrics by tier and case type
-- Grain: (tier, case_type)

CREATE OR REPLACE TABLE mart.sla_by_tier_case_type AS
SELECT
  tier,
  case_type,
  COUNT(*) AS cases,

  -- SLA B (triage exists)
  ROUND(100.0 * AVG(CASE WHEN first_touch_business_minutes IS NOT NULL AND sla_b_breached THEN 1 ELSE 0 END), 3) AS sla_b_breach_pct,
  quantile_cont(first_touch_business_minutes, 0.50) AS ft_p50_min,
  quantile_cont(first_touch_business_minutes, 0.90) AS ft_p90_min,
  quantile_cont(first_touch_business_minutes, 0.95) AS ft_p95_min,

  -- SLA A (resolved)
  ROUND(100.0 * AVG(CASE WHEN first_resolution_business_minutes_including_cw IS NOT NULL AND sla_a_breached_including_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_including_cw,
  ROUND(100.0 * AVG(CASE WHEN first_resolution_business_minutes_paused_cw IS NOT NULL AND sla_a_breached_paused_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_paused_cw,

  quantile_cont(first_resolution_business_minutes_including_cw, 0.50) AS fr_p50_min_inc_cw,
  quantile_cont(first_resolution_business_minutes_including_cw, 0.90) AS fr_p90_min_inc_cw,
  quantile_cont(first_resolution_business_minutes_including_cw, 0.95) AS fr_p95_min_inc_cw,

  quantile_cont(first_resolution_business_minutes_paused_cw, 0.50) AS fr_p50_min_pause_cw,
  quantile_cont(first_resolution_business_minutes_paused_cw, 0.90) AS fr_p90_min_pause_cw,
  quantile_cont(first_resolution_business_minutes_paused_cw, 0.95) AS fr_p95_min_pause_cw

FROM staging.case_sla_metrics
GROUP BY 1,2
ORDER BY 1,2;
