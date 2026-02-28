-- Mart 6.1 — Daily SLA metrics (trend view)
-- Grain: intake_date (case intake date)

CREATE OR REPLACE TABLE mart.sla_daily AS
WITH base AS (
  SELECT
    intake_date,
    first_touch_business_minutes,
    first_resolution_business_minutes_including_cw,
    first_resolution_business_minutes_paused_cw,
    sla_b_breached,
    sla_a_breached_including_cw,
    sla_a_breached_paused_cw,
    is_triage_missing_for_sla,
    is_terminal_missing_for_sla
  FROM staging.case_sla_metrics
)
SELECT
  intake_date,

  COUNT(*) AS cases_in_sla_table,

  -- SLA B evaluated when triage exists
  SUM(CASE WHEN first_touch_business_minutes IS NOT NULL THEN 1 ELSE 0 END) AS cases_with_triage,
  ROUND(100.0 * AVG(CASE WHEN first_touch_business_minutes IS NOT NULL AND sla_b_breached THEN 1 ELSE 0 END), 3) AS sla_b_breach_pct,

  quantile_cont(first_touch_business_minutes, 0.50) AS ft_p50_min,
  quantile_cont(first_touch_business_minutes, 0.90) AS ft_p90_min,
  quantile_cont(first_touch_business_minutes, 0.95) AS ft_p95_min,

  -- SLA A evaluated on resolved cases (terminal)
  SUM(CASE WHEN first_resolution_business_minutes_including_cw IS NOT NULL THEN 1 ELSE 0 END) AS cases_resolved,

  ROUND(100.0 * AVG(CASE WHEN first_resolution_business_minutes_including_cw IS NOT NULL AND sla_a_breached_including_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_including_cw,
  ROUND(100.0 * AVG(CASE WHEN first_resolution_business_minutes_paused_cw IS NOT NULL AND sla_a_breached_paused_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_paused_cw,

  quantile_cont(first_resolution_business_minutes_including_cw, 0.50) AS fr_p50_min_inc_cw,
  quantile_cont(first_resolution_business_minutes_including_cw, 0.90) AS fr_p90_min_inc_cw,
  quantile_cont(first_resolution_business_minutes_including_cw, 0.95) AS fr_p95_min_inc_cw,

  quantile_cont(first_resolution_business_minutes_paused_cw, 0.50) AS fr_p50_min_pause_cw,
  quantile_cont(first_resolution_business_minutes_paused_cw, 0.90) AS fr_p90_min_pause_cw,
  quantile_cont(first_resolution_business_minutes_paused_cw, 0.95) AS fr_p95_min_pause_cw,

  -- DQ rollups (monitoring)
  ROUND(100.0 * AVG(CASE WHEN is_triage_missing_for_sla THEN 1 ELSE 0 END), 3) AS dq_triage_missing_pct,
  ROUND(100.0 * AVG(CASE WHEN is_terminal_missing_for_sla THEN 1 ELSE 0 END), 3) AS dq_terminal_missing_pct

FROM base
GROUP BY 1
ORDER BY 1;
