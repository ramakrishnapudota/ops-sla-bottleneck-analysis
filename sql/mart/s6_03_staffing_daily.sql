-- Mart 6.3 — Staffing daily rollups (planned vs effective)
-- Grain: shift_date

CREATE OR REPLACE TABLE mart.staffing_daily AS
SELECT
  shift_date AS cal_date,
  team_tz,
  SUM(planned_agents) AS planned_agents_total,
  SUM(effective_agents) AS effective_agents_total,
  ROUND(AVG(shrinkage_rate), 4) AS avg_shrinkage_rate,
  ROUND(AVG(deterioration_multiplier), 4) AS avg_deterioration_multiplier
FROM raw.staffing_schedule
GROUP BY 1,2
ORDER BY 1,2;
