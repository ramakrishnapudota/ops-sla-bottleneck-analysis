-- Mart 6.5 — Congestion index (workload vs staffing)
-- Grain: cal_date
-- Simple, interpretable congestion index:
--   congestion_index = open_backlog_proxy / NULLIF(effective_agents_total, 0)

CREATE OR REPLACE TABLE mart.congestion_daily AS
SELECT
  b.cal_date,
  s.team_tz,
  b.intake_cases,
  b.terminal_cases,
  b.open_backlog_proxy,
  s.effective_agents_total,
  ROUND(b.open_backlog_proxy::DOUBLE / NULLIF(s.effective_agents_total, 0), 4) AS congestion_index
FROM mart.backlog_daily_proxy b
LEFT JOIN mart.staffing_daily s
  ON s.cal_date = b.cal_date
ORDER BY 1;
