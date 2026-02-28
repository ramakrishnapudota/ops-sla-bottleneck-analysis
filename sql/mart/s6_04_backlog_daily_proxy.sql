-- Mart 6.4 — Backlog proxy and flow metrics
-- Grain: cal_date
-- Backlog proxy:
--   open_backlog_proxy = cumulative_intake - cumulative_terminal (resolved + cancelled)

CREATE OR REPLACE TABLE mart.backlog_daily_proxy AS
WITH daily_intake AS (
  SELECT intake_date AS cal_date, COUNT(*) AS intake_cases
  FROM raw.cases
  GROUP BY 1
),
daily_terminal AS (
  SELECT
    CAST(COALESCE(resolved_ts, cancelled_ts) AS DATE) AS cal_date,
    COUNT(*) AS terminal_cases
  FROM staging.case_milestones
  WHERE resolved_ts IS NOT NULL OR cancelled_ts IS NOT NULL
  GROUP BY 1
),
calendar AS (
  SELECT cal_date::DATE AS cal_date
  FROM raw.calendar_dim
)
SELECT
  c.cal_date,
  COALESCE(i.intake_cases, 0) AS intake_cases,
  COALESCE(t.terminal_cases, 0) AS terminal_cases,

  SUM(COALESCE(i.intake_cases, 0)) OVER (ORDER BY c.cal_date) AS cum_intake,
  SUM(COALESCE(t.terminal_cases, 0)) OVER (ORDER BY c.cal_date) AS cum_terminal,

  (SUM(COALESCE(i.intake_cases, 0)) OVER (ORDER BY c.cal_date)
   - SUM(COALESCE(t.terminal_cases, 0)) OVER (ORDER BY c.cal_date)) AS open_backlog_proxy

FROM calendar c
LEFT JOIN daily_intake i USING(cal_date)
LEFT JOIN daily_terminal t USING(cal_date)
ORDER BY 1;
