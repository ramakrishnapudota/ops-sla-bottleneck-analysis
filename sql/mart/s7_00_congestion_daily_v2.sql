-- Step 7 Fix — Congestion drivers aligned to business-hour SLAs
-- - Exclude weekends/holidays (business-day view)
-- - Add flow-aligned index: intake_cases per effective agent
-- - Add backlog change (delta) to represent accumulating work

CREATE OR REPLACE TABLE mart.congestion_daily_v2 AS
WITH cal AS (
  SELECT
    cal_date,
    is_weekend,
    is_holiday
  FROM raw.calendar_dim
),
base AS (
  SELECT
    b.cal_date,
    s.team_tz,
    b.intake_cases,
    b.terminal_cases,
    b.open_backlog_proxy,
    s.effective_agents_total
  FROM mart.backlog_daily_proxy b
  LEFT JOIN mart.staffing_daily s
    ON s.cal_date = b.cal_date
  JOIN cal c
    ON c.cal_date = b.cal_date
  WHERE NOT c.is_weekend
    AND NOT c.is_holiday
),
with_delta AS (
  SELECT
    *,
    (open_backlog_proxy
      - LAG(open_backlog_proxy) OVER (PARTITION BY team_tz ORDER BY cal_date)
    ) AS backlog_delta
  FROM base
)
SELECT
  cal_date,
  team_tz,
  intake_cases,
  terminal_cases,
  open_backlog_proxy,
  backlog_delta,
  effective_agents_total,
  ROUND(intake_cases::DOUBLE / NULLIF(effective_agents_total, 0), 4) AS congestion_flow_index,
  ROUND(open_backlog_proxy::DOUBLE / NULLIF(effective_agents_total, 0), 4) AS congestion_stock_index
FROM with_delta
ORDER BY 1,2;
