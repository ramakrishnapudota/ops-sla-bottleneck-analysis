-- Step 8.1 (final) — Reopen penalty (ESTIMATED rework minutes)
-- Problem: event stream may not contain a second RESOLVED after REOPENED.
-- Solution: estimate reopen rework using observed bottleneck distribution.
--
-- Assumption (explicit):
--   Each reopen incurs additional work roughly equal to the tier-level median
--   investigation→review/QA business minutes (p50), which is the dominant bottleneck stage.
--
-- Output: one row per reopened case with estimated rework minutes.

CREATE OR REPLACE TABLE staging.reopen_penalty AS
WITH tier_penalty AS (
  SELECT
    tier,
    quantile_cont(mins_investigation_to_reviewqa, 0.50) AS est_reopen_penalty_business_minutes
  FROM staging.case_stage_durations
  WHERE mins_investigation_to_reviewqa IS NOT NULL
  GROUP BY 1
),
reopened_cases AS (
  SELECT DISTINCT
    e.case_id
  FROM staging.events_clean e
  WHERE e.status = 'REOPENED'
),
case_dim AS (
  SELECT
    s.case_id,
    s.tier,
    s.case_type
  FROM staging.case_sla_metrics s
)
SELECT
  r.case_id,
  c.tier,
  c.case_type,
  tp.est_reopen_penalty_business_minutes AS reopen_penalty_business_minutes
FROM reopened_cases r
JOIN case_dim c USING(case_id)
JOIN tier_penalty tp USING(tier);
