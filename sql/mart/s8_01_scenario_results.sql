-- Step 8.2 — Scenario results (counterfactual impact)
-- Output: mart.scenario_results (one row per scenario)
--
-- S1 (process): reduce Tier 3 investigation→reviewQA time by 20% (eligible cohort)
-- S2 (quality): reduce reopens by 25% using ESTIMATED reopen penalty minutes
-- S3 (combined): S1 + S2

CREATE OR REPLACE TABLE mart.scenario_results AS
WITH base AS (
  SELECT
    s.case_id,
    s.tier,
    s.case_type,
    s.intake_date,
    s.resolved_ts,
    s.first_resolution_business_minutes_including_cw AS fr_min_inc,
    s.sla_a_breached_including_cw AS breach_inc
  FROM staging.case_sla_metrics s
  WHERE s.resolved_ts IS NOT NULL
),
stage AS (
  SELECT
    case_id,
    mins_investigation_to_reviewqa
  FROM staging.case_stage_durations
),
eligible_t3 AS (
  SELECT
    b.*,
    st.mins_investigation_to_reviewqa
  FROM base b
  JOIN stage st USING(case_id)
  WHERE b.tier = 'TIER_3'
    AND st.mins_investigation_to_reviewqa IS NOT NULL
),
s1 AS (
  SELECT
    'S1_TIER3_reduce_investigation_to_reviewQA_20pct' AS scenario_name,
    COUNT(*) AS eligible_cases,
    ROUND(100.0 * AVG(CASE WHEN breach_inc THEN 1 ELSE 0 END), 3) AS baseline_breach_pct_inc,
    ROUND(100.0 * AVG(CASE WHEN (GREATEST(0, fr_min_inc - 0.20 * mins_investigation_to_reviewqa) > 1440) THEN 1 ELSE 0 END), 3) AS scenario_breach_pct_inc,

    SUM(CASE WHEN breach_inc THEN 1 ELSE 0 END) AS baseline_breaches_inc,
    SUM(CASE WHEN (GREATEST(0, fr_min_inc - 0.20 * mins_investigation_to_reviewqa) > 1440) THEN 1 ELSE 0 END) AS scenario_breaches_inc,

    ROUND(SUM(0.20 * mins_investigation_to_reviewqa) / 60.0, 2) AS resolution_hours_saved,
    0.0 AS reopen_hours_saved
  FROM eligible_t3
),
reopen AS (
  SELECT
    case_id,
    reopen_penalty_business_minutes
  FROM staging.reopen_penalty
  WHERE reopen_penalty_business_minutes IS NOT NULL
),
s2 AS (
  SELECT
    'S2_reduce_reopens_25pct' AS scenario_name,
    COUNT(*) AS eligible_cases,
    NULL::DOUBLE AS baseline_breach_pct_inc,
    NULL::DOUBLE AS scenario_breach_pct_inc,
    NULL::BIGINT AS baseline_breaches_inc,
    NULL::BIGINT AS scenario_breaches_inc,
    0.0 AS resolution_hours_saved,
    ROUND(0.25 * SUM(reopen_penalty_business_minutes) / 60.0, 2) AS reopen_hours_saved
  FROM reopen
),
s3 AS (
  SELECT
    'S3_combined_S1_plus_S2' AS scenario_name,
    s1.eligible_cases AS eligible_cases,
    s1.baseline_breach_pct_inc AS baseline_breach_pct_inc,
    s1.scenario_breach_pct_inc AS scenario_breach_pct_inc,
    s1.baseline_breaches_inc AS baseline_breaches_inc,
    s1.scenario_breaches_inc AS scenario_breaches_inc,
    ROUND(s1.resolution_hours_saved, 2) AS resolution_hours_saved,
    (SELECT reopen_hours_saved FROM s2) AS reopen_hours_saved
  FROM s1
)
SELECT
  scenario_name,
  eligible_cases,
  baseline_breach_pct_inc,
  scenario_breach_pct_inc,
  baseline_breaches_inc,
  scenario_breaches_inc,
  (baseline_breaches_inc - scenario_breaches_inc) AS breaches_avoided_inc,
  resolution_hours_saved,
  reopen_hours_saved,
  ROUND(resolution_hours_saved + reopen_hours_saved, 2) AS total_hours_saved
FROM s1

UNION ALL
SELECT
  scenario_name,
  eligible_cases,
  baseline_breach_pct_inc,
  scenario_breach_pct_inc,
  baseline_breaches_inc,
  scenario_breaches_inc,
  NULL AS breaches_avoided_inc,
  resolution_hours_saved,
  reopen_hours_saved,
  ROUND(resolution_hours_saved + reopen_hours_saved, 2) AS total_hours_saved
FROM s2

UNION ALL
SELECT
  scenario_name,
  eligible_cases,
  baseline_breach_pct_inc,
  scenario_breach_pct_inc,
  baseline_breaches_inc,
  scenario_breaches_inc,
  (baseline_breaches_inc - scenario_breaches_inc) AS breaches_avoided_inc,
  resolution_hours_saved,
  reopen_hours_saved,
  ROUND(resolution_hours_saved + reopen_hours_saved, 2) AS total_hours_saved
FROM s3
ORDER BY scenario_name;
