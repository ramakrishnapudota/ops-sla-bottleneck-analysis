-- Step 7.4 — Stage bottleneck ranking (minutes)
-- Grain: tier (p50/p90/p95 per stage)

CREATE OR REPLACE TABLE mart.driver_stage_durations AS
SELECT
  tier,

  quantile_cont(mins_intake_to_triage, 0.50) AS p50_intake_to_triage,
  quantile_cont(mins_intake_to_triage, 0.90) AS p90_intake_to_triage,
  quantile_cont(mins_intake_to_triage, 0.95) AS p95_intake_to_triage,

  quantile_cont(mins_triage_to_assignment, 0.50) AS p50_triage_to_assignment,
  quantile_cont(mins_triage_to_assignment, 0.90) AS p90_triage_to_assignment,
  quantile_cont(mins_triage_to_assignment, 0.95) AS p95_triage_to_assignment,

  quantile_cont(mins_assignment_to_investigation, 0.50) AS p50_assignment_to_investigation,
  quantile_cont(mins_assignment_to_investigation, 0.90) AS p90_assignment_to_investigation,
  quantile_cont(mins_assignment_to_investigation, 0.95) AS p95_assignment_to_investigation,

  quantile_cont(mins_investigation_to_reviewqa, 0.50) AS p50_investigation_to_reviewqa,
  quantile_cont(mins_investigation_to_reviewqa, 0.90) AS p90_investigation_to_reviewqa,
  quantile_cont(mins_investigation_to_reviewqa, 0.95) AS p95_investigation_to_reviewqa,

  quantile_cont(mins_reviewqa_to_resolved, 0.50) AS p50_reviewqa_to_resolved,
  quantile_cont(mins_reviewqa_to_resolved, 0.90) AS p90_reviewqa_to_resolved,
  quantile_cont(mins_reviewqa_to_resolved, 0.95) AS p95_reviewqa_to_resolved,

  quantile_cont(mins_intake_to_resolved, 0.50) AS p50_total,
  quantile_cont(mins_intake_to_resolved, 0.90) AS p90_total,
  quantile_cont(mins_intake_to_resolved, 0.95) AS p95_total

FROM staging.case_stage_durations
WHERE mins_intake_to_resolved IS NOT NULL
GROUP BY 1
ORDER BY 1;
