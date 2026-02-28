-- RAW QA 00 — Row counts and basic distribution checks
SELECT 'raw.cases' AS table_name, COUNT(*) AS row_count FROM raw.cases
UNION ALL
SELECT 'raw.events_log', COUNT(*) FROM raw.events_log
UNION ALL
SELECT 'raw.calendar_dim', COUNT(*) FROM raw.calendar_dim
UNION ALL
SELECT 'raw.staffing_schedule', COUNT(*) FROM raw.staffing_schedule;

-- Intake date partition coverage (should span ~180 days)
SELECT
  MIN(intake_date) AS min_intake_date,
  MAX(intake_date) AS max_intake_date,
  COUNT(DISTINCT intake_date) AS distinct_intake_days
FROM raw.cases;

-- Events per case distribution
SELECT
  COUNT(*) AS total_events,
  COUNT(DISTINCT case_id) AS distinct_cases_in_events,
  ROUND(COUNT(*)::DOUBLE / NULLIF(COUNT(DISTINCT case_id), 0), 3) AS avg_events_per_case
FROM raw.events_log;
