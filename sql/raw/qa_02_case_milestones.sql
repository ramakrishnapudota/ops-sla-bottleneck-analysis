-- RAW QA 02 — Case milestone completeness + ordering signals

-- Coverage: how many cases have key statuses at least once
WITH flags AS (
  SELECT
    case_id,
    MAX(CASE WHEN status='TRIAGE' THEN 1 ELSE 0 END) AS has_triage,
    MAX(CASE WHEN status='RESOLVED' THEN 1 ELSE 0 END) AS has_resolved,
    MAX(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) AS has_cancelled,
    MAX(CASE WHEN status='REOPENED' THEN 1 ELSE 0 END) AS has_reopened,
    MAX(CASE WHEN status='ESCALATED' THEN 1 ELSE 0 END) AS has_escalated,
    MAX(CASE WHEN status='CUSTOMER_WAIT' THEN 1 ELSE 0 END) AS has_customer_wait
  FROM raw.events_log
  GROUP BY 1
)
SELECT
  COUNT(*) AS cases_in_events,
  ROUND(100.0 * AVG(has_triage), 3) AS pct_cases_with_triage,
  ROUND(100.0 * AVG(has_resolved), 3) AS pct_cases_with_resolved,
  ROUND(100.0 * AVG(has_cancelled), 3) AS pct_cases_with_cancelled,
  ROUND(100.0 * AVG(has_reopened), 3) AS pct_cases_with_reopened,
  ROUND(100.0 * AVG(has_escalated), 3) AS pct_cases_with_escalated,
  ROUND(100.0 * AVG(has_customer_wait), 3) AS pct_cases_with_customer_wait
FROM flags;

-- Ordering signal: for cases with both INTAKE and TRIAGE timestamps present, triage should be after intake.
WITH t AS (
  SELECT
    case_id,
    MIN(CASE WHEN status='INTAKE' THEN event_ts END) AS intake_ts,
    MIN(CASE WHEN status='TRIAGE' THEN event_ts END) AS triage_ts
  FROM raw.events_log
  GROUP BY 1
)
SELECT
  COUNT(*) AS cases_with_both,
  ROUND(100.0 * AVG(CASE WHEN triage_ts < intake_ts THEN 1 ELSE 0 END), 4) AS pct_triage_before_intake
FROM t
WHERE intake_ts IS NOT NULL AND triage_ts IS NOT NULL;

-- Cancelled vs resolved sanity: cases should generally have one or the other (not both).
WITH f AS (
  SELECT
    case_id,
    MAX(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
    MAX(CASE WHEN status='RESOLVED' THEN 1 ELSE 0 END) AS resolved
  FROM raw.events_log
  GROUP BY 1
)
SELECT
  ROUND(100.0 * AVG(CASE WHEN cancelled=1 AND resolved=1 THEN 1 ELSE 0 END), 4) AS pct_cases_both_cancelled_and_resolved
FROM f;
