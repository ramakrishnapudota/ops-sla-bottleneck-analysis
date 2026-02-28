-- Step 4.3 — Case-level milestones using authoritative intake_ts from raw.cases.
-- Key rule:
-- - TRIAGE timestamp should be the first TRIAGE event at/after intake_ts when available
--   else fall back to earliest TRIAGE.
-- Same rule will be used later for other lead-time milestones.

CREATE OR REPLACE TABLE staging.case_milestones AS
WITH c AS (
  SELECT
    case_id,
    intake_ts,
    case_type,
    tier,
    team_tz,
    intake_date
  FROM raw.cases
),
e AS (
  SELECT
    case_id,
    status,
    event_ts_canonical
  FROM staging.events_clean
),
agg AS (
  SELECT
    c.case_id,
    c.intake_ts,
    c.case_type,
    c.tier,
    c.team_tz,
    c.intake_date,

    -- earliest observed timestamps (raw-ish)
    MIN(CASE WHEN e.status = 'TRIAGE' THEN e.event_ts_canonical END) AS triage_ts_any,
    MIN(CASE WHEN e.status = 'ASSIGNMENT' THEN e.event_ts_canonical END) AS assignment_ts,
    MIN(CASE WHEN e.status = 'INVESTIGATION' THEN e.event_ts_canonical END) AS investigation_ts,
    MIN(CASE WHEN e.status = 'CUSTOMER_WAIT' THEN e.event_ts_canonical END) AS customer_wait_first_ts,
    MIN(CASE WHEN e.status = 'REVIEW_QA' THEN e.event_ts_canonical END) AS review_qa_ts,
    MIN(CASE WHEN e.status = 'RESOLVED' THEN e.event_ts_canonical END) AS resolved_ts_any,
    MIN(CASE WHEN e.status = 'CANCELLED' THEN e.event_ts_canonical END) AS cancelled_ts,
    MIN(CASE WHEN e.status = 'REOPENED' THEN e.event_ts_canonical END) AS reopened_first_ts,
    MIN(CASE WHEN e.status = 'ESCALATED' THEN e.event_ts_canonical END) AS escalated_first_ts

  FROM c
  LEFT JOIN e ON e.case_id = c.case_id
  GROUP BY 1,2,3,4,5,6
),
triage_valid AS (
  SELECT
    a.*,

    -- triage at/after intake if exists
    (
      SELECT MIN(e2.event_ts_canonical)
      FROM staging.events_clean e2
      WHERE e2.case_id = a.case_id
        AND e2.status = 'TRIAGE'
        AND e2.event_ts_canonical >= a.intake_ts
    ) AS triage_ts_after_intake

  FROM agg a
)
SELECT
  case_id,
  intake_ts,
  case_type,
  tier,
  team_tz,
  intake_date,

  COALESCE(triage_ts_after_intake, triage_ts_any) AS triage_ts,
  assignment_ts,
  investigation_ts,
  customer_wait_first_ts,
  review_qa_ts,
  resolved_ts_any AS resolved_ts,
  cancelled_ts,
  reopened_first_ts,
  escalated_first_ts,

  -- derived quality flags
  CASE WHEN triage_ts_any IS NULL THEN TRUE ELSE FALSE END AS is_triage_missing,
  CASE WHEN resolved_ts_any IS NULL AND cancelled_ts IS NULL THEN TRUE ELSE FALSE END AS is_terminal_missing,

  CASE
    WHEN COALESCE(triage_ts_after_intake, triage_ts_any) IS NOT NULL
     AND COALESCE(triage_ts_after_intake, triage_ts_any) < intake_ts
    THEN TRUE ELSE FALSE
  END AS is_triage_before_intake_after_fix

FROM triage_valid;
