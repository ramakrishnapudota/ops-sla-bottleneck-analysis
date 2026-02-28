-- Step 7.1 — Stage durations in business minutes (case-level)
-- We compute business minutes between key milestones using the business-minute index method.

CREATE OR REPLACE TABLE staging.case_stage_durations AS
WITH m AS (
  SELECT
    case_id,
    intake_date,
    tier,
    case_type,
    team_tz,
    intake_ts,
    triage_ts,
    assignment_ts,
    investigation_ts,
    customer_wait_first_ts,
    review_qa_ts,
    resolved_ts,
    cancelled_ts
  FROM staging.case_milestones
),
-- helper: map timestamps to business-minute indices (asof join to last<=ts, then +1 if needed)
idx AS (
  SELECT
    m.*,

    (COALESCE(bi.minute_idx, -1) + CASE WHEN COALESCE(bi.minute_ts, TIMESTAMP '1900-01-01') < m.intake_ts THEN 1 ELSE 0 END) AS intake_idx,

    CASE WHEN m.triage_ts IS NULL THEN NULL ELSE
      (COALESCE(bt.minute_idx, -1) + CASE WHEN COALESCE(bt.minute_ts, TIMESTAMP '1900-01-01') < m.triage_ts THEN 1 ELSE 0 END)
    END AS triage_idx,

    CASE WHEN m.assignment_ts IS NULL THEN NULL ELSE
      (COALESCE(ba.minute_idx, -1) + CASE WHEN COALESCE(ba.minute_ts, TIMESTAMP '1900-01-01') < m.assignment_ts THEN 1 ELSE 0 END)
    END AS assignment_idx,

    CASE WHEN m.investigation_ts IS NULL THEN NULL ELSE
      (COALESCE(binv.minute_idx, -1) + CASE WHEN COALESCE(binv.minute_ts, TIMESTAMP '1900-01-01') < m.investigation_ts THEN 1 ELSE 0 END)
    END AS investigation_idx,

    CASE WHEN m.customer_wait_first_ts IS NULL THEN NULL ELSE
      (COALESCE(bcw.minute_idx, -1) + CASE WHEN COALESCE(bcw.minute_ts, TIMESTAMP '1900-01-01') < m.customer_wait_first_ts THEN 1 ELSE 0 END)
    END AS customer_wait_idx,

    CASE WHEN m.review_qa_ts IS NULL THEN NULL ELSE
      (COALESCE(bqa.minute_idx, -1) + CASE WHEN COALESCE(bqa.minute_ts, TIMESTAMP '1900-01-01') < m.review_qa_ts THEN 1 ELSE 0 END)
    END AS review_qa_idx,

    CASE WHEN m.resolved_ts IS NULL THEN NULL ELSE
      (COALESCE(br.minute_idx, -1) + CASE WHEN COALESCE(br.minute_ts, TIMESTAMP '1900-01-01') < m.resolved_ts THEN 1 ELSE 0 END)
    END AS resolved_idx

  FROM m
  ASOF JOIN staging.business_minutes_dim bi   ON m.intake_ts >= bi.minute_ts
  ASOF JOIN staging.business_minutes_dim bt   ON m.triage_ts >= bt.minute_ts
  ASOF JOIN staging.business_minutes_dim ba   ON m.assignment_ts >= ba.minute_ts
  ASOF JOIN staging.business_minutes_dim binv ON m.investigation_ts >= binv.minute_ts
  ASOF JOIN staging.business_minutes_dim bcw  ON m.customer_wait_first_ts >= bcw.minute_ts
  ASOF JOIN staging.business_minutes_dim bqa  ON m.review_qa_ts >= bqa.minute_ts
  ASOF JOIN staging.business_minutes_dim br   ON m.resolved_ts >= br.minute_ts
),
calc AS (
  SELECT
    case_id,
    intake_date,
    tier,
    case_type,
    team_tz,

    -- stage-to-stage durations (business minutes)
    CASE WHEN triage_idx IS NULL THEN NULL ELSE GREATEST(0, triage_idx - intake_idx) END AS mins_intake_to_triage,
    CASE WHEN assignment_idx IS NULL OR triage_idx IS NULL THEN NULL ELSE GREATEST(0, assignment_idx - triage_idx) END AS mins_triage_to_assignment,
    CASE WHEN investigation_idx IS NULL OR assignment_idx IS NULL THEN NULL ELSE GREATEST(0, investigation_idx - assignment_idx) END AS mins_assignment_to_investigation,
    CASE WHEN review_qa_idx IS NULL OR investigation_idx IS NULL THEN NULL ELSE GREATEST(0, review_qa_idx - investigation_idx) END AS mins_investigation_to_reviewqa,
    CASE WHEN resolved_idx IS NULL OR review_qa_idx IS NULL THEN NULL ELSE GREATEST(0, resolved_idx - review_qa_idx) END AS mins_reviewqa_to_resolved,

    CASE WHEN resolved_idx IS NULL THEN NULL ELSE GREATEST(0, resolved_idx - intake_idx) END AS mins_intake_to_resolved
  FROM idx
)
SELECT * FROM calc;
