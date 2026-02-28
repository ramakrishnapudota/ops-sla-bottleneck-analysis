-- Step 5.2 — SLA metrics in business time
-- SLA B: INTAKE -> TRIAGE within 2 business hours
-- SLA A: INTAKE -> first RESOLVED within 24 business hours
--   Variant 1: includes CUSTOMER_WAIT time
--   Variant 2: pauses clock during CUSTOMER_WAIT (subtracts business minutes within wait intervals)

-- Helper idea:
-- Convert any timestamp to a business-minute "index" using ASOF JOIN to the minute spine.
-- Then business_minutes_between(start, end) = end_idx - start_idx
-- where idx is the first business minute at/after timestamp (implemented via last<=ts + correction).

CREATE OR REPLACE TABLE staging.case_sla_metrics AS
WITH base_cases AS (
  SELECT
    case_id,
    intake_ts,
    triage_ts,
    resolved_ts,
    cancelled_ts,
    customer_wait_first_ts,
    case_type,
    tier,
    team_tz,
    intake_date
  FROM staging.case_milestones
),
-- Resolve "first RESOLVED at/after intake" (to avoid negative cycles due to tz inconsistency)
resolved_fix AS (
  SELECT
    b.*,
    (
      SELECT MIN(e.event_ts_canonical)
      FROM staging.events_clean e
      WHERE e.case_id = b.case_id
        AND e.status = 'RESOLVED'
        AND e.event_ts_canonical >= b.intake_ts
    ) AS resolved_ts_after_intake
  FROM base_cases b
),
triage_fix AS (
  SELECT
    r.*,
    (
      SELECT MIN(e.event_ts_canonical)
      FROM staging.events_clean e
      WHERE e.case_id = r.case_id
        AND e.status = 'TRIAGE'
        AND e.event_ts_canonical >= r.intake_ts
    ) AS triage_ts_after_intake
  FROM resolved_fix r
),
inputs AS (
  SELECT
    case_id,
    intake_ts,
    -- triage: prefer after-intake if available
    COALESCE(triage_ts_after_intake, triage_ts) AS triage_ts_final,
    -- resolve: prefer after-intake if available
    COALESCE(resolved_ts_after_intake, resolved_ts) AS resolved_ts_final,
    cancelled_ts,
    case_type,
    tier,
    team_tz,
    intake_date
  FROM triage_fix
),
-- CUSTOMER_WAIT intervals: each CUSTOMER_WAIT event until the next event for that case
cw_intervals AS (
  SELECT
    e.case_id,
    e.event_ts_canonical AS cw_start_ts,
    (
      SELECT MIN(e2.event_ts_canonical)
      FROM staging.events_clean e2
      WHERE e2.case_id = e.case_id
        AND e2.event_ts_canonical > e.event_ts_canonical
    ) AS cw_end_ts
  FROM staging.events_clean e
  WHERE e.status = 'CUSTOMER_WAIT'
),
-- Map CW intervals to business-minute indices and sum
cw_minutes AS (
  SELECT
    ci.case_id,
    SUM(
      CASE
        WHEN ci.cw_end_ts IS NULL THEN 0
        ELSE GREATEST(0, cw_end_idx - cw_start_idx)
      END
    ) AS customer_wait_business_minutes
  FROM (
    SELECT
      ci.case_id,
      ci.cw_start_ts,
      ci.cw_end_ts,

      -- start index (first business minute at/after cw_start_ts)
      (COALESCE(b1.minute_idx, -1)
        + CASE WHEN COALESCE(b1.minute_ts, TIMESTAMP '1900-01-01') < ci.cw_start_ts THEN 1 ELSE 0 END
      ) AS cw_start_idx,

      -- end index (first business minute at/after cw_end_ts)
      (COALESCE(b2.minute_idx, -1)
        + CASE WHEN COALESCE(b2.minute_ts, TIMESTAMP '1900-01-01') < ci.cw_end_ts THEN 1 ELSE 0 END
      ) AS cw_end_idx

    FROM cw_intervals ci
    -- ASOF: match last minute_ts <= timestamp
    ASOF JOIN staging.business_minutes_dim b1
      ON ci.cw_start_ts >= b1.minute_ts
    ASOF JOIN staging.business_minutes_dim b2
      ON ci.cw_end_ts >= b2.minute_ts
  ) ci
  GROUP BY 1
),
-- Map case endpoints to business-minute indices
idxs AS (
  SELECT
    i.*,

    -- intake index
    (COALESCE(bi.minute_idx, -1)
      + CASE WHEN COALESCE(bi.minute_ts, TIMESTAMP '1900-01-01') < i.intake_ts THEN 1 ELSE 0 END
    ) AS intake_idx,

    -- triage index
    CASE WHEN i.triage_ts_final IS NULL THEN NULL ELSE
      (COALESCE(bt.minute_idx, -1)
        + CASE WHEN COALESCE(bt.minute_ts, TIMESTAMP '1900-01-01') < i.triage_ts_final THEN 1 ELSE 0 END
      )
    END AS triage_idx,

    -- resolved index
    CASE WHEN i.resolved_ts_final IS NULL THEN NULL ELSE
      (COALESCE(br.minute_idx, -1)
        + CASE WHEN COALESCE(br.minute_ts, TIMESTAMP '1900-01-01') < i.resolved_ts_final THEN 1 ELSE 0 END
      )
    END AS resolved_idx

  FROM inputs i
  ASOF JOIN staging.business_minutes_dim bi
    ON i.intake_ts >= bi.minute_ts
  ASOF JOIN staging.business_minutes_dim bt
    ON i.triage_ts_final >= bt.minute_ts
  ASOF JOIN staging.business_minutes_dim br
    ON i.resolved_ts_final >= br.minute_ts
),
calc AS (
  SELECT
    x.case_id,
    x.case_type,
    x.tier,
    x.team_tz,
    x.intake_date,

    x.intake_ts,
    x.triage_ts_final AS triage_ts,
    x.resolved_ts_final AS resolved_ts,
    x.cancelled_ts,

    -- Business minutes between intake and triage (SLA B)
    CASE
      WHEN x.triage_idx IS NULL THEN NULL
      ELSE GREATEST(0, x.triage_idx - x.intake_idx)
    END AS first_touch_business_minutes,

    -- Business minutes between intake and resolved (SLA A include CW)
    CASE
      WHEN x.resolved_idx IS NULL THEN NULL
      ELSE GREATEST(0, x.resolved_idx - x.intake_idx)
    END AS first_resolution_business_minutes_including_cw

  FROM idxs x
),
final AS (
  SELECT
    c.*,

    COALESCE(w.customer_wait_business_minutes, 0) AS customer_wait_business_minutes,

    -- SLA A paused variant
    CASE
      WHEN c.first_resolution_business_minutes_including_cw IS NULL THEN NULL
      ELSE GREATEST(
        0,
        c.first_resolution_business_minutes_including_cw - COALESCE(w.customer_wait_business_minutes, 0)
      )
    END AS first_resolution_business_minutes_paused_cw,

    -- Convert to business hours (for readability)
    CASE WHEN c.first_touch_business_minutes IS NULL THEN NULL
         ELSE ROUND(c.first_touch_business_minutes / 60.0, 3) END AS first_touch_business_hours,

    CASE WHEN c.first_resolution_business_minutes_including_cw IS NULL THEN NULL
         ELSE ROUND(c.first_resolution_business_minutes_including_cw / 60.0, 3) END AS first_resolution_business_hours_including_cw,

    CASE WHEN c.first_resolution_business_minutes_including_cw IS NULL THEN NULL
         ELSE ROUND(
           (GREATEST(0, c.first_resolution_business_minutes_including_cw - COALESCE(w.customer_wait_business_minutes, 0))) / 60.0
         , 3) END AS first_resolution_business_hours_paused_cw,

    -- SLA thresholds (locked): B=2 business hours, A=24 business hours
    CASE
      WHEN c.first_touch_business_minutes IS NULL THEN NULL
      ELSE (c.first_touch_business_minutes > 120)
    END AS sla_b_breached,

    CASE
      WHEN c.first_resolution_business_minutes_including_cw IS NULL THEN NULL
      ELSE (c.first_resolution_business_minutes_including_cw > 1440)
    END AS sla_a_breached_including_cw,

    CASE
      WHEN c.first_resolution_business_minutes_including_cw IS NULL THEN NULL
      ELSE (GREATEST(0, c.first_resolution_business_minutes_including_cw - COALESCE(w.customer_wait_business_minutes, 0)) > 1440)
    END AS sla_a_breached_paused_cw,

    -- Data-quality tracking flags
    CASE
      WHEN c.triage_ts IS NULL THEN TRUE ELSE FALSE
    END AS is_triage_missing_for_sla,

    CASE
      WHEN c.resolved_ts IS NULL AND c.cancelled_ts IS NULL THEN TRUE ELSE FALSE
    END AS is_terminal_missing_for_sla,

    CASE
      WHEN c.triage_ts IS NOT NULL AND c.triage_ts < c.intake_ts THEN TRUE ELSE FALSE
    END AS is_triage_before_intake_for_sla

  FROM calc c
  LEFT JOIN cw_minutes w USING(case_id)
)
SELECT * FROM final;
