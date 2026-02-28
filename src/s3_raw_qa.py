from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List

import duckdb

DB_PATH = "ops_warehouse.duckdb"
OUT_PATH = Path("reports/run_summaries/step3_summary.json")

SQL_FILES = [
    "sql/raw/qa_00_row_counts.sql",
    "sql/raw/qa_01_event_integrity.sql",
    "sql/raw/qa_02_case_milestones.sql",
]


def _split_sql(sql: str) -> List[str]:
    # naive split on semicolons (safe for our simple QA files)
    return [s.strip() for s in sql.split(";") if s.strip()]


def run() -> Dict:
    t0 = time.perf_counter()
    con = duckdb.connect(DB_PATH)

    per_file_timings = {}
    # execute files (primarily for reproducibility; results will be recomputed below as structured metrics)
    for f in SQL_FILES:
        sql = Path(f).read_text(encoding="utf-8")
        stmts = _split_sql(sql)
        tf0 = time.perf_counter()
        for st in stmts:
            con.execute(st)
        tf1 = time.perf_counter()
        per_file_timings[f] = round(tf1 - tf0, 3)

    # Structured metrics (single-source-of-truth fields for logging)
    counts = {
        "raw_cases": con.execute("SELECT COUNT(*) FROM raw.cases").fetchone()[0],
        "raw_events_log": con.execute("SELECT COUNT(*) FROM raw.events_log").fetchone()[0],
        "raw_calendar_dim": con.execute("SELECT COUNT(*) FROM raw.calendar_dim").fetchone()[0],
        "raw_staffing_schedule": con.execute("SELECT COUNT(*) FROM raw.staffing_schedule").fetchone()[0],
    }

    intake_coverage = con.execute("""
        SELECT
          MIN(intake_date) AS min_intake_date,
          MAX(intake_date) AS max_intake_date,
          COUNT(DISTINCT intake_date) AS distinct_intake_days
        FROM raw.cases;
    """).fetchdf().to_dict(orient="records")[0]

    events_per_case = con.execute("""
        SELECT
          COUNT(*) AS total_events,
          COUNT(DISTINCT case_id) AS distinct_cases_in_events,
          ROUND(COUNT(*)::DOUBLE / NULLIF(COUNT(DISTINCT case_id), 0), 3) AS avg_events_per_case
        FROM raw.events_log;
    """).fetchdf().to_dict(orient="records")[0]

    integrity = con.execute("""
        SELECT
          ROUND(100.0 * AVG(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END), 3) AS pct_missing_event_ts,
          ROUND(100.0 * AVG(CASE WHEN is_duplicate THEN 1 ELSE 0 END), 3) AS pct_duplicate_flag,
          ROUND(100.0 * AVG(CASE WHEN is_late_arriving THEN 1 ELSE 0 END), 3) AS pct_late_arriving_flag,
          ROUND(100.0 * AVG(CASE WHEN event_tz = 'INCONSISTENT' THEN 1 ELSE 0 END), 3) AS pct_tz_inconsistent
        FROM raw.events_log;
    """).fetchdf().to_dict(orient="records")[0]

    dup_groups = con.execute("""
        SELECT
          COUNT(*) AS duplicate_groups,
          SUM(cnt - 1) AS duplicate_extra_rows
        FROM (
          SELECT case_id, status, event_ts, COUNT(*) AS cnt
          FROM raw.events_log
          WHERE event_ts IS NOT NULL
          GROUP BY 1,2,3
          HAVING COUNT(*) > 1
        );
    """).fetchdf().to_dict(orient="records")[0]

    ingestion_before_event = con.execute("""
        SELECT
          ROUND(100.0 * AVG(CASE WHEN event_ts IS NOT NULL AND ingestion_ts < event_ts THEN 1 ELSE 0 END), 3) AS pct_ingestion_before_event_ts
        FROM raw.events_log;
    """).fetchone()[0]

    milestone_coverage = con.execute("""
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
    """).fetchdf().to_dict(orient="records")[0]

    triage_before_intake = con.execute("""
        WITH t AS (
          SELECT
            case_id,
            MIN(CASE WHEN status='INTAKE' THEN COALESCE(event_ts, ingestion_ts) END) AS intake_ts,
            MIN(CASE WHEN status='TRIAGE' THEN COALESCE(event_ts, ingestion_ts) END) AS triage_ts
          FROM raw.events_log
          GROUP BY 1
        )
        SELECT
          COUNT(*) AS cases_with_both,
          ROUND(100.0 * AVG(CASE WHEN triage_ts < intake_ts THEN 1 ELSE 0 END), 4) AS pct_triage_before_intake
        FROM t
        WHERE intake_ts IS NOT NULL AND triage_ts IS NOT NULL;
    """).fetchdf().to_dict(orient="records")[0]

    cancelled_and_resolved = con.execute("""
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
    """).fetchone()[0]

    con.close()
    t1 = time.perf_counter()

    summary = {
        "step": 3,
        "runtime_seconds": {
            "by_file": per_file_timings,
            "end_to_end": round(t1 - t0, 3),
        },
        "counts": counts,
        "intake_coverage": intake_coverage,
        "events_per_case": events_per_case,
        "event_integrity_pct": integrity,
        "duplicate_groups": dup_groups,
        "pct_ingestion_before_event_ts": float(ingestion_before_event),
        "milestone_coverage_pct": milestone_coverage,
        "triage_before_intake_pct": triage_before_intake,
        "pct_cases_both_cancelled_and_resolved": float(cancelled_and_resolved),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"\nWrote: {OUT_PATH}")
    return summary


if __name__ == "__main__":
    run()
