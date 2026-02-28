from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List

import duckdb

DB_PATH = "ops_warehouse.duckdb"
OUT_PATH = Path("reports/run_summaries/step4_summary.json")

SQL_FILES = [
    "sql/staging/s4_01_events_dedup.sql",
    "sql/staging/s4_02_events_clean.sql",
    "sql/staging/s4_03_case_milestones.sql",
]


def run() -> Dict:
    t0 = time.perf_counter()
    con = duckdb.connect(DB_PATH)

    file_timings = {}
    for f in SQL_FILES:
        sql = Path(f).read_text(encoding="utf-8")
        t1 = time.perf_counter()
        con.execute(sql)
        t2 = time.perf_counter()
        file_timings[f] = round(t2 - t1, 3)

    counts = {
        "raw_events_log": con.execute("SELECT COUNT(*) FROM raw.events_log").fetchone()[0],
        "staging_events_deduped": con.execute("SELECT COUNT(*) FROM staging.events_deduped").fetchone()[0],
        "staging_events_clean": con.execute("SELECT COUNT(*) FROM staging.events_clean").fetchone()[0],
        "staging_case_milestones": con.execute("SELECT COUNT(*) FROM staging.case_milestones").fetchone()[0],
    }
    dedup_removed = counts["raw_events_log"] - counts["staging_events_deduped"]

    triage_before_intake_raw = con.execute("""
        WITH t AS (
          SELECT
            case_id,
            MIN(CASE WHEN status='INTAKE' THEN COALESCE(event_ts, ingestion_ts) END) AS intake_ts,
            MIN(CASE WHEN status='TRIAGE' THEN COALESCE(event_ts, ingestion_ts) END) AS triage_ts
          FROM raw.events_log
          GROUP BY 1
        )
        SELECT ROUND(100.0 * AVG(CASE WHEN triage_ts < intake_ts THEN 1 ELSE 0 END), 4)
        FROM t
        WHERE intake_ts IS NOT NULL AND triage_ts IS NOT NULL;
    """).fetchone()[0]

    triage_before_intake_fixed = con.execute("""
        WITH t AS (
          SELECT case_id, intake_ts, triage_ts
          FROM staging.case_milestones
        )
        SELECT ROUND(100.0 * AVG(CASE WHEN triage_ts < intake_ts THEN 1 ELSE 0 END), 4)
        FROM t
        WHERE intake_ts IS NOT NULL AND triage_ts IS NOT NULL;
    """).fetchone()[0]

    anomaly_rates = con.execute("""
        SELECT
          ROUND(100.0 * AVG(CASE WHEN is_event_ts_missing THEN 1 ELSE 0 END), 3) AS pct_event_ts_missing,
          ROUND(100.0 * AVG(CASE WHEN is_tz_inconsistent THEN 1 ELSE 0 END), 3) AS pct_tz_inconsistent,
          ROUND(100.0 * AVG(CASE WHEN is_ingestion_before_event_ts THEN 1 ELSE 0 END), 3) AS pct_ingestion_before_event_ts
        FROM staging.events_clean;
    """).fetchdf().to_dict(orient="records")[0]

    con.close()
    t_end = time.perf_counter()

    summary = {
        "step": 4,
        "runtime_seconds": {"by_file": file_timings, "end_to_end": round(t_end - t0, 3)},
        "counts": counts,
        "dedup_removed_rows": int(dedup_removed),
        "triage_before_intake_pct": {
            "raw_signal": float(triage_before_intake_raw),
            "after_milestone_fix": float(triage_before_intake_fixed),
        },
        "anomaly_rates_pct": anomaly_rates,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"\nWrote: {OUT_PATH}")
    return summary


if __name__ == "__main__":
    run()
