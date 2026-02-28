from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

import duckdb

DB_PATH = "ops_warehouse.duckdb"
OUT_PATH = Path("reports/run_summaries/step7_summary.json")

SQL_FILES = [
    "sql/staging/s7_01_case_stage_durations.sql",
    "sql/staging/s7_00_case_congestion_exposure.sql",
    "sql/mart/s7_00_congestion_daily_v2.sql",
    "sql/mart/s7_01_driver_congestion_buckets.sql",
    "sql/mart/s7_02_driver_reopen_impact.sql",
    "sql/mart/s7_03_driver_stage_durations.sql",
    "sql/mart/s7_04_driver_summary.sql",
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
        "staging_case_stage_durations": con.execute("SELECT COUNT(*) FROM staging.case_stage_durations").fetchone()[0],
        "driver_congestion_buckets": con.execute("SELECT COUNT(*) FROM mart.driver_congestion_buckets").fetchone()[0],
        "driver_reopen_impact": con.execute("SELECT COUNT(*) FROM mart.driver_reopen_impact").fetchone()[0],
        "driver_stage_durations": con.execute("SELECT COUNT(*) FROM mart.driver_stage_durations").fetchone()[0],
        "driver_summary": con.execute("SELECT COUNT(*) FROM mart.driver_summary").fetchone()[0],
    }

    # Pull a few headline values for resume bullets
    summary_row = con.execute("SELECT * FROM mart.driver_summary").fetchdf().to_dict(orient="records")[0]

    # Top-level stage bottleneck: which stage has highest p95 contribution (by tier=ALL proxy using totals)
    # We'll compute based on p95 values for Tier 3 (most stressed) as the "worst-case ops" narrative.
    t3 = con.execute("""
      SELECT *
      FROM mart.driver_stage_durations
      WHERE tier = 'TIER_3'
    """).fetchdf().to_dict(orient="records")
    tier3 = t3[0] if t3 else {}

    bottleneck = None
    if tier3:
        stages = {
            "intake_to_triage": tier3.get("p95_intake_to_triage"),
            "triage_to_assignment": tier3.get("p95_triage_to_assignment"),
            "assignment_to_investigation": tier3.get("p95_assignment_to_investigation"),
            "investigation_to_reviewqa": tier3.get("p95_investigation_to_reviewqa"),
            "reviewqa_to_resolved": tier3.get("p95_reviewqa_to_resolved"),
        }
        bottleneck = max(stages.items(), key=lambda kv: (kv[1] if kv[1] is not None else -1))

    con.close()
    t_end = time.perf_counter()

    out = {
        "step": 7,
        "runtime_seconds": {"by_file": file_timings, "end_to_end": round(t_end - t0, 3)},
        "counts": counts,
        "driver_summary": summary_row,
        "tier3_p95_bottleneck_stage": (None if not bottleneck else {"stage": bottleneck[0], "p95_minutes": float(bottleneck[1])}),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps(out, indent=2, default=str))
    print(f"\nWrote: {OUT_PATH}")
    return out


if __name__ == "__main__":
    run()
