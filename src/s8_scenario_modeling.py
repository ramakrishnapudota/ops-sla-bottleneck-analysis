from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

import duckdb

import math


def _nan_to_none(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    return v

DB_PATH = "ops_warehouse.duckdb"
OUT_PATH = Path("reports/run_summaries/step8_summary.json")

SQL_FILES = [
    "sql/staging/s8_01_reopen_penalty.sql",
    "sql/mart/s8_01_scenario_results.sql",
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
        "reopen_penalty_rows": con.execute("SELECT COUNT(*) FROM staging.reopen_penalty").fetchone()[0],
        "scenario_results_rows": con.execute("SELECT COUNT(*) FROM mart.scenario_results").fetchone()[0],
    }

    scenarios = con.execute("""
      SELECT *
      FROM mart.scenario_results
      ORDER BY scenario_name
    """).fetchdf().to_dict(orient="records")

    # Lightweight headline extraction
    headline = {row["scenario_name"]: {k: _nan_to_none(v) for k, v in row.items()} for row in scenarios}

    con.close()
    t_end = time.perf_counter()

    out = {
        "step": 8,
        "runtime_seconds": {"by_file": file_timings, "end_to_end": round(t_end - t0, 3)},
        "counts": counts,
        "scenario_results": headline,
        "notes": {
            "eligibility": "S1 applies only to Tier 3 cases with milestone-based stage decomposition available; S2 uses reopen penalty minutes as rework proxy.",
            "sla_threshold_minutes": {"sla_a": 1440, "sla_b": 120},
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps(out, indent=2, default=str))
    print(f"\nWrote: {OUT_PATH}")
    return out


if __name__ == "__main__":
    run()
