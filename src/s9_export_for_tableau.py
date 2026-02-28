from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

DB_PATH = "ops_warehouse.duckdb"
EXPORT_DIR = Path("data/exports")
OUT_PATH = Path("reports/run_summaries/step9_export_summary.json")

TABLES: List[Tuple[str, str]] = [
    ("mart.sla_daily", "mart_sla_daily.csv"),
    ("mart.sla_by_tier_case_type", "mart_sla_by_tier_case_type.csv"),
    ("mart.staffing_daily", "mart_staffing_daily.csv"),
    ("mart.backlog_daily_proxy", "mart_backlog_daily_proxy.csv"),
    ("mart.congestion_daily", "mart_congestion_daily.csv"),
    ("mart.driver_stage_durations", "mart_driver_stage_durations.csv"),
    ("mart.driver_reopen_impact", "mart_driver_reopen_impact.csv"),
    ("mart.scenario_results", "mart_scenario_results.csv"),
]


def export_table(con: duckdb.DuckDBPyConnection, table: str, filename: str) -> Dict:
    t0 = time.perf_counter()
    df = con.execute(f"SELECT * FROM {table}").df()
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / filename
    df.to_csv(out_path, index=False)
    t1 = time.perf_counter()
    return {
        "table": table,
        "file": str(out_path),
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "seconds": round(t1 - t0, 3),
    }


def run() -> Dict:
    t0 = time.perf_counter()
    con = duckdb.connect(DB_PATH)

    exports = []
    for table, fname in TABLES:
        exports.append(export_table(con, table, fname))

    con.close()
    t1 = time.perf_counter()

    out = {
        "step": 9,
        "runtime_seconds": round(t1 - t0, 3),
        "exports": exports,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"\nWrote: {OUT_PATH}")
    return out


if __name__ == "__main__":
    run()
