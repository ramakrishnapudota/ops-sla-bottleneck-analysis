from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

import duckdb

DB_PATH = "ops_warehouse.duckdb"
OUT_PATH = Path("reports/run_summaries/step6_summary.json")

SQL_FILES = [
    "sql/mart/s6_01_sla_daily.sql",
    "sql/mart/s6_02_sla_by_tier_case_type.sql",
    "sql/mart/s6_03_staffing_daily.sql",
    "sql/mart/s6_04_backlog_daily_proxy.sql",
    "sql/mart/s6_05_congestion_daily.sql",
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
        "mart_sla_daily": con.execute("SELECT COUNT(*) FROM mart.sla_daily").fetchone()[0],
        "mart_sla_by_tier_case_type": con.execute("SELECT COUNT(*) FROM mart.sla_by_tier_case_type").fetchone()[0],
        "mart_staffing_daily": con.execute("SELECT COUNT(*) FROM mart.staffing_daily").fetchone()[0],
        "mart_backlog_daily_proxy": con.execute("SELECT COUNT(*) FROM mart.backlog_daily_proxy").fetchone()[0],
        "mart_congestion_daily": con.execute("SELECT COUNT(*) FROM mart.congestion_daily").fetchone()[0],
    }

    # Key headline metrics to log
    headline = con.execute("""
        SELECT
          ROUND(AVG(sla_b_breach_pct), 3) AS avg_sla_b_breach_pct_daily,
          ROUND(AVG(sla_a_breach_pct_including_cw), 3) AS avg_sla_a_breach_pct_inc_cw_daily,
          ROUND(AVG(sla_a_breach_pct_paused_cw), 3) AS avg_sla_a_breach_pct_pause_cw_daily
        FROM mart.sla_daily;
    """).fetchdf().to_dict(orient="records")[0]

    # Congestion summary
    cong = con.execute("""
        SELECT
          ROUND(AVG(congestion_index), 4) AS avg_congestion_index,
          quantile_cont(congestion_index, 0.50) AS cong_p50,
          quantile_cont(congestion_index, 0.90) AS cong_p90,
          quantile_cont(congestion_index, 0.95) AS cong_p95
        FROM mart.congestion_daily
        WHERE congestion_index IS NOT NULL;
    """).fetchdf().to_dict(orient="records")[0]

    con.close()
    t_end = time.perf_counter()

    summary = {
        "step": 6,
        "runtime_seconds": {"by_file": file_timings, "end_to_end": round(t_end - t0, 3)},
        "counts": counts,
        "headline_daily_avgs_pct": headline,
        "congestion_summary": {k: (None if v is None else float(v)) for k, v in cong.items()},
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"\nWrote: {OUT_PATH}")
    return summary


if __name__ == "__main__":
    run()
