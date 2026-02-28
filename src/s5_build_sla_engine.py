from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

import duckdb

DB_PATH = "ops_warehouse.duckdb"
OUT_PATH = Path("reports/run_summaries/step5_summary.json")

SQL_FILES = [
    "sql/staging/s5_01_business_minutes_dim.sql",
    "sql/staging/s5_02_case_sla_metrics.sql",
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
        "business_minutes_dim": con.execute("SELECT COUNT(*) FROM staging.business_minutes_dim").fetchone()[0],
        "case_sla_metrics": con.execute("SELECT COUNT(*) FROM staging.case_sla_metrics").fetchone()[0],
    }

    # Core outcomes
    breach_rates = con.execute("""
        SELECT
          ROUND(100.0 * AVG(CASE WHEN sla_b_breached THEN 1 ELSE 0 END), 3) AS sla_b_breach_pct,
          ROUND(100.0 * AVG(CASE WHEN sla_a_breached_including_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_including_cw,
          ROUND(100.0 * AVG(CASE WHEN sla_a_breached_paused_cw THEN 1 ELSE 0 END), 3) AS sla_a_breach_pct_paused_cw
        FROM staging.case_sla_metrics
        WHERE resolved_ts IS NOT NULL;  -- only evaluate SLA A on resolved cases
    """).fetchdf().to_dict(orient="records")[0]

    pct_triage_invalid = con.execute("""
        SELECT ROUND(100.0 * AVG(CASE WHEN is_triage_before_intake_for_sla THEN 1 ELSE 0 END), 4)
        FROM staging.case_sla_metrics
        WHERE triage_ts IS NOT NULL;
    """).fetchone()[0]

    # Percentiles for key durations (minutes)
    pctiles = con.execute("""
        SELECT
          quantile_cont(first_touch_business_minutes, 0.50) AS ft_p50_min,
          quantile_cont(first_touch_business_minutes, 0.90) AS ft_p90_min,
          quantile_cont(first_touch_business_minutes, 0.95) AS ft_p95_min,

          quantile_cont(first_resolution_business_minutes_including_cw, 0.50) AS fr_p50_min_inc,
          quantile_cont(first_resolution_business_minutes_including_cw, 0.90) AS fr_p90_min_inc,
          quantile_cont(first_resolution_business_minutes_including_cw, 0.95) AS fr_p95_min_inc,

          quantile_cont(first_resolution_business_minutes_paused_cw, 0.50) AS fr_p50_min_pause,
          quantile_cont(first_resolution_business_minutes_paused_cw, 0.90) AS fr_p90_min_pause,
          quantile_cont(first_resolution_business_minutes_paused_cw, 0.95) AS fr_p95_min_pause
        FROM staging.case_sla_metrics
        WHERE resolved_ts IS NOT NULL;
    """).fetchdf().to_dict(orient="records")[0]

    con.close()
    t_end = time.perf_counter()

    summary = {
        "step": 5,
        "runtime_seconds": {"by_file": file_timings, "end_to_end": round(t_end - t0, 3)},
        "counts": counts,
        "breach_rates_pct": breach_rates,
        "triage_before_intake_pct_for_sla": float(pct_triage_invalid),
        "percentiles_minutes": {k: (None if v is None else float(v)) for k, v in pctiles.items()},
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"\nWrote: {OUT_PATH}")
    return summary


if __name__ == "__main__":
    run()
