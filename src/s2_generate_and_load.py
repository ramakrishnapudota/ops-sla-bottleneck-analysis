from __future__ import annotations

import json
import math
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import CONFIG


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
REPO_ROOT = Path(".")
DB_PATH = REPO_ROOT / "ops_warehouse.duckdb"

OUT_BASE = REPO_ROOT / "data" / "generated"
OUT_CASES = OUT_BASE / "cases"
OUT_EVENTS = OUT_BASE / "events_log"
OUT_STAFF = OUT_BASE / "staffing_schedule"
OUT_CAL = OUT_BASE / "calendar_dim"

RUN_SUMMARY_PATH = REPO_ROOT / "reports" / "run_summaries" / "step2_summary.json"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _ensure_dirs() -> None:
    for p in [OUT_CASES, OUT_EVENTS, OUT_STAFF, OUT_CAL, RUN_SUMMARY_PATH.parent]:
        p.mkdir(parents=True, exist_ok=True)


def _write_partitioned_parquet(df: pd.DataFrame, base_dir: Path, part_col: str, part_value: str, filename: str) -> Path:
    """
    Write a parquet file into hive-style partition dir:
      base_dir/<part_col>=<part_value>/<filename>.parquet
    """
    part_dir = base_dir / f"{part_col}={part_value}"
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / f"{filename}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="zstd")
    return path


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _weekday_weights() -> np.ndarray:
    w = CONFIG.intake.weekday_weights
    weights = np.array([w[i] for i in range(7)], dtype=float)
    weights = weights / weights.sum()
    return weights


def _make_calendar(start_date: str, days: int, tz: str) -> pd.DataFrame:
    """
    calendar_dim is day-grain. Business hours will be applied in SLA logic later,
    but we encode weekends + holidays here.
    """
    start = pd.Timestamp(start_date).tz_localize(tz).normalize()
    dates = pd.date_range(start=start, periods=days, freq="D", tz=tz)

    cal = pd.DataFrame({"cal_date": dates.date})
    cal["dow"] = pd.Series(dates.weekday, dtype="int16")
    cal["is_weekend"] = cal["dow"].isin([5, 6])

    # Holidays within Jul–Dec 2025 (given CONFIG start_date default):
    # - Independence Day (Jul 4)
    # - Labor Day (1st Monday in Sep)
    # - Thanksgiving (4th Thursday in Nov)
    # - Christmas (Dec 25)
    year = pd.Timestamp(start_date).year
    indep = pd.Timestamp(f"{year}-07-04").date()
    christmas = pd.Timestamp(f"{year}-12-25").date()

    # Labor Day: first Monday of September
    sep1 = pd.Timestamp(f"{year}-09-01")
    labor = (sep1 + pd.offsets.Week(weekday=0)).date()

    # Thanksgiving: 4th Thursday of November
    nov1 = pd.Timestamp(f"{year}-11-01")
    first_thu = nov1 + pd.offsets.Week(weekday=3)  # Thu=3
    thanks = (first_thu + pd.offsets.Week(3)).date()  # add 3 weeks

    holiday_set = {indep, labor, thanks, christmas}
    cal["is_holiday"] = cal["cal_date"].isin(holiday_set)
    cal["holiday_name"] = np.where(
        cal["cal_date"].eq(indep),
        "Independence Day",
        np.where(
            cal["cal_date"].eq(labor),
            "Labor Day",
            np.where(
                cal["cal_date"].eq(thanks),
                "Thanksgiving",
                np.where(cal["cal_date"].eq(christmas), "Christmas Day", None),
            ),
        ),
    )
    return cal


def _make_staffing(start_date: str, days: int, tz: str) -> pd.DataFrame:
    """
    staffing_schedule is shift-grain per day: planned agents + effective agents after shrinkage and deterioration.
    """
    start = pd.Timestamp(start_date).tz_localize(tz).normalize()
    dates = pd.date_range(start=start, periods=days, freq="D", tz=tz)

    planned_by_dow = CONFIG.staffing.planned_agents_weekday
    shrink = CONFIG.staffing.shrinkage_rate
    det_days = CONFIG.staffing.deterioration_days
    det_min = CONFIG.staffing.deterioration_multiplier_min

    shifts = CONFIG.staffing.shifts
    day_ratio = CONFIG.staffing.day_shift_ratio
    swing_ratio = 1.0 - day_ratio

    rows = []
    for i, dt in enumerate(dates):
        dow = int(dt.weekday())
        planned_total = int(planned_by_dow[dow])

        # deterioration ramps down over the last det_days
        days_left = (days - 1) - i
        if days_left < det_days:
            # linear ramp from 1.0 down to det_min
            frac = 1.0 - (days_left / max(1, det_days))
            det_mult = 1.0 - frac * (1.0 - det_min)
        else:
            det_mult = 1.0

        for shift_name, start_hr, end_hr in shifts:
            shift_ratio = day_ratio if shift_name == "DAY" else swing_ratio
            planned_shift = int(round(planned_total * shift_ratio))

            effective = int(round(planned_shift * (1.0 - shrink) * det_mult))
            rows.append(
                {
                    "shift_date": dt.date(),
                    "team_tz": tz,
                    "shift_name": shift_name,
                    "shift_start_hour": start_hr,
                    "shift_end_hour": end_hr,
                    "planned_agents": planned_shift,
                    "shrinkage_rate": float(shrink),
                    "deterioration_multiplier": float(det_mult),
                    "effective_agents": effective,
                }
            )

    return pd.DataFrame(rows)


def _sample_case_mix(rng: np.random.Generator, n: int) -> Tuple[np.ndarray, np.ndarray]:
    # case_type
    type_items = list(CONFIG.case_mix.type_weights.items())
    type_vals = np.array([k for k, _ in type_items], dtype=object)
    type_w = np.array([v for _, v in type_items], dtype=float)
    type_w = type_w / type_w.sum()
    case_type = rng.choice(type_vals, size=n, p=type_w)

    # tier
    tier_items = list(CONFIG.case_mix.tier_weights.items())
    tier_vals = np.array([k for k, _ in tier_items], dtype=object)
    tier_w = np.array([v for _, v in tier_items], dtype=float)
    tier_w = tier_w / tier_w.sum()
    tier = rng.choice(tier_vals, size=n, p=tier_w)

    return case_type, tier


def _sample_intake_timestamps(
    rng: np.random.Generator,
    day_start_local: pd.Timestamp,
    n: int,
    tz: str,
) -> pd.DatetimeIndex:
    """
    Intake times skew toward morning and mid-day within business hours.
    Business hours are 08:00–18:00; we sample within that window with a simple beta shape.
    """
    start_hr = CONFIG.business_hours.start_hour
    end_hr = CONFIG.business_hours.end_hour
    span_hours = end_hr - start_hr

    # beta distribution to skew toward earlier hours
    x = rng.beta(2.2, 3.0, size=n)  # 0..1
    minutes = (x * span_hours * 60).astype(int)

    base = day_start_local + pd.Timedelta(hours=start_hr)
    ts = base + pd.to_timedelta(minutes, unit="m")

    # small random seconds jitter
    ts = ts + pd.to_timedelta(rng.integers(0, 60, size=n), unit="s")
    return pd.DatetimeIndex(ts).tz_convert(tz)


def _lognormal_minutes(rng: np.random.Generator, median_min: float, sigma: float, size: int) -> np.ndarray:
    """
    Use median + sigma to parameterize lognormal.
    lognormal median = exp(mu) => mu = log(median)
    """
    mu = math.log(max(1e-6, median_min))
    vals = rng.lognormal(mean=mu, sigma=sigma, size=size)
    return vals


def _inject_messiness(
    rng: np.random.Generator,
    events: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply required messy behaviors:
    - missing event_ts (event_ts NULL; ingestion_ts present)
    - duplicates (exact retries)
    - out-of-order (shuffle within case a bit by event_ts)
    - timezone inconsistencies (some timestamps stored "as if UTC" w/ wrong tz label)
    """
    n = len(events)

    # missing event_ts
    miss_rate = CONFIG.messy.missing_event_ts_rate
    miss_mask = rng.random(n) < miss_rate
    events.loc[miss_mask, "event_ts"] = pd.NaT

    # tz inconsistencies: shift event_ts by treating local as UTC for subset
    tz_rate = CONFIG.messy.tz_inconsistency_rate
    tz_mask = rng.random(n) < tz_rate
    # Only apply where event_ts exists
    tz_mask = tz_mask & events["event_ts"].notna()
    # naive "wrong conversion": drop tz then re-localize as UTC then convert back to primary tz
    # This effectively shifts time and mimics bad storage.
    primary_tz = CONFIG.teams.primary_tz
    bad = events.loc[tz_mask, "event_ts"].dt.tz_convert(primary_tz).dt.tz_localize(None)
    bad = bad.dt.tz_localize("UTC").dt.tz_convert(primary_tz)
    events.loc[tz_mask, "event_ts"] = bad
    events.loc[tz_mask, "event_tz"] = "INCONSISTENT"

    # out-of-order: for subset of cases, swap a couple events by nudging ingestion_ts
    ooo_rate = CONFIG.messy.out_of_order_rate
    if n > 0:
        # mark some rows as late-arriving by increasing ingestion_ts
        ooo_mask = rng.random(n) < ooo_rate
        bump_minutes = rng.integers(10, 240, size=ooo_mask.sum())
        events.loc[ooo_mask, "ingestion_ts"] = events.loc[ooo_mask, "ingestion_ts"] + pd.to_timedelta(bump_minutes, unit="m")
        events.loc[ooo_mask, "is_late_arriving"] = True

    # duplicates: duplicate a subset of rows
    dup_rate = CONFIG.messy.duplicate_event_rate
    dup_mask = rng.random(n) < dup_rate
    dups = events.loc[dup_mask].copy()
    if len(dups) > 0:
        # simulate retry: ingestion comes slightly later, event_id differs
        dups["event_id"] = dups["event_id"] + "_dup"
        dups["ingestion_ts"] = dups["ingestion_ts"] + pd.to_timedelta(rng.integers(1, 30, size=len(dups)), unit="m")
        dups["is_duplicate"] = True
        events = pd.concat([events, dups], ignore_index=True)

    return events


def _build_events_for_cases(
    rng: np.random.Generator,
    case_ids: np.ndarray,
    intake_ts: pd.DatetimeIndex,
    case_type: np.ndarray,
    tier: np.ndarray,
) -> pd.DataFrame:
    """
    Generate an event stream per case reflecting the locked workflow.
    """
    n = len(case_ids)
    primary_tz = CONFIG.teams.primary_tz

    # TRIAGE duration (minutes)
    triage_medians = np.vectorize(CONFIG.stage_times.triage_median_by_tier_min.get)(tier).astype(float)
    triage_min = _lognormal_minutes(rng, median_min=1.0, sigma=1.0, size=n) * 0  # placeholder for shape
    triage_min = np.array([
        _lognormal_minutes(rng, median_min=m, sigma=CONFIG.stage_times.triage_sigma, size=1)[0]
        for m in triage_medians
    ])

    # ASSIGNMENT delay after triage (small)
    assign_min = _lognormal_minutes(rng, median_min=25.0, sigma=0.8, size=n)

    # INVESTIGATION duration
    resolve_medians = np.vectorize(CONFIG.stage_times.resolve_median_by_tier_min.get)(tier).astype(float)
    inv_min = np.array([
        _lognormal_minutes(rng, median_min=m, sigma=CONFIG.stage_times.resolve_sigma, size=1)[0]
        for m in resolve_medians
    ])

    # REVIEW_QA duration
    review_min = _lognormal_minutes(rng, median_min=CONFIG.stage_times.review_qa_median_min, sigma=CONFIG.stage_times.review_qa_sigma, size=n)

    # CUSTOMER_WAIT occurrence + duration
    cw_rate = CONFIG.stage_times.customer_wait_rate
    has_cw = rng.random(n) < cw_rate
    cw_min = _lognormal_minutes(rng, median_min=CONFIG.stage_times.customer_wait_median_min, sigma=CONFIG.stage_times.customer_wait_sigma, size=n)
    cw_min = np.where(has_cw, cw_min, 0.0)

    # ESCALATION occurrence
    esc_rate = CONFIG.messy.escalation_rate
    has_esc = rng.random(n) < esc_rate

    # CANCELLATION (small subset, more likely earlier)
    cancel_rate = 0.012
    is_cancelled = rng.random(n) < cancel_rate

    # Build base timestamps
    intake = pd.DatetimeIndex(intake_ts).tz_convert(primary_tz)
    triage_ts = intake + pd.to_timedelta(triage_min, unit="m")
    assign_ts = triage_ts + pd.to_timedelta(assign_min, unit="m")
    inv_ts = assign_ts + pd.to_timedelta(inv_min, unit="m")

    # Insert CUSTOMER_WAIT mid-investigation when present
    # Simplified: CUSTOMER_WAIT starts halfway through investigation
    cw_start_ts = assign_ts + pd.to_timedelta(inv_min * 0.45, unit="m")
    cw_end_ts = cw_start_ts + pd.to_timedelta(cw_min, unit="m")
    # Resume investigation after CW if present
    inv_end_ts = np.where(has_cw, cw_end_ts + pd.to_timedelta(inv_min * 0.55, unit="m"), inv_ts)
    inv_end_ts = pd.DatetimeIndex(inv_end_ts).tz_convert(primary_tz)

    review_ts = inv_end_ts + pd.to_timedelta(review_min, unit="m")
    resolved_ts = review_ts + pd.to_timedelta(_lognormal_minutes(rng, median_min=20.0, sigma=0.7, size=n), unit="m")

    # If cancelled: cancel around triage/assignment area and do not resolve
    cancel_ts = triage_ts + pd.to_timedelta(_lognormal_minutes(rng, median_min=45.0, sigma=0.8, size=n), unit="m")

    # Event assembly
    rows: List[Dict] = []
    for i in range(n):
        cid = str(case_ids[i])
        # baseline event tz label; we’ll later inject inconsistencies
        tz_label = primary_tz

        # INTAKE
        rows.append({
            "event_id": f"{cid}_001",
            "case_id": cid,
            "status": "INTAKE",
            "event_ts": intake[i],
            "ingestion_ts": intake[i] + pd.Timedelta(minutes=int(rng.integers(0, 15))),
            "event_tz": tz_label,
            "is_late_arriving": False,
            "is_duplicate": False,
        })

        # TRIAGE (sometimes missing milestone later; injected elsewhere)
        rows.append({
            "event_id": f"{cid}_002",
            "case_id": cid,
            "status": "TRIAGE",
            "event_ts": triage_ts[i],
            "ingestion_ts": triage_ts[i] + pd.Timedelta(minutes=int(rng.integers(0, 20))),
            "event_tz": tz_label,
            "is_late_arriving": False,
            "is_duplicate": False,
        })

        # If cancelled, emit CANCELLED and stop
        if bool(is_cancelled[i]):
            rows.append({
                "event_id": f"{cid}_003",
                "case_id": cid,
                "status": "CANCELLED",
                "event_ts": cancel_ts[i],
                "ingestion_ts": cancel_ts[i] + pd.Timedelta(minutes=int(rng.integers(0, 30))),
                "event_tz": tz_label,
                "is_late_arriving": False,
                "is_duplicate": False,
            })
            continue

        # ASSIGNMENT
        rows.append({
            "event_id": f"{cid}_003",
            "case_id": cid,
            "status": "ASSIGNMENT",
            "event_ts": assign_ts[i],
            "ingestion_ts": assign_ts[i] + pd.Timedelta(minutes=int(rng.integers(0, 30))),
            "event_tz": tz_label,
            "is_late_arriving": False,
            "is_duplicate": False,
        })

        # ESCALATED (optional)
        if bool(has_esc[i]):
            esc_ts = assign_ts[i] + pd.Timedelta(minutes=int(rng.integers(30, 240)))
            rows.append({
                "event_id": f"{cid}_004e",
                "case_id": cid,
                "status": "ESCALATED",
                "event_ts": esc_ts,
                "ingestion_ts": esc_ts + pd.Timedelta(minutes=int(rng.integers(0, 45))),
                "event_tz": tz_label,
                "is_late_arriving": False,
                "is_duplicate": False,
            })

        # INVESTIGATION (we log a single milestone here)
        rows.append({
            "event_id": f"{cid}_004",
            "case_id": cid,
            "status": "INVESTIGATION",
            "event_ts": assign_ts[i] + pd.Timedelta(minutes=int(rng.integers(5, 35))),
            "ingestion_ts": assign_ts[i] + pd.Timedelta(minutes=int(rng.integers(10, 60))),
            "event_tz": tz_label,
            "is_late_arriving": False,
            "is_duplicate": False,
        })

        # CUSTOMER_WAIT (optional)
        if bool(has_cw[i]):
            rows.append({
                "event_id": f"{cid}_005",
                "case_id": cid,
                "status": "CUSTOMER_WAIT",
                "event_ts": cw_start_ts[i],
                "ingestion_ts": cw_start_ts[i] + pd.Timedelta(minutes=int(rng.integers(0, 60))),
                "event_tz": tz_label,
                "is_late_arriving": False,
                "is_duplicate": False,
            })
            # return from customer wait back into investigation implicitly; we won’t add extra state

        # REVIEW_QA
        rows.append({
            "event_id": f"{cid}_006",
            "case_id": cid,
            "status": "REVIEW_QA",
            "event_ts": review_ts[i],
            "ingestion_ts": review_ts[i] + pd.Timedelta(minutes=int(rng.integers(0, 45))),
            "event_tz": tz_label,
            "is_late_arriving": False,
            "is_duplicate": False,
        })

        # RESOLVED
        rows.append({
            "event_id": f"{cid}_007",
            "case_id": cid,
            "status": "RESOLVED",
            "event_ts": resolved_ts[i],
            "ingestion_ts": resolved_ts[i] + pd.Timedelta(minutes=int(rng.integers(0, 120))),
            "event_tz": tz_label,
            "is_late_arriving": False,
            "is_duplicate": False,
        })

        # REOPENED (optional) after resolved (tier-based)
        reopen_p = CONFIG.messy.reopen_rate_by_tier[str(tier[i])]
        if rng.random() < reopen_p:
            dmin, dmax = CONFIG.messy.reopen_delay_days_range
            reopen_delay_days = int(rng.integers(dmin, dmax + 1))
            reopen_ts = resolved_ts[i] + pd.Timedelta(days=reopen_delay_days) + pd.Timedelta(minutes=int(rng.integers(30, 600)))
            rows.append({
                "event_id": f"{cid}_008",
                "case_id": cid,
                "status": "REOPENED",
                "event_ts": reopen_ts,
                "ingestion_ts": reopen_ts + pd.Timedelta(minutes=int(rng.integers(0, 180))),
                "event_tz": tz_label,
                "is_late_arriving": False,
                "is_duplicate": False,
            })

    events = pd.DataFrame(rows)

    # Apply messy injections
    events = _inject_messiness(rng, events)

    # Missing milestones: remove TRIAGE or RESOLVED for a subset of cases
    miss_mile = CONFIG.messy.missing_milestone_rate
    if len(events) > 0 and miss_mile > 0:
        # choose cases to affect
        unique_cases = events["case_id"].drop_duplicates().to_numpy()
        m = int(round(len(unique_cases) * miss_mile))
        if m > 0:
            affected = set(rng.choice(unique_cases, size=m, replace=False))
            # for each affected case, randomly drop TRIAGE or RESOLVED
            drop_status = rng.choice(["TRIAGE", "RESOLVED"], size=m, replace=True)
            drop_map = dict(zip(list(affected), list(drop_status)))
            keep_mask = ~events.apply(lambda r: (r["case_id"] in drop_map) and (r["status"] == drop_map[r["case_id"]]), axis=1)
            events = events.loc[keep_mask].copy()

    return events


def generate_and_load(mode: str = "dev") -> Dict:
    """
    # -----------------------------------------------------------------
    # Clean partitioned outputs to prevent multi-run double counting
    # -----------------------------------------------------------------
    import shutil
    for pth in [OUT_CASES, OUT_EVENTS]:
        if pth.exists():
            for child in pth.glob("*"):
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()

    mode:
      - dev  : smaller run for sanity (fast on laptop)
      - full : target-scale run (300k cases ~2M events)
    """
    _ensure_dirs()

    t0 = time.perf_counter()
    rng = _rng(CONFIG.output.random_seed)

    tz = CONFIG.teams.primary_tz
    days = CONFIG.window.days

    # Scale control
    if mode == "full":
        cases_target = CONFIG.scale.cases_target
    else:
        # dev run: still realistic but much smaller
        cases_target = 50_000

    # Calendar + staffing (single files)
    cal = _make_calendar(CONFIG.window.start_date, days, tz)
    staff = _make_staffing(CONFIG.window.start_date, days, tz)

    pq.write_table(pa.Table.from_pandas(cal, preserve_index=False), OUT_CAL / "calendar_dim.parquet", compression="zstd")
    pq.write_table(pa.Table.from_pandas(staff, preserve_index=False), OUT_STAFF / "staffing_schedule.parquet", compression="zstd")

    # Allocate cases across days using weekday weights + EOQ ramp
    start = pd.Timestamp(CONFIG.window.start_date).tz_localize(tz).normalize()
    day_index = pd.date_range(start=start, periods=days, freq="D", tz=tz)

    w = np.array([CONFIG.intake.weekday_weights[int(d.weekday())] for d in day_index], dtype=float)

    # EOQ ramp across last N days
    ramp_days = CONFIG.intake.eoq_ramp_days
    max_mult = CONFIG.intake.eoq_multiplier_max
    mult = np.ones(days, dtype=float)
    if ramp_days > 0:
        for i in range(days - ramp_days, days):
            frac = (i - (days - ramp_days)) / max(1, ramp_days - 1)
            mult[i] = 1.0 + frac * (max_mult - 1.0)
    w = w * mult
    w = w / w.sum()

    # Determine counts per day (multinomial)
    cases_per_day = rng.multinomial(cases_target, w)

    # ID sequencing
    # stable, sortable IDs: C000000001 ...
    case_id_start = 1
    case_rows_total = 0
    event_rows_total = 0
    case_files = 0
    event_files = 0

    for di, dt in enumerate(day_index):
        n = int(cases_per_day[di])
        if n == 0:
            continue

        day_str = str(dt.date())
        day_start = dt

        case_ids = np.array([f"C{case_id_start + i:09d}" for i in range(n)], dtype=object)
        case_id_start += n

        case_type, tier = _sample_case_mix(rng, n)
        intake_ts = _sample_intake_timestamps(rng, day_start_local=day_start, n=n, tz=tz)

        cases_df = pd.DataFrame(
            {
                "case_id": case_ids,
                "intake_ts": intake_ts,
                "case_type": case_type,
                "tier": tier,
                "team_tz": tz,
            }
        )

        events_df = _build_events_for_cases(rng, case_ids, intake_ts, case_type, tier)

        # Write partitioned parquet by intake_date
        _write_partitioned_parquet(
            cases_df,
            base_dir=OUT_CASES,
            part_col="intake_date",
            part_value=day_str,
            filename=f"cases_{day_str}_n{n}",
        )
        case_files += 1

        # Events partitioned by intake_date (same as case)
        _write_partitioned_parquet(
            events_df,
            base_dir=OUT_EVENTS,
            part_col="intake_date",
            part_value=day_str,
            filename=f"events_{day_str}_rows{len(events_df)}",
        )
        event_files += 1

        case_rows_total += len(cases_df)
        event_rows_total += len(events_df)

    t_gen = time.perf_counter()

    # Load into DuckDB raw schema
    con = duckdb.connect(str(DB_PATH))

    # Calendar + staffing
    con.execute("CREATE OR REPLACE TABLE raw.calendar_dim AS SELECT * FROM read_parquet(?);", [str(OUT_CAL / "calendar_dim.parquet")])
    con.execute("CREATE OR REPLACE TABLE raw.staffing_schedule AS SELECT * FROM read_parquet(?);", [str(OUT_STAFF / "staffing_schedule.parquet")])

    # Partitioned datasets (hive_partitioning picks up intake_date)
    con.execute(
        "CREATE OR REPLACE TABLE raw.cases AS "
        "SELECT * FROM read_parquet(?, hive_partitioning=1);",
        [str(OUT_CASES / "**" / "*.parquet")],
    )
    con.execute(
        "CREATE OR REPLACE TABLE raw.events_log AS "
        "SELECT * FROM read_parquet(?, hive_partitioning=1);",
        [str(OUT_EVENTS / "**" / "*.parquet")],
    )

    # Basic stats for summary
    counts = {
        "raw_calendar_dim": con.execute("SELECT COUNT(*) FROM raw.calendar_dim").fetchone()[0],
        "raw_staffing_schedule": con.execute("SELECT COUNT(*) FROM raw.staffing_schedule").fetchone()[0],
        "raw_cases": con.execute("SELECT COUNT(*) FROM raw.cases").fetchone()[0],
        "raw_events_log": con.execute("SELECT COUNT(*) FROM raw.events_log").fetchone()[0],
    }

    # Quick messy-data rates (approx)
    dq = {
        "events_missing_event_ts_pct": float(con.execute(
            "SELECT 100.0 * AVG(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END) FROM raw.events_log"
        ).fetchone()[0]),
        "events_duplicate_flag_pct": float(con.execute(
            "SELECT 100.0 * AVG(CASE WHEN is_duplicate THEN 1 ELSE 0 END) FROM raw.events_log"
        ).fetchone()[0]),
        "events_late_arriving_flag_pct": float(con.execute(
            "SELECT 100.0 * AVG(CASE WHEN is_late_arriving THEN 1 ELSE 0 END) FROM raw.events_log"
        ).fetchone()[0]),
        "events_tz_inconsistent_pct": float(con.execute(
            "SELECT 100.0 * AVG(CASE WHEN event_tz = 'INCONSISTENT' THEN 1 ELSE 0 END) FROM raw.events_log"
        ).fetchone()[0]),
    }

    con.close()

    t_load = time.perf_counter()

    summary = {
        "step": 2,
        "mode": mode,
        "config": {
            "window_days": CONFIG.window.days,
            "start_date": CONFIG.window.start_date,
            "cases_target": cases_target,
        },
        "generated": {
            "case_rows_expected": int(case_rows_total),
            "event_rows_expected": int(event_rows_total),
            "case_part_files": int(case_files),
            "event_part_files": int(event_files),
            "calendar_rows": int(len(cal)),
            "staffing_rows": int(len(staff)),
        },
        "loaded_counts": counts,
        "data_quality_pct": dq,
        "runtime_seconds": {
            "generate_total": round(t_gen - t0, 3),
            "load_total": round(t_load - t_gen, 3),
            "end_to_end": round(t_load - t0, 3),
        },
        "paths": {
            "cases_dir": str(OUT_CASES),
            "events_dir": str(OUT_EVENTS),
            "db_path": str(DB_PATH),
        },
    }

    RUN_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Step 2: Generate synthetic data + load raw DuckDB tables.")
    parser.add_argument("--mode", choices=["dev", "full"], default="dev", help="dev=fast sanity run, full=target scale")
    args = parser.parse_args()

    s = generate_and_load(mode=args.mode)
    print(json.dumps(s, indent=2))
    print(f"\nWrote run summary: {RUN_SUMMARY_PATH}")
