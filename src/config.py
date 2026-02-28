from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional


# =============================================================================
# Simulation Config (Authoritative Source of Truth)
# B2B SaaS Security & Compliance Platform — Ops Bottleneck & SLA Breach Analysis
# Timezone note:
# - We will generate event timestamps in a "team local" timezone by default,
#   but we will intentionally inject timezone inconsistencies per messy-data reqs.
# =============================================================================


@dataclass(frozen=True)
class SimulationWindow:
    days: int = 180  # ~6 months
    start_date: str = "2025-07-01"  # stable, in-project anchor (can change later if needed)


@dataclass(frozen=True)
class ScaleTargets:
    cases_target: int = 300_000
    avg_events_per_case: float = 6.7  # goal: ~2M events
    events_target: int = 2_000_000


@dataclass(frozen=True)
class Teams:
    # Primary operating timezone for the Security Ops team (used for business hours calc)
    primary_tz: str = "America/Los_Angeles"
    # We'll simulate a smaller secondary timezone footprint to create realistic tz mix
    secondary_tzs: Tuple[str, ...] = ("America/Denver", "America/New_York")


@dataclass(frozen=True)
class States:
    # Locked workflow stages
    main_flow: Tuple[str, ...] = (
        "INTAKE",
        "TRIAGE",
        "ASSIGNMENT",
        "INVESTIGATION",
        "CUSTOMER_WAIT",  # optional / pausable variant
        "REVIEW_QA",
        "RESOLVED",
    )
    side_states: Tuple[str, ...] = ("REOPENED", "ESCALATED", "CANCELLED")


@dataclass(frozen=True)
class BusinessHours:
    # Locked business hours: Mon–Fri 08:00–18:00 local team tz
    start_hour: int = 8
    end_hour: int = 18
    workdays: Tuple[int, ...] = (0, 1, 2, 3, 4)  # Mon=0 ... Sun=6 (Python convention)
    # Holidays will be generated in calendar_dim; we seed a realistic list later in SQL.
    # For now: placeholder count for simulation planning.
    holidays_per_6mo: int = 4


@dataclass(frozen=True)
class SLADefinitions:
    # Locked SLAs (business hours)
    first_resolution_hours: float = 24.0  # INTAKE -> first RESOLVED
    first_touch_hours: float = 2.0        # INTAKE -> first TRIAGE


@dataclass(frozen=True)
class CaseMix:
    # Case types represent common security review request types
    # Use weights to simulate volume mix
    type_weights: Dict[str, float] = None  # set in __post_init__ below

    # Case tier (complexity / customer segment) influences cycle time + reopen rates
    tier_weights: Dict[str, float] = None

    def __post_init__(self):
        object.__setattr__(
            self,
            "type_weights",
            {
                "VENDOR_ASSESSMENT": 0.45,
                "ACCESS_REVIEW": 0.35,
                "POLICY_EXCEPTION": 0.20,
            },
        )
        object.__setattr__(
            self,
            "tier_weights",
            {
                "TIER_1": 0.55,  # smaller / simpler
                "TIER_2": 0.35,
                "TIER_3": 0.10,  # largest / most complex
            },
        )


@dataclass(frozen=True)
class IntakePattern:
    # Realistic weekday skew: Mon/Tue heavier, Fri lighter; weekend very low (but not zero)
    # We will generate daily volume weights and then allocate cases across days
    weekday_weights: Dict[int, float] = None  # Mon=0 ... Sun=6

    # Mild seasonality (end-of-quarter compliance pushes)
    # We’ll apply a multiplier ramp over the final ~30 days
    eoq_ramp_days: int = 30
    eoq_multiplier_max: float = 1.20  # up to +20% intake at peak

    def __post_init__(self):
        object.__setattr__(
            self,
            "weekday_weights",
            {
                0: 1.25,  # Mon
                1: 1.15,  # Tue
                2: 1.05,  # Wed
                3: 1.00,  # Thu
                4: 0.85,  # Fri
                5: 0.15,  # Sat
                6: 0.10,  # Sun
            },
        )


@dataclass(frozen=True)
class StaffingModel:
    """
    Staffing schedule is planned coverage by team/shift/date with shrinkage.
    We simulate:
    - Base FTE coverage by weekday
    - Shifts (day / swing)
    - Shrinkage (PTO, meetings, training)
    - Under-coverage in later months to drive congestion + SLA breaches
    """

    # Shifts in local team timezone
    shifts: Tuple[Tuple[str, int, int], ...] = (
        ("DAY", 8, 16),    # 08:00–16:00
        ("SWING", 10, 18), # 10:00–18:00
    )

    # Planned agents (pre-shrinkage) by weekday for the overall team
    # We'll later split by shift with a ratio.
    planned_agents_weekday: Dict[int, int] = None  # Mon=0 ... Sun=6

    # Shrinkage rate (percentage of time lost)
    shrinkage_rate: float = 0.22  # 22% typical-ish for ops teams

    # Coverage deterioration: reduce effective staffing over last N days
    deterioration_days: int = 60
    deterioration_multiplier_min: float = 0.85  # down to -15%

    # Shift allocation ratio (DAY vs SWING)
    day_shift_ratio: float = 0.65

    def __post_init__(self):
        object.__setattr__(
            self,
            "planned_agents_weekday",
            {
                0: 38,  # Mon
                1: 36,  # Tue
                2: 34,  # Wed
                3: 34,  # Thu
                4: 30,  # Fri
                5: 6,   # Sat limited coverage
                6: 4,   # Sun limited coverage
            },
        )


@dataclass(frozen=True)
class CongestionEffects:
    """
    We want congestion effects driven by staffing vs workload.
    We will model a daily "load index" and inflate stage durations when load > capacity.
    """

    # Baseline load variability
    daily_noise_sigma: float = 0.10  # ±10%ish random day-to-day

    # How strongly congestion inflates durations
    # (durations * (1 + alpha * max(0, load_index - 1)))
    alpha: float = 1.35

    # Backlog carryover strength (how much yesterday's backlog impacts today)
    backlog_carryover: float = 0.65


@dataclass(frozen=True)
class StageTimeDistributions:
    """
    Stage durations in minutes (pre business-time conversion).
    We'll use lognormal-ish parameters later in generator.
    Parameters are expressed as median + sigma (log-space proxy) per tier.
    """

    # TRIAGE latency from INTAKE (minutes) baseline (before congestion)
    triage_median_by_tier_min: Dict[str, float] = None
    triage_sigma: float = 0.85

    # Resolution cycle time from ASSIGNMENT/INVESTIGATION -> RESOLVED (minutes)
    resolve_median_by_tier_min: Dict[str, float] = None
    resolve_sigma: float = 0.95

    # Optional CUSTOMER_WAIT durations (minutes) for subset of cases
    customer_wait_median_min: float = 720.0  # 12 hours median waiting
    customer_wait_sigma: float = 1.10
    customer_wait_rate: float = 0.28  # % of cases that enter CUSTOMER_WAIT at least once

    # REVIEW_QA duration (minutes)
    review_qa_median_min: float = 90.0
    review_qa_sigma: float = 0.70

    def __post_init__(self):
        object.__setattr__(
            self,
            "triage_median_by_tier_min",
            {
                "TIER_1": 35,
                "TIER_2": 55,
                "TIER_3": 85,
            },
        )
        object.__setattr__(
            self,
            "resolve_median_by_tier_min",
            {
                "TIER_1": 420,   # 7h median
                "TIER_2": 720,   # 12h median
                "TIER_3": 1320,  # 22h median (pushes breaches under congestion)
            },
        )


@dataclass(frozen=True)
class MessyDataRates:
    # Must-simulate messy data behaviors
    missing_event_ts_rate: float = 0.018  # event_ts missing but ingestion_ts present
    missing_milestone_rate: float = 0.012 # missing triage/resolved for some cases
    duplicate_event_rate: float = 0.020   # retry logging duplicates
    out_of_order_rate: float = 0.015      # late arriving events / sequence disorder

    # Timezone inconsistencies:
    # portion of events where local time stored as UTC (or tz field wrong)
    tz_inconsistency_rate: float = 0.030

    # Reopen behavior
    reopen_rate_by_tier: Dict[str, float] = None
    reopen_delay_days_range: Tuple[int, int] = (1, 7)

    # Escalations
    escalation_rate: float = 0.035

    def __post_init__(self):
        object.__setattr__(
            self,
            "reopen_rate_by_tier",
            {
                "TIER_1": 0.035,
                "TIER_2": 0.055,
                "TIER_3": 0.085,
            },
        )


@dataclass(frozen=True)
class OutputControls:
    # We will generate files in partitions to avoid memory spikes.
    # Generator will write partitioned parquet or csv (decision later).
    write_format: str = "parquet"  # parquet preferred for scale; Tableau extract will come from mart exports
    partition_granularity: str = "day"  # day partitions for intake date
    random_seed: int = 42


@dataclass(frozen=True)
class Config:
    window: SimulationWindow = SimulationWindow()
    scale: ScaleTargets = ScaleTargets()
    teams: Teams = Teams()
    states: States = States()
    business_hours: BusinessHours = BusinessHours()
    sla: SLADefinitions = SLADefinitions()

    case_mix: CaseMix = CaseMix()
    intake: IntakePattern = IntakePattern()
    staffing: StaffingModel = StaffingModel()
    congestion: CongestionEffects = CongestionEffects()
    stage_times: StageTimeDistributions = StageTimeDistributions()
    messy: MessyDataRates = MessyDataRates()
    output: OutputControls = OutputControls()


CONFIG = Config()
