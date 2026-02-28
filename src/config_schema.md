# Config Schema — Synthetic Ops Warehouse Simulation

This document explains why each config group exists and how it maps to business context.
It is the human-readable spec that Security Ops leadership could review.

---

## SimulationWindow
- **days (180)**: 6 months scope (~2 quarters)
- **start_date**: stable anchor date for reproducibility

## ScaleTargets
- **cases_target (~300k)** and **events_target (~2M)**: matches portfolio scale requirement
- **avg_events_per_case**: target guidance, not a hard promise

## Teams
- **primary_tz**: team-local timezone used for business-hour calculations
- **secondary_tzs**: used only to inject realistic timezone mixture

## States
- Locked workflow:
  INTAKE → TRIAGE → ASSIGNMENT → INVESTIGATION → CUSTOMER_WAIT → REVIEW_QA → RESOLVED
- Side states:
  REOPENED, ESCALATED, CANCELLED

## BusinessHours
- Mon–Fri 08:00–18:00 local time
- calendar_dim will encode weekends + holidays
- holidays_per_6mo is planning guidance; actual dates will be explicit in calendar_dim

## SLADefinitions
- SLA A: First Resolution SLA = 24 business hours (INTAKE → first RESOLVED)
  - Variant 1 counts CUSTOMER_WAIT
  - Variant 2 pauses during CUSTOMER_WAIT
- SLA B: First Touch SLA = 2 business hours (INTAKE → first TRIAGE)

## CaseMix
- case_type weights:
  - VENDOR_ASSESSMENT (45%), ACCESS_REVIEW (35%), POLICY_EXCEPTION (20%)
- tier weights:
  - TIER_1 (55%), TIER_2 (35%), TIER_3 (10%)
Tier influences cycle time, reopen probability.

## IntakePattern
- weekday volume skew: heavier Mon/Tue, lower Fri, minimal weekend
- end-of-quarter ramp: last ~30 days + up to 20% intake increase

## StaffingModel
- planned agents by weekday + shift structure
- shrinkage applied to planned coverage
- coverage deterioration in last 60 days (down to -15%) to drive congestion + breaches
- day vs swing shift ratio controls coverage distribution across the workday

## CongestionEffects
- daily load index with noise + backlog carryover
- durations inflate when load_index > 1 using alpha elasticity

## StageTimeDistributions
All baseline durations are in minutes (pre business-time conversion).
- TRIAGE latency: tiered medians
- RESOLUTION cycle time: tiered medians (TIER_3 close to SLA threshold when congested)
- CUSTOMER_WAIT: applied to subset (rate) with its own distribution
- REVIEW_QA: final review step duration

## MessyDataRates
Must-simulate data issues:
- missing event timestamps
- missing milestone events (some cases missing TRIAGE/RESOLVED)
- duplicate and out-of-order events
- timezone inconsistencies
Ops realism:
- reopen rates vary by tier
- escalation rate applied to small subset

## OutputControls
- write_format = parquet by default (scale-friendly)
- partition_granularity = day (supports incremental refresh simulation)
- random_seed fixed for reproducibility

---
