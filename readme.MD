# Operational Bottleneck & SLA Breach Analysis  
**B2B SaaS Security Operations | 6-Month Operational Simulation (~300K Cases, ~1.9M Events)**  

**Live Interactive Dashboard:**  
https://public.tableau.com/app/profile/rama.krishna.pudota/viz/OPSSLA/OperationalBottleneckSLABreachAnalysis  

---

## Dashboard Overview  

![Dashboard Overview](assets/dashboard_full.png)

---

## Overview  

This project simulates internal Operations Analytics work at a B2B SaaS Security & Compliance company.

Over a six-month operational window, approximately **300,000 security review cases** and **1.9 million event records** were modeled in a local DuckDB warehouse. A business-hour SLA engine (Mon–Fri, 08:00–18:00, holiday-adjusted) was implemented to evaluate performance, isolate bottlenecks, and quantify operational improvement levers.

Two SLAs were analyzed:

- **SLA B (First Touch):** 2 business hours  
- **SLA A (First Resolution):** 24 business hours  

All SLA logic, bottleneck analysis, and scenario modeling were implemented in SQL. Tableau is used strictly for visualization of curated mart tables.

---

## Business Context  

This project simulates an internal Security Operations analytics engagement.

**Stakeholders**

- VP, Security Operations (executive sponsor)  
- Director, Security Operations (performance owner)  
- Workforce Management  

**Problem Statement**

SLA breaches increased over two consecutive quarters. Leadership required:

- Root-cause diagnosis  
- Bottleneck identification  
- Quantified improvement levers  
- Policy sensitivity analysis  
- Executive monitoring framework  

The objective is operational decision support, not visualization alone.

---

## Key Findings  

### Tier Is the Primary Breach Driver  

- **Tier 1 SLA A breach rate:** ~3–4%  
- **Tier 2 SLA A breach rate:** ~7–8%  
- **Tier 3 SLA A breach rate:** ~16–18%  

Tier 3 exhibits 2–3× higher breach rates than Tier 2.  
Tier complexity is the dominant driver of SLA risk.

![Tier Heatmap](assets/heatmap.png)

---

### Tail Latency Drives SLA Exposure  

Tier 3 **p95 resolution time:** ~2,642 business minutes.  

Median performance remains stable, but breach exposure concentrates in extreme tail cases.

![Tier 3 Bottleneck](assets/bottleneck_tier3.png)

---

## Bottleneck Identified  

Stage-level cycle time decomposition shows **Investigation → Review/QA** dominates p95 resolution time.

This downstream constraint is the primary operational bottleneck driving SLA A breaches.

---

## Policy Sensitivity (Customer Wait Treatment)  

SLA A breach rates vary materially by policy definition:

- **Including Customer Wait:** ~6.6%  
- **Paused During Customer Wait:** ~2.7%  

Reporting definition alone shifts perceived performance by ~4 percentage points.

---

## Scenario Modeling — Quantified Operational Impact  

### Scenario 1: Reduce Tier 3 Investigation → Review/QA Duration by 20%  

- Eligible cases: 8,243  
- SLA A breaches avoided: 1,003  
- Operational hours saved: ~28,199 hours  

### Scenario 2: Reduce Reopens by 25%  

- Eligible reopened cases: 13,791  
- Rework hours saved: ~33,301 hours  

### Combined Impact  

- **Total operational hours saved:** ~61,500 hours  

Reopen reduction lowers workload but does not directly reduce first-resolution SLA breach rates.

![Scenario Impact](assets/scenario_impact.png)

---

# Architecture & Data Modeling

## Technology Stack  

- DuckDB (local warehouse simulation)  
- Python 3.11  
- SQL (PostgreSQL-style syntax)  
- Pandas / NumPy  
- Tableau Public  

---

## Data Model  

### Raw Layer  
- `cases` (~300K rows)  
- `events_log` (~1.9M rows)  
- `staffing_schedule`  
- `calendar_dim`  

### Staging Layer  
- Canonicalized event timestamps  
- Business-minute time spine  
- Milestone derivation  
- SLA metric computation  

### Mart Layer  
- `sla_daily`  
- `sla_by_tier_case_type`  
- `driver_stage_durations`  
- `congestion_daily`  
- `scenario_results`  

All business logic resides in SQL. Tableau consumes curated mart tables only.

---

## Event Normalization & Data Integrity Controls  

Raw event data intentionally modeled operational imperfections:

- Missing timestamps  
- Late-arriving ingestion records  
- Duplicate retry logging  
- Out-of-order transitions  
- Timezone inconsistencies  
- Reopen-after-resolution patterns  

Staging layer normalization:

- Deduplicated events using `(case_id, status, canonical_ts)` with deterministic tie-breaking  
- Reconciled `event_ts` and `ingestion_ts` to construct canonical time  
- Derived milestone timestamps using first-valid-after-intake selection  
- Flagged invalid temporal sequences (e.g., TRIAGE before INTAKE)  
- Explicit separation of terminal states (RESOLVED vs CANCELLED)  

All normalization implemented in SQL prior to SLA computation.

---

## Business-Time Computation Methodology  

SLA measurement used a minute-level business time spine:

- Generated business-minute dimension excluding weekends and holidays  
- ASOF joins mapped milestone timestamps to business-minute indices  
- Business-minute deltas computed via index arithmetic  
- Dual SLA A variants supported:
  - Including CUSTOMER_WAIT  
  - Paused during CUSTOMER_WAIT  

This avoids naive timestamp subtraction and ensures accurate business-hour compliance.

---

## Stage-Level Cycle Time Decomposition  

Resolution time decomposed into business-minute stage durations:

- Intake → Triage  
- Triage → Assignment  
- Assignment → Investigation  
- Investigation → Review/QA  
- Review/QA → Resolved  

Percentiles (p50/p90/p95) computed per tier to isolate structural bottlenecks.

Tail analysis identified Investigation → Review/QA as the dominant Tier 3 constraint.

---

## Counterfactual Modeling Framework  

Improvement scenarios modeled deterministically at case level:

- Process lever reduced Investigation → Review/QA duration by 20% for eligible Tier 3 cases  
- Reopen reduction estimated rework using tier-level stage duration medians  
- Combined impact aggregated hours saved and breach deltas  

Raw dataset remained unchanged; modeled intervention effects calculated in isolated analytical layers.

---

## Data Scale & Performance  

- ~300,000 cases  
- ~1.9 million event records  
- ~180 business days  
- ~75,000 business-minute rows  
- End-to-end SLA engine runtime: ~1.4 seconds (local DuckDB)  

Executed locally on a MacBook Air, simulating warehouse-style analytics workflows.

---

## Operational Monitoring Readiness  

Curated marts structured for recurring monitoring:

- Daily SLA breach tracking  
- Tail percentile volatility analysis  
- Tier-level risk segmentation  
- Congestion index monitoring  
- Scenario impact roll-ups  

Supports scheduled refresh without embedding business logic in Tableau.

---

## Operating Principles  

- All business logic implemented in SQL  
- Raw → Staging → Mart separation  
- Deterministic, reproducible pipeline  
- No logic embedded in visualization layer  
- Generated artifacts excluded from version control  

Repository reflects warehouse-style analytics discipline.

---

## Dashboard Components  

- SLA Breach Trend (Daily)  
- Resolution Tail Percentiles (P50 / P90 / P95)  
- Tier × Case Type Risk Heatmap  
- Tier 3 Stage Decomposition  
- Scenario Impact Quantification  

Executive questions addressed:

1. Are we breaching?  
2. Where is risk concentrated?  
3. Which stage drives tail latency?  
4. What measurable improvement is achievable?  

---

## Limitations & Assumptions  

- Synthetic dataset with realistic distributional assumptions  
- Congestion modeling did not materially lift SLA performance; breach risk primarily tier- and process-driven  
- Reopen penalty estimated using tier-level stage proxies  
- Fixed business-hour window; no multi-timezone routing modeled  

Assumptions documented to preserve interpretability.

---

## Reproducibility  

```bash
python -m src.s2_generate_and_load --mode full
python -m src.s3_raw_qa
python -m src.s4_build_staging
python -m src.s5_build_sla_engine
python -m src.s6_build_marts
python -m src.s7_driver_analysis
python -m src.s8_scenario_modeling
python -m src.s9_export_for_tableau
