# Impact Log — Operational Bottleneck & SLA Breach Analysis

Owner: Rama Krishna Pudota
Company context: B2B SaaS Security & Compliance (Simulated)
Local warehouse: DuckDB (ops_warehouse.duckdb)
Refresh cadence: Simulated daily pipeline (batch)

---

## Metric Conventions (used throughout)
- **Rows processed**: counts by table (raw/staging/mart)
- **Runtime**: wall-clock seconds (per script/query + total)
- **SLA Breach Rates**:
  - SLA A: First Resolution (24 business hours) — **variant 1 includes CUSTOMER_WAIT**
  - SLA A: First Resolution — **variant 2 pauses during CUSTOMER_WAIT**
  - SLA B: First Touch (2 business hours) — INTAKE → TRIAGE
- **Percentiles**: p50 / p90 / p95 for cycle times & stage times (business-time where applicable)
- **Data quality**: missing timestamps %, missing milestones %, dup rate %, out-of-order rate %
- **Operational metrics** (later): backlog proxy, WIP, throughput, reopens rate, escalations rate
- **Scenario outcomes** (later): estimated breach reduction %, hours saved, capacity delta

---

## Step Entries

### Step 0 — Project bootstrap & environment sanity
**Date:** <YYYY-MM-DD>
**What changed:** Repo structure, venv, dependencies, DuckDB placeholder schemas.

**Artifacts created/updated:**
- ops_warehouse.duckdb (schemas: raw/staging/mart)
- requirements.txt
- reports/impact_log.md

**Runtime / rows (if applicable):**
- Rows loaded: N/A
- Runtime: N/A

**Risks / notes:**
- <e.g., interpreter mismatch fixed, brew python pinned, etc.>

---

### Step 1 — Simulation parameter config (planned)
**Date:** <YYYY-MM-DD>
**What changed:**
- <to be filled>

**Key metrics:**
- <to be filled>


---

### Step 1 — Simulation parameter config (authoritative source of truth)
**Date:** 2025-10-03

**What changed:**
- Created src/config.py (authoritative simulation parameter definitions)
- Created src/config_schema.md (human-readable specification)
- Locked simulation window (180 days starting 2025-07-01)
- Locked scale targets (~300K cases, ~2M events)
- Defined business hours (Mon–Fri 08:00–18:00 local team timezone)
- Defined SLA A (24 business hours resolution) — dual variant logic
- Defined SLA B (2 business hours first touch)
- Defined case mix, tier distribution, staffing model, congestion elasticity
- Defined messy data rates (missing timestamps, duplicates, OOO, tz issues, reopens, escalations)

**Artifacts created/updated:**
- src/config.py
- src/config_schema.md
- reports/impact_log.md

**Runtime / rows (if applicable):**
- Rows processed: N/A
- Runtime: N/A

**Risks / notes:**
- Parameters are baseline assumptions; will validate distribution realism after first generation.
- Congestion + deterioration intentionally structured to create SLA pressure in final 60 days.


---

### Step 2 — Synthetic data generation + raw DuckDB load (partitioned refresh simulation, hardened)
**Date:** 2025-10-09

**What changed:**
- Implemented partitioned synthetic generator for cases + events_log (hive partitions by intake_date)
- Generated calendar_dim (weekend + holiday flags) and staffing_schedule (shift-grain coverage w/ shrinkage + deterioration)
- Loaded raw datasets into DuckDB (raw.cases, raw.events_log, raw.calendar_dim, raw.staffing_schedule)
- Executed dev-scale validation run (~50K cases) for sanity verification
- Executed full-scale run (target ~300K cases, ~2M events)
- Identified partition glob double-counting issue (dev + full parquet files loaded together)
- Hardened pipeline by adding automatic partition cleanup before generation to prevent multi-run contamination
- Re-ran full-scale generation after cleanup to ensure raw layer integrity

**Artifacts created/updated:**
- src/s2_generate_and_load.py (generation logic + cleanup hardening)
- data/generated/cases/ (partitioned parquet, cleaned + regenerated)
- data/generated/events_log/ (partitioned parquet, cleaned + regenerated)
- data/generated/calendar_dim/calendar_dim.parquet
- data/generated/staffing_schedule/staffing_schedule.parquet
- ops_warehouse.duckdb (raw tables rebuilt)
- reports/run_summaries/step2_summary_dev.json
- reports/run_summaries/step2_summary_full.json
- reports/impact_log.md

**Runtime / rows (full-scale run):**
- raw.cases: 300,000
- raw.events_log: 1,931,252
- raw.calendar_dim: 180
- raw.staffing_schedule: 360
- Runtime (seconds):
  - generate_total: 23.817
  - load_total: 0.984
  - end_to_end: 24.801

**Observed data quality (events_log, full-scale):**
- missing event_ts: 1.783%
- duplicate events (flag): 1.971%
- late-arriving events (flag): 1.489%
- timezone inconsistencies: 2.942%

**Risks / notes:**
- Root cause of initial row inflation: parquet glob loading across multiple runs within same partition path.
- Mitigation implemented: enforced partition cleanup prior to generation.
- Raw layer now reflects single-run, target-scale dataset appropriate for downstream SLA and bottleneck analysis.


---

### Step 3 — Raw QA & reconciliation gates (raw readiness confirmed)
**Date:** 2025-10-10

**What changed:**
- Implemented reusable raw QA SQL pack for row counts, partition coverage, event integrity, milestone completeness, and ordering signals.
- Executed readiness gates confirming raw layer is suitable for downstream staging + SLA logic.

**Artifacts created/updated:**
- sql/raw/qa_00_row_counts.sql
- sql/raw/qa_01_event_integrity.sql
- sql/raw/qa_02_case_milestones.sql
- reports/impact_log.md

**Runtime / rows (if applicable):**
- QA pack runtime: ~0.54s total (file runtimes: 0.415s + 0.080s + 0.044s)
- raw.cases: 300,000
- raw.events_log: 1,931,252
- raw.calendar_dim: 180
- raw.staffing_schedule: 360
- distinct intake days: 180 (2025-07-01 → 2025-12-27)
- avg events per case: 6.438

**Gate results:**
- pct missing event_ts: 1.783%
- pct duplicate flag: 1.971%
- pct late-arriving flag: 1.489%
- pct tz inconsistent: 2.942%
- duplicate extra rows (by case_id/status/event_ts grouping): 37,405
- pct ingestion before event_ts: 2.916%
- pct cases with TRIAGE: 99.389%
- pct cases with RESOLVED: 98.212%
- pct cases with CUSTOMER_WAIT: 27.729%
- pct triage before intake (cases with both): 2.8858%
- pct cases both cancelled and resolved: 0.0%

**Risks / notes:**
- Some negative lead-time signals are expected due to tz inconsistency + out-of-order behavior; staging layer will implement canonical timestamps and “first valid after intake” logic.
- ingestion_ts < event_ts occurs for ~2.9% of events; staging will retain raw values but compute canonical timestamp for analysis.


---

### Step 3 — Raw QA & reconciliation gates (scripted, reproducible)
**Date:** 2025-10-10

**What changed:**
- Formalized raw QA gates into runnable module (python -m src.s3_raw_qa)
- Persisted structured QA summary output for auditing and impact tracking

**Artifacts created/updated:**
- src/s3_raw_qa.py
- reports/run_summaries/step3_summary.json
- reports/impact_log.md

**Runtime / rows (if applicable):**
- Step runtime (seconds): 0.646
- raw.cases: 300,000
- raw.events_log: 1,931,252
- distinct intake days: 180
- avg events per case: 6.438

**Gate results (key):**
- pct missing event_ts: 1.783%
- pct duplicate flag: 1.971%
- pct late-arriving flag: 1.489%
- pct tz inconsistent: 2.942%
- pct triage before intake (raw signal): 2.9048%
- pct ingestion before event_ts: 2.916%
- pct cases both cancelled and resolved: 0.0%

**Risks / notes:**
- Raw anomalies (tz inconsistency, out-of-order arrivals) are expected by design; staging layer will canonicalize timestamps and enforce “first valid after intake” milestone selection.


---

### Step 4 — Staging layer build (canonical events + case milestones, scripted)
**Date:** 2025-10-15

**What changed:**
- Formalized staging build into runnable module (python -m src.s4_build_staging)
- Built staging.events_deduped using canonical timestamp + deterministic tie-breakers
- Built staging.events_clean with canonical timestamp and anomaly flags for SLA logic
- Built staging.case_milestones using authoritative intake_ts and milestone timestamp derivations
- Persisted structured staging build summary output for audit and impact tracking

**Artifacts created/updated:**
- src/s4_build_staging.py
- sql/staging/s4_01_events_dedup.sql
- sql/staging/s4_02_events_clean.sql
- sql/staging/s4_03_case_milestones.sql
- ops_warehouse.duckdb (staging tables created/replaced)
- reports/run_summaries/step4_summary.json
- reports/impact_log.md

**Runtime / rows (if applicable):**
- Step runtime (seconds): 1.786
- raw.events_log: 1,931,252
- staging.events_deduped: 1,893,847
- Dedup rows removed: 37,405
- staging.case_milestones: 300,000

**Quality signals:**
- TRIAGE-before-INTAKE (raw signal): 2.9048%
- TRIAGE-before-INTAKE (after milestone derivation): 2.9326%
- staging.events_clean anomaly rates:
  - event_ts missing: 1.818%
  - tz inconsistent: 2.941%
  - ingestion before event_ts: 2.957%

**Risks / notes:**
- TRIAGE-before-INTAKE remains ~2.9% due to intended tz inconsistency/out-of-order behavior; SLA B logic will track invalid lead-time cases explicitly rather than silently correcting.


---

### Step 5 — Business-hours SLA engine (SLA A dual variants + SLA B)
**Date:** 2025-10-20

**What changed:**
- Built business-minute spine excluding weekends + holidays (08:00–18:00)
- Implemented business-time duration calculations using ASOF joins to minute spine
- Produced case-level SLA metrics table with:
  - SLA B (INTAKE→TRIAGE, 2 business hours)
  - SLA A (INTAKE→RESOLVED, 24 business hours) — including CUSTOMER_WAIT vs paused during CUSTOMER_WAIT
- Captured breach rates + p50/p90/p95 percentiles in a repeatable run summary

**Artifacts created/updated:**
- sql/staging/s5_01_business_minutes_dim.sql
- sql/staging/s5_02_case_sla_metrics.sql
- src/s5_build_sla_engine.py
- ops_warehouse.duckdb (staging.business_minutes_dim, staging.case_sla_metrics)
- reports/run_summaries/step5_summary.json
- reports/impact_log.md

**Runtime / rows (if applicable):**
- Step runtime (seconds): 1.439
- business_minutes_dim rows: 75,000
- case_sla_metrics rows: 292,784

**SLA breach rates (resolved cases):**
- SLA B breach % (first touch): 11.827%
- SLA A breach % (including CUSTOMER_WAIT): 6.57%
- SLA A breach % (paused during CUSTOMER_WAIT): 2.698%

**Percentiles (minutes, resolved cases):**
- First touch p50/p90/p95: 39.0, 131.0, 180.0
- First resolution incl CW p50/p90/p95: 409.0, 1102.0, 1570.0
- First resolution paused CW p50/p90/p95: 342.0, 846.0, 1060.0

**Data quality tracking:**
- triage_before_intake (for SLA): 2.9175%


---

### Step 6 — Curated marts for Tableau (no business logic in viz)
**Date:** 2025-10-24

**What changed:**
- Built Tableau-ready mart layer from staging outputs:
  - mart.sla_daily (breach rates + p50/p90/p95 trends)
  - mart.sla_by_tier_case_type (tier/type breakdown)
  - mart.staffing_daily (planned vs effective staffing)
  - mart.backlog_daily_proxy (flow + backlog proxy)
  - mart.congestion_daily (workload vs staffing index)
- Persisted mart build summary output for audit and impact tracking

**Artifacts created/updated:**
- src/s6_build_marts.py
- sql/mart/s6_01_sla_daily.sql
- sql/mart/s6_02_sla_by_tier_case_type.sql
- sql/mart/s6_03_staffing_daily.sql
- sql/mart/s6_04_backlog_daily_proxy.sql
- sql/mart/s6_05_congestion_daily.sql
- ops_warehouse.duckdb (mart tables created/replaced)
- reports/run_summaries/step6_summary.json
- reports/impact_log.md

**Runtime / rows (if applicable):**
- Step runtime (seconds): 0.532
- mart.sla_daily rows: 180
- mart.sla_by_tier_case_type rows: 9

**Headline metrics (daily averages):**
- Avg SLA B breach % (daily): 8.881%
- Avg SLA A breach % incl CW (daily): 5.702%
- Avg SLA A breach % paused CW (daily): 2.32%

**Congestion summary:**
- congestion_index p50/p90/p95: 127.537, 493.0999699999999, 610.9624999999996


---

### Step 7 — Bottleneck & driver analysis (tier gradient + stage decomposition + reopen impact)
**Date:** 2025-10-30

**What changed:**
- Built driver/bottleneck marts:
  - Stage-level business-minute duration decomposition (p50/p90/p95) by tier
  - Reopen impact on cycle time (avg + tail) by tier and case type
  - Case-level congestion exposure (10-biz-day window) to test staffing/workload coupling
- Identified primary bottleneck stage for tail latency and SLA A risk

**Key findings (from marts):**
- Primary bottleneck: **Tier 3 tail latency is dominated by investigation → review/QA** (p95 ≈ 2641.8 business minutes)
- Tier gradient is dominant driver of both first-touch and first-resolution SLAs (Tier 3 materially worse across p90/p95 and breach rates)
- Congestion exposure (as simulated) shows minimal lift on SLA B and SLA A breach rates, indicating primary constraints are process/complexity rather than capacity

**Artifacts created/updated:**
- sql/staging/s7_01_case_stage_durations.sql
- sql/staging/s7_00_case_congestion_exposure.sql
- sql/mart/s7_00_congestion_daily_v2.sql
- sql/mart/s7_01_driver_congestion_buckets.sql
- sql/mart/s7_02_driver_reopen_impact.sql
- sql/mart/s7_03_driver_stage_durations.sql
- sql/mart/s7_04_driver_summary.sql
- src/s7_driver_analysis.py
- ops_warehouse.duckdb (staging + mart driver tables)
- reports/run_summaries/step7_summary.json
- reports/impact_log.md

**Runtime / coverage:**
- Step runtime (seconds): 1.366
- Cases with stage decomposition rows: 82,142

**Bottleneck signal:**
- Tier-3 p95 bottleneck stage: investigation_to_reviewqa (2641.8 min)

**Congestion test (exposure deciles):**
- SLA B breach lift p90 vs p50 (abs % pts): -0.677
- SLA A breach lift p90 vs p50 (abs % pts): -1.956


---

### Step 8 — Scenario modeling (process + quality levers; quantified impact)
**Date:** 2025-11-06

**What changed:**
- Modeled counterfactual improvement scenarios using business-minute metrics:
  - Process lever: reduce Tier 3 investigation→review/QA time by 20% (eligible cohort)
  - Quality lever: reduce reopens by 25% using estimated reopen rework minutes (tier-level bottleneck proxy)
  - Combined scenario: process + quality
- Published Tableau-ready scenario table: mart.scenario_results

**Artifacts created/updated:**
- sql/staging/s8_01_reopen_penalty.sql
- sql/mart/s8_01_scenario_results.sql
- src/s8_scenario_modeling.py
- ops_warehouse.duckdb (staging.reopen_penalty, mart.scenario_results)
- reports/run_summaries/step8_summary.json
- reports/impact_log.md

**Runtime / rows:**
- Step runtime (seconds): 0.472
- reopen_penalty rows: 13,791

**Headline scenario outcomes:**
- Tier 3 breaches avoided (S1 eligible cohort, incl CW): 1003
- Total hours saved (combined): 61499.86

