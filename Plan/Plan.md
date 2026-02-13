=========================================================================================================
MVP 0: CSV Contract & Data Integrity Gate
=========================================================================================================

You are building MVP 0 of a Project Recovery Engine.

Your ONLY responsibility:
Validate input CSV files and enforce a strict data contract.

This MVP is a HARD GATE.
If data is invalid, NOTHING downstream runs.

If you auto-correct data, infer intent, or proceed with warnings → you failed.

=====================
INPUT FILES
=====================

1. project_schedule.csv
2. resource_cost_unit.csv

Both are loaded as pandas DataFrames.

=====================
CORE PRINCIPLE
=====================

Garbage in → HARD STOP.
No silent fixes.
No assumptions.
No defaults beyond explicitly allowed ones.

=====================
project_schedule.csv — SCHEMA
=====================

REQUIRED COLUMNS:

- project_id
- activity_id
- activity_name
- planned_start (ISO YYYY-MM-DD)
- planned_finish (ISO YYYY-MM-DD)
- planned_duration (int, working days)
- percent_complete (0–100)
- predecessors (string, nullable)
- resource_id
- fte_allocation (fte > 0, numeric, no upper bound)

OPTIONAL COLUMNS:

- baseline_start
- baseline_finish
- actual_start
- actual_finish
- actual_duration
- constraint_type
- constraint_date
- risk_impact
- probability_percent

=====================
resource_cost_unit.csv — SCHEMA
=====================

REQUIRED COLUMNS:

- resource_id
- resource_rate
- resource_max_fte
- resource_start_date

OPTIONAL COLUMNS (WITH DEFAULTS):

- resource_end_date (null = no limit)
- resource_working_hours (default = 8)
- resource_calendar (default = Mon–Fri)
- resource_holidays (list or empty)
- resource_skills

=====================
TYPE & FORMAT VALIDATION
=====================

Dates:

- Must be valid ISO format
- planned_finish >= planned_start
- baseline_finish >= baseline_start (if present)
- constraint_date required if constraint_type exists

Numbers:

- planned_duration > 0
- actual_duration ≥ 0 (if present)
- fte_allocation (fte > 0, numeric, no upper bound)
- percent_complete ∈ [0, 100]
- probability_percent ∈ [0, 100]

=====================
LOGICAL CONSISTENCY RULES
=====================

- percent_complete == 100 → actual_finish MUST exist
- percent_complete == 0 → actual_finish MUST be empty
- actual_finish >= actual_start (if both exist)
- remaining_duration must NOT exist (engine-calculated later)

=====================
DEPENDENCY VALIDATION
=====================

For predecessors column:

- Format: <activity_id><FS|SS|FF|SF><optional lag>
- Multiple predecessors separated by `;`
- Lag must be integer working days (e.g. +2d, -1d)

Rules:

- activity cannot depend on itself
- referenced predecessor activity_id MUST exist
- invalid dependency types → FAIL
- malformed lag syntax → FAIL

DO NOT:

- Infer missing dependency types
- Normalize delimiters
- Repair malformed strings

=====================
REFERENTIAL INTEGRITY
=====================

- activity_id unique within project_id
- resource_id in project_schedule.csv
  MUST exist in resource_cost_unit.csv

=====================
FAILURE BEHAVIOR
=====================

On ANY violation:

- Raise exception
- Include:
  • row index
  • column name
  • offending value
  • clear error message

No warnings. No partial success.

=====================
OUTPUT
=====================

If validation PASSES:
Return:
{
"project_schedule_df": cleaned_df,
"resource_cost_df": cleaned_df
}

If validation FAILS:
Raise ValidationError with structured details.

=====================
IMPLEMENTATION RULES
=====================

- No mutation of original DataFrames
- No defaults beyond documented ones
- Validation logic only — NO calculations
- Functions must be deterministic and testable

=====================
EXPECTED FUNCTIONS
=====================

- validate_project_schedule(df) -> DataFrame
- validate_resource_cost(df) -> DataFrame
- validate_dependencies(df) -> None
- validate_referential_integrity(schedule_df, resource_df) -> None

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Missing required column
- Invalid date format
- planned_finish < planned_start
- percent_complete = 100 without actual_finish
- Self-dependency
- Dependency referencing non-existent activity
- Resource referenced but missing in resource file

=========================================================================================================
MVP 1: Dependency Graph & DAG Engine
=========================================================================================================

You are building MVP 1 of a Project Recovery Engine.

Your ONLY responsibility in this task:
Build a correct dependency graph (DAG) from validated schedule data.

DO NOT:

- Calculate dates
- Use calendars
- Calculate ES / EF / LS / LF
- Calculate cost
- Build UI
- Auto-fix bad data

If you do any of the above, you failed.

=====================
INPUT ASSUMPTIONS
=====================

Input is already validated (MVP 0 completed).

You receive a pandas DataFrame `df_schedule` with columns:

- project_id
- activity_id (unique within project)
- predecessors (string, may be empty)

Predecessor format:

- Multiple predecessors separated by `;`
- Each predecessor encoded as:
  <activity_id><dependency_type><optional_lag>

Examples:

- "3FS"
- "5SS+2d"
- "7FF-1d"
- "2SF"

Valid dependency types:

- FS, SS, FF, SF

Lag rules:

- Lag is optional
- Lag is an integer in working days
- Default lag = 0
- Lag can be positive or negative

=====================
OUTPUT REQUIREMENTS
=====================

For each project_id independently:

1. Build a directed graph:

   - Each node = activity_id
   - Each edge = dependency
   - Edge attributes:
     - predecessor_id
     - successor_id
     - dependency_type (FS/SS/FF/SF)
     - lag_days (int)

2. Validate DAG:

   - No self-dependencies
   - No circular dependencies
   - If a cycle exists → raise a hard exception
     (DO NOT attempt to fix it)

3. Produce:
   - graph (networkx.DiGraph)
   - topological order (list of activity_ids)
   - reverse topological order

=====================
IMPLEMENTATION RULES
=====================

- Use Python
- Use networkx.DiGraph
- Graph is per project (no cross-project edges)
- Raise descriptive errors (activity_id, cause)
- Do NOT store dates or durations in graph
- Do NOT mutate input DataFrame

=====================
EXPECTED FUNCTIONS
=====================

Implement at minimum:

- parse_predecessors(predecessor_str) -> List[EdgeSpec]
- build_dependency_graph(df_schedule, project_id) -> DiGraph
- validate_dag(graph) -> None
- get_topological_orders(graph) -> (topo, reverse_topo)

EdgeSpec structure:

- predecessor_id
- dependency_type
- lag_days

=====================
TEST CASES (MANDATORY)
=====================

Include unit-test-like examples:

- Single activity, no predecessors
- Multiple FS predecessors
- Mixed FS / SS / FF / SF
- Negative lag
- Self-dependency (must fail)
- Simple cycle (A → B → A must fail)

=====================
OUTPUT FORMAT
=====================

Return a dictionary keyed by project_id:
{
project_id: {
"graph": DiGraph,
"topo_order": [...],
"reverse_topo_order": [...]
}
}

No UI. No printing. No side effects.

====================================================================================
MVP 2: CPM Scheduling Engine (No Calendars)
====================================================================================
You are building MVP 2 of a Project Recovery Engine.

Your ONLY responsibility:
Implement correct CPM calculations (ES, EF, LS, LF, Float, Critical Path)
on top of an already-validated dependency graph.

If you touch calendars, UI, cost, resources, or actuals → you failed.

=====================
INPUT ASSUMPTIONS
=====================

MVP 1 is complete.

You receive, per project:

- A networkx.DiGraph `G`
- Nodes = activity_id
- Edges have attributes:
  - dependency_type: FS / SS / FF / SF
  - lag_days: int (can be negative)

You also receive a pandas DataFrame `df_schedule` with:

- activity_id
- planned_duration (integer, working days)

Durations are integers.
Time is represented as integer day offsets (Day 0, Day 1, ...).
NO dates.

=====================
OUTPUT REQUIREMENTS
=====================

For each activity_id, compute:

- ES (Early Start, int)
- EF (Early Finish, int)
- LS (Late Start, int)
- LF (Late Finish, int)
- total_float_days (int)
- on_critical_path (bool)

Definitions:

- EF = ES + duration
- LS = LF - duration
- total_float = LS - ES
- Critical Path ⇢ total_float == 0

=====================
ALGORITHM (NON-NEGOTIABLE)
=====================

1. Identify project start:

   - Project ES = 0

2. Forward Pass (topological order):
   For each node:

   - ES = max(candidate_ES from all incoming edges)
   - If no predecessors → ES = 0
   - EF = ES + duration

3. Backward Pass (reverse topological order):
   - Project LF = max(EF of all nodes)
     For each node:
   - LF = min(candidate_LF from all outgoing edges)
   - If no successors → LF = project_LF
   - LS = LF - duration

=====================
EDGE-AWARE LOGIC (CORE OF THIS MVP)
=====================

You MUST implement dependency-specific logic.

For forward pass candidate ES:

FS: pred.EF + lag
SS: pred.ES + lag
FF: pred.EF + lag - succ.duration
SF: pred.ES + lag - succ.duration

For backward pass candidate LF (mirror logic):

FS: succ.LS - lag
SS: succ.LS - lag + node.duration
FF: succ.LF - lag
SF: succ.LF - lag + node.duration

If this logic is wrong, the engine is invalid.

=====================
IMPLEMENTATION RULES
=====================

- Do NOT modify the graph
- Do NOT store durations or ES/EF inside the graph
- All calculations live in a separate data structure
- Raise clear errors for:
  - Missing duration
  - Negative duration
  - Invalid dependency type

=====================
EXPECTED FUNCTIONS
=====================

Implement at minimum:

- compute_forward_pass(G, durations) -> dict[activity_id, {ES, EF}]
- compute_backward_pass(G, durations, forward_results) -> dict[activity_id, {LS, LF}]
- compute_float_and_cp(forward, backward) -> final_results

Final result per activity:
{
"ES": int,
"EF": int,
"LS": int,
"LF": int,
"total_float_days": int,
"on_critical_path": bool
}

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Simple FS chain
- Parallel paths with float
- SS dependency
- FF dependency
- SF dependency
- Positive lag
- Negative lag
- Mixed dependency types

At least one test must prove:

- Non-FS dependencies affect ES/LS correctly

=====================
OUTPUT FORMAT
=====================

Return a pandas DataFrame indexed by activity_id
with all computed fields.

No printing.
No UI.
No dates.
No calendars.

====================================================================================
MVP 3: Calendar-Aware Scheduling Engine
====================================================================================
You are building MVP 3 of a Project Recovery Engine.

Your ONLY responsibility:
Convert integer-based CPM results into calendar-aware date calculations.

You are EXTENDING MVP 2.
You are NOT rewriting it.

If you touch cost, resources, UI, scenarios, or actuals → you failed.

=====================
INPUT ASSUMPTIONS
=====================

MVP 2 is complete and correct.

Inputs:

1. CPM results per activity:

   - ES_offset (int, working days)
   - EF_offset
   - LS_offset
   - LF_offset
   - duration_days

2. Schedule metadata:

   - project_start_date (ISO YYYY-MM-DD)

3. Calendar data:
   - Default calendar:
     • 5 working days per week
     • Saturday/Sunday = non-working
   - Optional per-activity calendar overrides (future-proofed, not required yet)

=====================
GOAL
=====================

Convert all offsets into real dates while respecting calendars.

You must produce:

- ES_date
- EF_date
- LS_date
- LF_date

Dates must reflect WORKING DAYS, not calendar days.

=====================
NON-NEGOTIABLE RULES
=====================

- ES_date = add_working_days(project_start_date, ES_offset)
- EF_date = add_working_days(ES_date, duration_days)
- LS/LF follow the same logic

- Working-day arithmetic MUST:
  • Skip weekends
  • Be deterministic
  • Be reversible

If ES_offset = 0 → ES_date = project_start_date
If duration = 1 → EF_date = next working day

=====================
CALENDAR DESIGN (DO NOT VIOLATE)
=====================

Calendars are DATA, not graph structure.

DO NOT:

- Embed calendars into DAG nodes
- Store dates in the graph
- Hardcode weekends into logic outside calendar utilities

Calendar responsibilities live in:

- add_working_days()
- subtract_working_days()

=====================
FUNCTIONS YOU MUST IMPLEMENT
=====================

- is_working_day(date, calendar) -> bool
- add_working_days(start_date, n, calendar) -> date
- subtract_working_days(end_date, n, calendar) -> date
- convert_offsets_to_dates(cpm_results, project_start_date, calendar)

Calendar structure:
{
"working_days": [0,1,2,3,4], # Mon–Fri
"holidays": [] # empty for MVP 3
}

=====================
EDGE CASES YOU MUST HANDLE
=====================

- Start date falls on weekend → shift to next working day
- Negative offsets (from negative float)
- Zero-duration tasks (milestones)
- Large offsets (performance must be acceptable)

=====================
OUTPUT
=====================

Return a pandas DataFrame with:

- activity_id
- ES_date
- EF_date
- LS_date
- LF_date
- total_float_days
- on_critical_path

Dates must be ISO formatted.

No printing.
No UI.
No cost.
No actuals.
No status date.

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Simple chain crossing weekends
- Parallel tasks finishing on different weekdays
- Negative float converting to past dates
- Zero-duration milestone
- Project start on Saturday

If any test fails, the implementation is invalid.

====================================================================================
MVP 4: Status Date & Actuals Integration
====================================================================================
You are building MVP 4 of a Project Recovery Engine.

Your ONLY responsibility:
Introduce STATUS DATE and ACTUALS into the existing
calendar-aware CPM engine.

You are EXTENDING MVP 3.
You are NOT redesigning earlier MVPs.

If you touch cost, EVM, UI, resources, or scenarios → you failed.

=====================
NEW CONCEPTS (MANDATORY)
=====================

Introduce a global:

- status_date (ISO YYYY-MM-DD)

Status date represents:
“The date up to which actual progress is known.”

=====================
INPUT ASSUMPTIONS
=====================

You receive, per activity:

- planned_start_date
- planned_finish_date
- planned_duration_days
- actual_start_date (optional)
- actual_finish_date (optional)
- percent_complete (0–100)
- ES_date / EF_date / LS_date / LF_date (from MVP 3)

All dates are calendar-aware and validated.

=====================
CORE RULES (DO NOT VIOLATE)
=====================

1. Actuals override forecast BEFORE status_date
2. Planned logic applies AFTER status_date
3. Nothing can finish before it starts
4. Negative float IS allowed
5. No “auto-correction” of bad data

=====================
REMAINING DURATION LOGIC
=====================

For each activity:

If percent_complete == 100:

- remaining_duration = 0
- forecast_finish = actual_finish_date (must exist)

If percent_complete == 0:

- remaining_duration = planned_duration
- forecast_start = max(ES_date, status_date)

If 0 < percent_complete < 100:

- remaining_duration = planned_duration × (1 - percent_complete / 100)
- forecast_start = max(actual_start_date, status_date)

Remaining duration is in WORKING DAYS.

=====================
FORECAST DATE RULES
=====================

- Forecast_start_date:
  max(ES_date, actual_start_date, status_date)

- Forecast_finish_date:
  add_working_days(forecast_start_date, remaining_duration)

- Successors must respect updated forecast_finish_date
  via dependency propagation (reuse DAG logic)

DO NOT recompute full CPM from scratch.
Only propagate forward where constraints tighten.

=====================
EDGE CASES (YOU MUST HANDLE)
=====================

- Actual start exists but percent_complete = 0
- Percent complete > 0 but actual_start missing → ERROR
- Actual finish after status_date
- Activities not yet started but planned to have started
- Zero-duration milestones with actual dates

=====================
WHAT THIS MVP PRODUCES
=====================

Per activity:

- forecast_start_date
- forecast_finish_date
- remaining_duration_days
- schedule_slip_days (forecast_finish - planned_finish)

Project-level:

- current_forecast_finish
- total_schedule_variance_days

=====================
IMPLEMENTATION RULES
=====================

- Do NOT mutate baseline or planned dates
- Keep planned, actual, and forecast dates separate
- Reuse calendar utilities from MVP 3
- Raise explicit errors for inconsistent actuals

=====================
OUTPUT
=====================

Return a pandas DataFrame with:

- activity_id
- planned_start_date
- planned_finish_date
- actual_start_date
- actual_finish_date
- forecast_start_date
- forecast_finish_date
- remaining_duration_days
- schedule_slip_days

No printing.
No UI.
No cost.
No EVM.
No resource logic.

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Task 50% complete at status date
- Task completed early
- Task started late, incomplete
- Successor pulled late by predecessor actual slip
- Negative float after actuals applied

====================================================================================
MVP 5: Resource Load & Cost Engine
====================================================================================
You are building MVP 5 of a Project Recovery Engine.

Your ONLY responsibility:
Calculate resource load and cost using schedule forecasts and resource data.

You are EXTENDING MVP 4.
You are NOT optimizing, recommending, or leveling resources.

If you touch EVM formulas, recovery actions, UI, or scenarios → you failed.

=====================
INPUT ASSUMPTIONS
=====================

MVP 4 is complete.

You receive:

1. Activity schedule data (per activity):

- activity_id
- forecast_start_date
- forecast_finish_date
- remaining_duration_days
- planned_duration_days
- actual_duration_days (optional)
- percent_complete
- resource_id
- fte_allocation (fte > 0, numeric, no upper bound)

2. Resource data (resource_cost_unit.csv):

- resource_id
- resource_rate (per hour)
- resource_max_fte
- resource_start_date
- resource_end_date (optional)
- resource_working_hours (default = 8)
- resource_calendar (default = Mon–Fri)
- resource_holidays (optional)

All inputs are validated.

=====================
CORE DEFINITIONS
=====================

Working day:

- Defined by resource calendar
- Excludes weekends and resource_holidays

Load:

- load_hours = working_days × resource_working_hours × fte_allocation

Cost:

- cost = load_hours × resource_rate

=====================
CALCULATION RULES
=====================

PLANNED:

- planned_load_hours =
  planned_duration_days × resource_working_hours × fte
- planned_cost = planned_load_hours × rate

ACTUAL:
If percent_complete > 0:

- actual_load_hours =
  actual_duration_days × resource_working_hours × fte
- actual_cost = actual_load_hours × rate

FORECAST (REMAINING):
If percent_complete < 100:

- remaining_load_hours =
  remaining_duration_days × resource_working_hours × fte
- remaining_cost = remaining_load_hours × rate

EAC (Cost-only, no EVM yet):

- EAC_cost = actual_cost + remaining_cost

=====================
RESOURCE AVAILABILITY RULES
=====================

- Resource cannot work before resource_start_date
- If resource_end_date exists, cannot work after it
- If activity forecast violates availability → FLAG (do not fix)

- If total assigned FTE > resource_max_fte on any day → FLAG overload

This MVP DETECTS overloads.
It does NOT resolve them.

=====================
EDGE CASES YOU MUST HANDLE
=====================

- Activity with multiple resources (sum costs)
- Zero-duration milestone (zero cost)
- Resource with custom working hours
- Resource holiday during activity window
- Actual cost > planned cost (allowed)

=====================
WHAT THIS MVP PRODUCES
=====================

Per activity:

- planned_load_hours
- planned_cost
- actual_load_hours
- actual_cost
- remaining_load_hours
- remaining_cost
- EAC_cost

Per resource:

- daily_assigned_fte
- peak_fte
- overload_days_count

Project-level:

- total_planned_cost
- total_actual_cost
- total_remaining_cost
- total_EAC_cost

=====================
IMPLEMENTATION RULES
=====================

- Reuse calendar utilities from MVP 3
- Do NOT spread cost evenly across calendar days unless required
- All calculations must be deterministic
- No rounding until final output

=====================
OUTPUT
=====================

Return:

1. Activity cost DataFrame
2. Resource utilization DataFrame
3. Project cost summary dict

No printing.
No UI.
No EVM metrics.
No recovery logic.

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Single resource, single activity
- Partial completion activity
- Resource holiday inside activity window
- Resource overload detection
- Resource unavailable during forecast window

====================================================================================
MVP 6: Earned Value Management (EVM) Engine
====================================================================================

You are building MVP 6 of a Project Recovery Engine.

Your ONLY responsibility:
Compute standard Earned Value Management (EVM) metrics
using schedule and cost outputs from previous MVPs.

You are EXTENDING MVP 5.
You are NOT fixing schedules, reallocating resources, or suggesting actions.

If you touch UI, recovery logic, optimization, or scenario cloning → you failed.

=====================
INPUT ASSUMPTIONS
=====================

MVP 5 is complete and correct.

You receive:

PROJECT-LEVEL INPUTS

- status_date (ISO YYYY-MM-DD)
- total_planned_cost (BAC)
- total_actual_cost (AC)
- total_remaining_cost

ACTIVITY-LEVEL INPUTS (per activity)

- planned_cost
- actual_cost
- percent_complete
- planned_start_date
- planned_finish_date

All costs are currency-agnostic numbers.
All dates are calendar-aware.

=====================
EVM CORE DEFINITIONS (STRICT)
=====================

BAC (Budget at Completion):

- Sum of all planned_cost

AC (Actual Cost):

- Sum of all actual_cost up to status_date

PV (Planned Value):

- Sum of planned_cost \* planned_percent_complete_at_status

EV (Earned Value):

- Sum of planned_cost \* actual_percent_complete

planned_percent_complete_at_status:

- Linear spread between planned_start_date and planned_finish_date
- Clamped between 0 and 100
- Calculated as of status_date

=====================
METRICS YOU MUST COMPUTE
=====================

Basic:

- PV
- EV
- AC
- BAC

Variances:

- CV = EV - AC
- SV = EV - PV

Performance Indexes:

- CPI = EV / AC (if AC > 0)
- SPI = EV / PV (if PV > 0)

Forecasting:
User-selectable EAC formulas:

1. EAC = BAC / CPI
2. EAC = AC + (BAC - EV)
3. EAC = AC + Bottom-Up ETC
4. EAC = AC + ((BAC - EV) / (CPI × SPI))

Supporting:

- ETC = EAC - AC
- VAC = BAC - EAC

Efficiency Targets:

- TCPI(BAC) = (BAC - EV) / (BAC - AC)
- TCPI(EAC) = (BAC - EV) / (EAC - AC)

=====================
RULES YOU MUST ENFORCE
=====================

- No division by zero (return NaN with reason)
- Negative variances allowed
- EVM is read-only: no auto-corrections
- Planned cost is the basis for EV, not actual cost

=====================
EDGE CASES YOU MUST HANDLE
=====================

- Activity planned but not started by status_date
- Activity finished early or late
- Actual cost exists but percent_complete = 0
- AC > BAC (allowed)
- EV > BAC (allowed)

=====================
WHAT THIS MVP PRODUCES
=====================

Project-level EVM summary:

- PV, EV, AC, BAC
- CV, SV
- CPI, SPI
- EAC (for each formula)
- ETC
- VAC
- TCPI(BAC)
- TCPI(EAC)

Optional (nice-to-have but encouraged):

- Time-phased PV/EV/AC curves (DataFrame)

=====================
IMPLEMENTATION RULES
=====================

- All formulas must match PMI standards
- No rounding until final presentation
- Keep calculations explainable and traceable
- Functions must be pure and testable

=====================
OUTPUT
=====================

Return:

1. Project-level EVM metrics dict
2. Optional time-series DataFrame for PV/EV/AC

No printing.
No UI.
No recommendations.
No scenario logic.

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- On-plan project
- Behind schedule, under budget
- Ahead of schedule, over budget
- Zero AC at early status date
- CPI ≠ SPI behavior

====================================================================================
MVP 7: Root Cause Analytics (Read-Only Diagnosis)
====================================================================================

You are building MVP 7 of a Project Recovery Engine.

Your ONLY responsibility:
Analyze schedule, cost, and resource outputs to explain
WHY the project is late or over budget.

You are EXTENDING MVP 6.
You are NOT recommending fixes.
You are NOT changing data.
You are NOT simulating scenarios.

If you propose actions, optimization, or UI → you failed.

=====================
GOAL
=====================

Produce a ranked, explainable Root Cause Table answering:
“Why are we late / over budget?”

Diagnosis only. No prescriptions.

=====================
INPUT ASSUMPTIONS
=====================

You receive outputs from MVP 4–6:

SCHEDULE DATA (per activity):

- activity_id
- on_critical_path (bool)
- total_float_days
- schedule_slip_days
- forecast_finish_date
- planned_finish_date
- dependency_types (FS/SS/FF/SF)

COST DATA (per activity):

- planned_cost
- actual_cost
- remaining_cost
- EAC_cost

RESOURCE DATA:

- resource_id
- peak_fte
- resource_max_fte
- overload_days_count

PROJECT-LEVEL:

- total_schedule_variance_days
- total_cost_variance (CV)
- CPI, SPI

All inputs are read-only.

=====================
ROOT CAUSE CATEGORIES
=====================

You MUST classify each cause as ONE of:

1. Critical Path Slippage
2. Negative Float / Logic Constraint
3. Resource Overallocation
4. Cost Overrun
5. Risk / Uncertainty (rule-based proxy)

No free-form categories.

=====================
ANALYSIS RULES (NON-NEGOTIABLE)
=====================

A. SCHEDULE ROOT CAUSES

Identify activities where:

- on_critical_path == True
- AND schedule_slip_days > 0

Impact_days = schedule_slip_days

B. LOGIC / CONSTRAINT ROOT CAUSES

Identify activities where:

- total_float_days < 0
- OR SS / FF / SF dependencies constrain start/finish

Impact_days = abs(total_float_days)

C. RESOURCE ROOT CAUSES

Identify resources where:

- peak_fte > resource_max_fte
- overload_days_count > 0

Impact_days =
proportional to overload severity × duration

D. COST ROOT CAUSES

Identify activities where:

- EAC_cost > planned_cost

Impact_cost = EAC_cost - planned_cost

E. RISK / UNCERTAINTY (RULE-BASED, NO ML)

Flag activities where:

- High remaining_duration
- High cost exposure
- Low float

Impact = heuristic score only (no dates changed)

=====================
CONFIDENCE SCORING (MANDATORY)
=====================

Each root cause must include:
confidence_percent (0–100)

Rules (example):

- Direct critical path slip → 90–100%
- Indirect resource overload → 60–80%
- Risk proxy → 40–60%

Explainability > precision.

=====================
RANKING LOGIC
=====================

Rank root causes by:

1. Impact_days (schedule)
2. Impact_cost (cost)
3. Confidence_percent

Produce a single, unified ranked list.

=====================
OUTPUT
=====================

Return a pandas DataFrame:
Columns:

- cause_type
- activity_id (or resource_id)
- description
- impact_days
- impact_cost
- confidence_percent

Sorted by highest impact first.

=====================
IMPLEMENTATION RULES
=====================

- Deterministic rules only
- No random scoring
- No learned models
- No mutation of source data
- Clear inline comments explaining logic

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Late project driven purely by critical path
- On-time schedule but cost overrun
- Resource overload causing downstream delay
- Negative float without actual delay
- Multiple overlapping root causes

====================================================================================
MVP 8: Heuristic Recovery Engine (No Optimization)
====================================================================================
You are building MVP 8 of a Project Recovery Engine.

Your ONLY responsibility:
Generate and rank RECOVERY OPTIONS using deterministic heuristics
based on diagnosed root causes.

You are EXTENDING MVP 7.
You are NOT automatically applying changes.
You are NOT optimizing globally.
You are NOT using ML, solvers, or black-box logic.

If you mutate baseline data, auto-apply changes, or hide trade-offs → you failed.

=====================
GOAL
=====================

Answer the PM’s question:
“What can I do, and what happens if I do it?”

Provide OPTIONS, not decisions.

=====================
INPUT ASSUMPTIONS
=====================

You receive:

1. Root Cause Table (from MVP 7):

- cause_type
- activity_id / resource_id
- impact_days
- impact_cost
- confidence_percent

2. Schedule Data (from MVP 4):

- forecast_start_date
- forecast_finish_date
- remaining_duration_days
- on_critical_path
- dependency_types

3. Resource Data (from MVP 5):

- resource_id
- resource_skills
- resource_rate
- resource_max_fte
- peak_fte
- overload_days_count

4. Cost Data:

- planned_cost
- actual_cost
- remaining_cost
- EAC_cost

All inputs are read-only.

=====================
RECOVERY ACTION TYPES (STRICT)
=====================

You may generate ONLY these actions:

1. Resource Swap
2. FTE Adjustment
3. Duration Compression
4. Fast-Tracking
5. Scope Deferral (flag-only, no deletion)

No other actions allowed.

=====================
ACTION GENERATION RULES
=====================

A. Resource Swap
Trigger:

- Resource overload OR high cost driver

Logic:

- Find alternate resources with:
  • Matching or higher skill overlap
  • Available FTE in forecast window
- Compute:
  • Cost delta
  • Schedule impact (if any)
  • Risk increase (heuristic)

B. FTE Adjustment
Trigger:

- Activity on critical path
- Remaining duration > 0

Logic:

- Increase or decrease FTE within resource_max_fte
- Duration change = remaining_duration / fte_change_ratio
- Recalculate downstream impact via DAG propagation

C. Duration Compression
Trigger:

- High schedule slip
- Activity has float or parallel successors

Logic:

- Apply % reduction cap (e.g. max 20%)
- Increase cost proportionally
- Never compress milestones

D. Fast-Tracking
Trigger:

- FS dependency on critical path

Logic:

- Convert FS → SS with lag
- Flag increased risk
- Only if successor not started

E. Scope Deferral
Trigger:

- Cost overrun AND low criticality

Logic:

- Flag task for deferral
- Immediate cost reduction
- Finish impact clearly shown

=====================
IMPACT SIMULATION (MANDATORY)
=====================

For EACH recovery option:

- Clone data in-memory
- Apply change locally
- Propagate effects forward only
- Compute deltas:
  • Finish date
  • Total cost
  • Peak resource load
  • Risk score

NO permanent mutation.

=====================
RANKING LOGIC
=====================

Rank recovery options using weighted score:

Score =
(schedule_recovery_weight × finish_days_saved)

- (cost_recovery_weight × cost_delta)

* (risk_weight × risk_increase)

Weights are configurable constants.

Explain every score component.

=====================
OUTPUT
=====================

Return a pandas DataFrame:
Columns:

- action_type
- target_activity_id
- description
- finish_date_delta_days
- cost_delta
- peak_fte_delta
- risk_delta
- confidence_percent
- explanation

Sorted by best score first.

=====================
IMPLEMENTATION RULES
=====================

- Deterministic heuristics only
- Every recommendation must be explainable
- No silent assumptions
- No auto-apply
- No scenario persistence (that’s MVP 9)

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Single critical path recovery
- Cost-only recovery option
- Resource overload resolution
- Conflicting recovery options
- Recovery that improves cost but hurts schedule

====================================================================================
MVP 9: Scenario Management & Change Control
====================================================================================
You are building MVP 9 of a Project Recovery Engine.

Your ONLY responsibility:
Provide safe scenario cloning, change tracking, and delta comparison.

You are EXTENDING MVP 8.
You are NOT recalculating logic from scratch.
You are NOT modifying baseline or current forecast silently.

If you auto-apply changes or merge scenarios implicitly → you failed.

=====================
GOAL
=====================

Allow PMs to experiment without fear.

Scenarios must be:

- Isolated
- Switchable
- Comparable
- Exportable

=====================
CORE CONCEPTS
=====================

1. BASELINE

- Immutable
- Loaded from original CSV

2. CURRENT FORECAST

- Output of MVP 4–6
- Read-only reference

3. SCENARIO

- Clone of current forecast
- Contains a list of explicit changes
- No hidden mutations

=====================
SCENARIO RULES (NON-NEGOTIABLE)
=====================

- A scenario is created ONLY by cloning
- A scenario never modifies baseline
- All changes are additive and reversible
- No scenario affects another scenario

=====================
CHANGE MODEL
=====================

Each change must be recorded as:

{
change_id,
change_type,
target_entity, # activity_id or resource_id
field_modified,
old_value,
new_value,
timestamp
}

No anonymous mutations allowed.

=====================
SUPPORTED CHANGE TYPES
=====================

ONLY allow changes generated by MVP 8:

- Resource Swap
- FTE Adjustment
- Duration Compression
- Fast-Tracking
- Scope Deferral (flag only)

No free-form edits.

=====================
DELTA CALCULATION
=====================

For each scenario, compute deltas vs:
A) Baseline
B) Current Forecast

Deltas must include:

- Finish date delta
- Total cost delta
- Peak resource load delta
- Risk score delta

=====================
UNDO / REDO BEHAVIOR
=====================

- Undo removes last change only
- Redo reapplies removed change
- Order matters
- No branching undo trees

=====================
EXPORT RULES
=====================

Allow export of:

- Scenario schedule CSV
- Scenario cost CSV
- Change log CSV

Export schema must be strict and documented.

=====================
IMPLEMENTATION RULES
=====================

- Scenarios stored as immutable snapshots + change logs
- No deep copy of entire engine state unless required
- Deterministic replays of change sequences
- Clear separation between data and logic

=====================
OUTPUT
=====================

Expose:

- create_scenario(name)
- apply_change(scenario_id, change)
- undo_last_change(scenario_id)
- compare_scenario(scenario_id)

Return:

- Scenario summary object
- Delta comparison DataFrame
- Change log DataFrame

No UI.
No auto-saving.
No side effects.

=====================
TEST CASES (MANDATORY)
=====================

Include tests for:

- Scenario isolation
- Undo/redo correctness
- Conflicting changes in same scenario
- Export consistency
- Switching between scenarios without drift

====================================================================================
MVP 10: UI Layer (Read-First, Controlled Edit)
====================================================================================
You are building MVP 10: the UI layer for a Project Recovery Engine.

Your ONLY responsibility:
Expose existing engine outputs in a way that enforces
correct PM decision flow.

You are NOT:

- Re-implementing logic
- Recalculating data
- Adding intelligence
- “Fixing” engine behavior

If logic appears in the UI layer → you failed.

=====================
UI PHILOSOPHY (NON-NEGOTIABLE)
=====================

PMs think in this order:

1. Are we screwed? (Status)
2. Why? (Diagnosis)
3. What can I do? (Options)
4. What happens if I do it? (Impact)
5. Can I undo it? (Control)

The UI MUST enforce this order.
No skipping. No shortcuts.

=====================
TECH STACK (LOCKED)
=====================

- Python
- Streamlit (or equivalent minimal framework)
- Pandas DataFrames as inputs
- No backend recalculation

=====================
GLOBAL FILTER BAR (ALWAYS VISIBLE)
=====================

Purpose:
Define WHAT DATA IS VISIBLE, not how it is calculated.

Filters operate on derived DataFrames only.
Filters MUST NOT trigger recalculation.

A. SCOPE FILTERS

- Portfolio selector (single-select, default = All)
- Project selector (multi-select, constrained by portfolio)

Rules:

- Portfolio selection limits available projects
- “All Projects” must be explicit
- Scope affects visibility only

B. TEMPORAL VIEW FILTER

- Baseline
- Current Forecast
- Scenario (enabled only if scenarios exist)

Rules:

- Switches data source only
- No recomputation
- Updates all tabs and exports

C. ISSUE FILTERS

- Issue Type: Schedule / Cost / Resource
- Severity: High / Medium / Low
- Root Cause: Dependency / Resource / Scope / Calendar

D. RESOURCE FILTERS

- Resource ID / Role
- Overloaded Only (boolean)
- Critical Path Only (boolean)

E. RECOVERY FILTERS (Recovery tab only)

- Recovery category
- Finish improvement ≥ X days
- Cost impact ≤ Y
- Risk delta direction (↑ / ↓)
- Scope deferral allowed (yes / no)

FILTER RULES (NON-NEGOTIABLE):

- Filters affect visibility only
- Filters do not persist beyond session
- Clearing filters restores full scope
- Filter state is UI-only

Metrics shown reflect the CURRENT FILTER SCOPE.

When multiple projects are selected:

- Finish date = MAX finish date
- Cost = SUM
- Risk = MAX weighted risk

UI must display scope label:
“Showing metrics for: <Portfolio → Project(s) → View>”

=====================
GLOBAL UI ELEMENTS
=====================

A. PROJECT HEALTH STRIP (Sticky, always visible)

Show:

- Overall status: 🟢 🟡 🔴 (derived, not recalculated)
- Finish variance (days)
- Cost variance
- CPI / SPI
- Peak resource overload
- Risk exposure score

Clicking a metric:

- Navigates to the relevant tab
- Applies read-only filters

=====================
TAB STRUCTURE (LOCKED)
=====================

Tabs (left to right):
[Overview] [Schedule] [Resources] [Cost] [Recovery] [Scenarios] [Export]

User cannot reorder tabs.

=====================
FILTER APPLICATION BY TAB
=====================

Overview:

- Scope + Issue filters apply

Schedule:

- Scope + Issue + Resource filters apply
- Critical path visibility only (no recalculation)

Resources:

- Scope + Resource filters apply
- Clicking overload applies temporary UI filter only

Cost:

- Scope filters apply

Recovery:

- ALL filters apply
- UI must indicate how many options are hidden by filters

Scenarios:

- Scope + Temporal filters apply
- Scenario selector overrides Temporal view

Export:

- Exports reflect CURRENT FILTER STATE ONLY

=====================
TAB 1 — OVERVIEW (READ-ONLY)
=====================

Purpose:
Diagnosis only. No edits allowed.

Components:

1. Network Summary Panel

   - Total activities
   - Total dependencies
   - DAG validity (✔ / ❌)
   - Critical path length

2. Critical Path Viewer

   - Interactive graph
   - Red = critical
   - Grey = non-critical
   - Click highlights predecessors/successors

3. Root Cause Table
   - Ranked
   - Filterable
   - Read-only

If DAG invalid → disable all other tabs.

=====================
TAB 2 — SCHEDULE
=====================

Purpose:
Understand schedule damage.

Sections:
A. Schedule Summary

- Planned vs Forecast finish
- Total slip
- % slip from:
  • Critical path
  • Logic
  • Calendars

B. Activity Impact Table

- Sorted by downstream impact
- Shows:
  • Critical path flag
  • Float
  • Slip contribution
  • Dependency type

No editing here.

=====================
TAB 3 — RESOURCES
=====================

Purpose:
Expose overloads clearly.

Sections:
A. Resource Heatmap

- Rows = resources
- Columns = time buckets
- Red = overload

B. Resource Detail Table

- Max FTE
- Peak FTE
- Overload days
- Cost rate

Clicking overload:

- Filters affected activities

=====================
TAB 4 — COST
=====================

Purpose:
Explain money, not impress.

Sections:
A. Cost Waterfall

- Baseline → Planned → Actual → EAC

B. Cost Driver Table

- Ranked by impact
- Activity + resource level

No edits.

=====================
TAB 5 — RECOVERY
=====================

Purpose:
Controlled intervention.

Sections:
A. Recovery Options Table

- From MVP 8
- Sorted by score
- Shows trade-offs clearly

B. Option Detail Panel

- Before vs After
- Finish delta
- Cost delta
- Risk delta

Buttons:

- “Apply to Scenario”
- “Explain Why” (text from engine)

No direct edits allowed.

=====================
TAB 6 — SCENARIOS
=====================

Purpose:
Safety & control.

Sections:

- Scenario selector
- Change log
- Delta comparison vs baseline & forecast

Buttons:

- Undo last change
- Reset scenario
- Export scenario

=====================
TAB 7 — EXPORT
=====================

Purpose:
Leave the tool cleanly.

Exports:

- Schedule CSV
- Cost CSV
- Resource CSV
- Change log CSV

Schema must match engine definitions exactly.

All exports reflect current scope, filters, and temporal view.
Hidden rows must NOT be exported.

=====================
CHANGE PREVIEW BAR (ALWAYS VISIBLE)
=====================

Show:

- # of modified items
- Finish delta
- Cost delta
- Risk delta

Buttons:

- Apply change
- Undo

No silent state changes.

=====================
CHANGE HIGHLIGHTING (MANDATORY)
=====================

Purpose:
Make scenario changes immediately visible without interpretation.

Change highlighting is VISUAL ONLY.
No recalculation.
No inference.
No hidden logic.

=====================
WHAT COUNTS AS A CHANGE
=====================

A row is considered “changed” if:

- It differs from Current Forecast OR Baseline
- Change is explicitly recorded in Scenario Change Log (MVP 9)

UI must NOT attempt to detect changes independently.

=====================
COLOR CODING RULES (LOCKED)
=====================

Use consistent, non-negotiable colors:

- GREEN → Improvement
  • Finish earlier
  • Cost decrease
  • Risk reduction
  • Resource load reduction

- RED → Negative impact
  • Finish delay
  • Cost increase
  • Risk increase
  • Resource overload increase

- YELLOW → Neutral / Trade-off
  • One metric improves, another worsens
  • Scope deferral flags
  • Calendar shifts without net gain

- GREY → No change

No gradients.
No heatmaps here.
Binary clarity only.

=====================
WHERE CHANGES MUST BE HIGHLIGHTED
=====================

Highlight changes in:

- Activity Impact Table
- Resource Detail Table
- Cost Driver Table
- Recovery Options comparison (Before vs After)
- Scenario Delta Comparison table
- Export preview tables (UI only, not CSV)

At minimum:

- Changed cells
- Changed rows
- Delta columns

=====================
INTERACTION RULES
=====================

- Hovering a highlighted cell shows:
  • Old value
  • New value
  • Change source (action ID)

- Clicking a highlighted row:
  • Filters Change Log to relevant entries
  • Does NOT modify scenario

=====================
TEMPORAL BEHAVIOR
=====================

- Baseline view → highlight vs Baseline
- Forecast view → highlight vs Forecast
- Scenario view → highlight vs Forecast + Baseline toggle

User must always know:
“What am I comparing against?”

=====================
ANTI-PATTERNS (FORBIDDEN)
=====================

- Auto-highlighting inferred changes
- Highlighting without change log reference
- Color meaning changing by tab
- Hiding negative changes by default

=====================
FAIL CONDITIONS
=====================

This MVP FAILS if:

- UI invents changes
- Highlighting logic diverges from scenario change log
- Users cannot visually distinguish improvements vs regressions

=====================
IMPLEMENTATION RULES
=====================

- UI consumes engine outputs only
- No recalculation in UI
- No hidden state
- No auto-save
- All actions explicit

=====================
FAIL CONDITIONS
=====================

This MVP FAILS if:

- UI recomputes metrics
- Users can edit raw dates/costs
- Recovery actions apply silently
- Scenarios overwrite each other

- Filters trigger recalculation
- Portfolio metrics shown without scope label
- Exports ignore active filters
- UI silently resets filters

=====================
OUTPUT
=====================

A working Streamlit app
that strictly reflects engine outputs
and enforces PM decision flow.
