This is a professional-grade Project Recovery & What-If Analysis engine for PMs and PMOs. Logically correct, graph-driven, calendar-aware, and scenario-safe. Below is the design the core computation engine + UI behavior, starting from two CSV inputs.

This app take two csv file,

1. project_schedule.csv
2. resource_cost_unit.csv-containing resource information with their availability, cost, FTE, Dates available, calendar holidays, using this information the cost of projects will be calculated. The columns are below

# 1. project_schedule.csv

containing schedule of project including resource allocation of task and other below attribute, this file will come from Planisware or ms project containing below columns

**REQUIRED COLUMNS:**
portfolio_name
project_id
project_name
project_description
activity_id -(1,2, etc.)
activity_name
activity_type (Milestones or Task)
planned_start- (ISO dates (YYYY-MM-DD))
planned_finish- (ISO dates (YYYY-MM-DD))
planned_duration –(working days, as per calendar, calculate this)
percent_complete (will be empty in csv)
predecessor_id- (Can be empty, Must reference valid activity_id, Multiple predecessors→ delimiter ; with type like 3FS;5FS (in same task) or 2SS or 3FF or 4FS+1d etc.)
resource_id
fte_allocation (fte > 0, numeric, no upper bound)

**OPTIONAL COLUMNS:**

baseline_1_start- (ISO dates (YYYY-MM-DD))
baseline_1_finish- (ISO dates (YYYY-MM-DD))
baseline_1_duration–(working days, as per calendar, calculate this)
actual_start- (ISO dates (YYYY-MM-DD))
actual_finish- (ISO dates (YYYY-MM-DD))
actual_duration- (working days, as per calendar, calculate this)
constraint_type (can be blank or we can have data, start no earlier than, or finish no earlier than)
constraint_date(date corresponding to constraint type)
risk_impact (can be empty)
probability_percent (can be empty)
cost_impact_of_risk(can be empty)
delay_impact_days(can be empty)

Just for the records columns, it will be coming from Planisware, dont touch this, this will coming from csv:
Ignore any planned_duration or actual_duration, successor_id columns from CSV.
All durations MUST be computed internally from dates and calendars.
CSV duration fields and successor_id are display-only metadata and never used in logic.

# 2. resource_cost_unit

The second csv u will get is for resource related information = resource_cost_unit

REQUIRED COLUMNS:

- resource_id
- resource_rate
- resource_max_fte
- resource_start_date (when resource is onboarded, and can start working)

OPTIONAL COLUMNS (WITH DEFAULTS):

- resource_end_date (null = no limit)
- resource_working_hours (null = default = 8)
- resource_calendar (can have value like 5d_8h, string value, you dont have to evaluate anything from here, just for records, data coming from planisware)(default = Mon–Fri)
- resource_holidays (list or empty)
- resource_skills

ADDITONAL INFORMATION FROM CSV
resource_name
resource_role
6_day_working? (null = default = 5 days working excluding weekends, default = Mon–Fri)
7_day_working? (null = default = 5 days working excluding weekends, default = Mon–Fri)

TYPE & FORMAT VALIDATION

Dates:

- Must be valid ISO format
- planned_finish >= planned_start
- baseline_finish >= baseline_start (if present)
- constraint_date required if constraint_type exists

Numbers:

- fte_allocation (fte > 0, numeric, no upper bound)
- percent_complete ∈ [0, 100]
- probability_percent ∈ [0, 100]

LOGICAL CONSISTENCY RULES

- actual_finish >= actual_start (if both exist)
- remaining_duration (engine-calculated later, will be empty in csv)

DEPENDENCY VALIDATION

Predecessor format:

- Multiple predecessors separated by `;`

- Each predecessor encoded as:  <activity_id><dependency_type><optional_lag>
  Examples:

- "3FS;2SF"
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

Rules:

- activity cannot depend on itself
- referenced predecessor activity_id MUST exist
- invalid dependency types → FAIL
- malformed lag syntax → FAIL

DO NOT:

- Infer missing dependency types
- Normalize delimiters
- Repair malformed strings

REFERENTIAL INTEGRITY

- activity_id unique within project_id
- resource_id in project_schedule.csv MUST exist in resource_cost_unit.csv

MVP 0 — UI SCAFFOLD + CSV INGESTION

You are building a web app for Project Recovery & What-If Analysis.

SCOPE:

- UI shell
- CSV upload
- Data validation display

GOAL:
I must be able to upload CSV files and SEE the data immediately.

REQUIREMENTS:

1. UI Layout (static, no analytics yet

Global filter:

- Portfolio
- Project
- Activity

Top Bar (sticky):

- Project Health Strip (placeholders only)
  • Overall Status (—)
  • Finish Variance (—)
  • Cost Variance (—)
  • % Critical Path (—)
  • Peak Resource Overload (—)
  • Risk Exposure (—)

Filters (non-functional for now):

- Portfolio (dropdown)
- Project (dropdown)
- Activity (dropdown)

Tabs:

- Tab 1: Overview (empty placeholders)
- Tab 2: Schedule Recovery (empty)
- Tab 3: Resource Recovery (empty)
- Tab 4: Cost Recovery (empty)
- Tab 5: EVM (empty)
- Tab 6: Project Data (ACTIVE)
- Tab 7: Resource Data (ACTIVE)
- Tab 8: Scenarios (empty)

2. CSV Upload

Allow upload of:

- project_schedule.csv
- resource_cost_unit.csv

Rules:

- Files are stored in memory
- No transformations
- No inferred columns

3. Data Display

Tab 6:

- Render project_schedule.csv EXACTLY as uploaded
- Show all columns
- Preserve ordering
- No calculated fields

Tab 7:

- Render resource_cost_unit.csv EXACTLY as uploaded

4. Validation (DISPLAY ONLY)

Validate ONLY:

- Required columns exist
- ISO date format correctness
- Numeric fields are numeric

Show validation errors in a panel.
DO NOT fix data.
DO NOT infer defaults.

5. Hard Rules

DO NOT:

- Compute durations
- Parse dependencies
- Modify data
- Add extra columns
- Add business logic
- assume anything, let me know if u have doubts

OUTPUT:
A working UI where I can upload both CSVs and view raw data.

MVP 1 — Dependency Parsing & DAG

You are EXTENDING the existing app.

SCOPE:

- Dependency parsing
- DAG construction
- Validation errors

INPUT:
Use data already uploaded via project_schedule.csv.

DEPENDENCY RULES:

- Read predecessor_id column only
- Format: <activity_id><FS|SS|FF|SF> (optional lag)
- Multiple separated by ;
- Lag is integer working days (±Nd)

Examples:

- "3FS;2FS"
- "5SS+2d"
- "7FF-1d"
- "2SF"

Include unit-test-like examples:

- Single activity, no predecessors
- Multiple FS predecessors
- Mixed FS / SS / FF / SF
- Negative lag
- Self dependency → ERROR
- Missing referenced activity → ERROR
- Invalid dependency type → ERROR
- Malformed lag → ERROR
- Cycles → ERROR

Valid dependency types:

- FS, SS, FF, SF
  Lag rules:
- Lag is optional
- Lag is an integer in working days
- Default lag = 0
- Lag can be positive or negative

OUTPUT:
Tab 1 (Overview):

- Show dependency graph (nodes + edges)

Tab 6 (Project Data):

- Add ONE computed column (read-only):
  • dependency_validation_status (OK / ERROR)

MVP 2 — CPM ENGINE (INTEGER DAYS ONLY)

Extend the app with a CPM engine.

SCOPE:

- CPM only
- Integer day offsets
- Dependency-aware logic

PREREQUISITE:

- DAG from MVP 1 MUST already exist

CALCULATE per activity:

- ES, EF, LS, LF
- total_float_days
- on_critical_path

RULES:

- Support FS, SS, FF, SF with lags
- Project start = minimum planned_start
- Milestones (0 duration) supported
- Negative float allowed

STORE CPM RESULTS:

- Separate internal structure
- DO NOT overwrite CSV data

UI OUTPUT:

Tab 1:

- Highlight critical path in graph

Tab 6:

- Append computed columns:
  ES, EF, LS, LF, total_float_days, on_critical_path

Include tests for:

- Simple FS chain
- Parallel paths with float
- SS dependency
- FF dependency
- SF dependency
- Positive lag
- Negative lag
- Mixed dependency types

MVP 3 — Calendar-Aware Dates

Extend CPM to calendar-aware scheduling.

SCOPE:

- Convert CPM offsets → dates
- Working-day math
- Weekend handling

DEFAULT CALENDAR:

- Mon–Fri working
- Sat/Sun non-working

RULES:

- planned_start is anchor
- Skip weekends
- Zero-duration milestones supported
- Start on weekend → shift forward

OUTPUT:

Tab 6:
Add:

- ES_date
- EF_date
- LS_date
- LF_date
- planned_duration

DO NOT:

- Store calendars in DAG
- Hardcode logic outside utility functions

EDGE CASES YOU MUST HANDLE

- Start date falls on weekend → shift to next working day
- Negative offsets (from negative float)
- Zero-duration tasks (milestones)
- Large offsets (performance must be acceptable)

Include tests for:

- Simple chain crossing weekends
- Parallel tasks finishing on different weekdays
- Negative float converting to past dates
- Zero-duration milestone
- Project start on Saturday

MVP 4 — Forecasting

CALCULATE(in WORKING DAYS)

- remaining_duration
- forecast_start_date
- forecast_finish_date
- actual_duration
- Delay Carried In (days)
- Total Schedule Delay (days)
- Task-Created Delay (days)
- Delay Absorbed (days)

VALIDATE:

- percent_complete will be empty from csv
- actual_finish is present, then consider percent_complete == 100
- actual_finish is not present then consider percent_complete == 0

Computed delay metrics

Step 1: Calculate Delay Carried In (Inherited Delay)
Goal: Find how much delay is \*\*coming from predecessors
`Delay Carried In/Delay from predecessor= MAX(0, Pred.actual_finish + lag − Pred.baseline_finish − lag)`
Then for the task: `Delay Carried In = MAX(delay from all predecessors)`

Step 2: Calculate Total Schedule Delay
Goal: Measure overall delay vs baseline, regardless of cause.
`Total Schedule Delay = MAX( actual_start − baseline_start,     actual_finish − baseline_finish )`

- Shows “how late is this task vs plan”
- Captures the \*\*visible impact on schedule

Step 3: Calculate Task-Created Delay (Net Task Slip / Root Cause)
Goal: Measure how much delay this task itself added, ignoring inherited delay.
`Task-Created Delay = MAX(0, Total Schedule Delay − Delay Carried In)`

- Task-Created Delay > 0 → Root-Cause Task
- Task-Created Delay = 0 → Delay purely inherited

Step 4: Calculate Delay Absorbed (Optional / Insight)
Goal: Measure how much upstream delay the task absorbed
`Delay Absorbed = Delay Carried In − Task-Created Delay`

- Positive → task helped recover schedule
- Zero → task neutral
- Negative → task worsened schedule (rare)

OUTPUT:

Tab 1:

- Current Forecast Finish

Tab 6:

- forecast_start_date
- forecast_finish_date
- remaining_duration_days
- actual_duration (this is empty from csv)
- Delay Carried In (days)
- Total Schedule Delay (days)
- Task-Created Delay (days)
- Delay Absorbed (days)

Include tests for:

- Task 50% complete
- Task completed early
- Task started late, incomplete
- Successor pulled late by predecessor actual slip
- Negative float after actuals applied

MVP 5 — Resource Load & Cost

Add resource load & cost calculations.

INPUT:

- resource_cost_unit.csv
- Forecast dates from MVP 4

CALCULATE:
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

RULES: RESOURCE AVAILABILITY RULES

- Resource cannot work before resource_start_date
- If resource_end_date exists, cannot work after it
- If activity forecast violates availability → FLAG (do not fix)
- If total assigned FTE > resource_max_fte on any day → FLAG overload

EDGE CASES YOU MUST HANDLE

- Activity with multiple resources (sum costs)
- Zero-duration milestone (zero cost)
- Resource with custom working hours
- Resource holiday during activity window
- Actual cost > planned cost (allowed)

Include tests for:

- Single resource, single activity
- Partial completion activity
- Resource holiday inside activity window
- Resource overload detection
- Resource unavailable during forecast window

OUTPUT:
Tab 3: Resource Recovery (read-only)
Tab 4: Cost Recovery (read-only)
Tab 6: All calculated cost columns appended

You are EXTENDING the existing application.

ABSOLUTE RULE:
This phase is READ-ONLY.
NO schedule changes.
NO resource changes.
NO scenario cloning.
NO recovery actions.

You are computing TRUTH, not fixing problems.

MVP 6 — EARNED VALUE MANAGEMENT (EVM)

INPUTS (already computed):

- Planned cost
- Actual cost
- Remaining cost
- EAC_cost (from MVP 5)
- Planned dates
- Forecast dates
- Percent complete

PROJECT-LEVEL METRICS (MANDATORY):

1. PV (Planned Value)
2. EV (Earned Value)
3. AC (Actual Cost)
4. BAC
5. CV = EV – AC
6. SV = EV – PV
7. VAC = BAC – EAC
8. CPI = EV / AC
9. SPI = EV / PV

EAC CALCULATION OPTIONS (compute ALL, display selectable):

10. EAC = AC + remaining_cost
11. EAC = BAC / CPI
12. EAC = AC + (BAC – EV)
13. EAC = AC + Bottom-up ETC
14. EAC = AC + [(BAC – EV) / (CPI × SPI)]

15. ETC = EAC – AC
16. TCPI (BAC-based)
17. TCPI (EAC-based)

RULES:

- Division by zero → show "—"
- Negative values allowed
- No rounding tricks

UI OUTPUT:

Tab 5: EVM

- Metric table
- User can toggle EAC formula to display
- No editing allowed

MVP 7 — ROOT CAUSE ANALYTICS

GOAL:
Answer ONE question:
"Why are we late / over budget?"

ROOT CAUSE CATEGORIES (EXACTLY ONE PER ENTRY):

1. Critical Path Slippage
2. Negative Float / Logic Constraint
3. Resource Overallocation
4. Cost Overrun
5. Risk / Uncertainty (proxy-based)

RULE-BASED CLASSIFICATION:

- On critical path AND slipped → Critical Path Slippage
- total_float < 0 → Negative Float
- resource overload days > 0 → Resource Overallocation
- actual_cost > planned_cost → Cost Overrun
- High remaining_duration AND high cost AND low float → Risk Proxy

CONFIDENCE SCORING (MANDATORY):

- Direct critical path impact → 90–100%
- Indirect resource constraint → 60–80%
- Risk proxy → 40–60%

RANKING (STRICT ORDER):

1. Impact_days
2. Impact_cost
3. confidence_percent

OUTPUT:

Tab 1 (Overview):

- Root Cause Summary Table (ranked)

Tab 2 (Schedule Recovery – READ ONLY):

- For each activity:
  • On critical path (Y/N)
  • Total float
  • Total schedule delay
  • Task-created delay
  • Delay carried in
  • Dependency type causing constraint

Tab 3: Resource Recovery
Show:

- Resource utilization table
  - Resource
  - Max FTE
  - Peak assigned FTE
  - Overload days count
- Highlight overloaded resources
  Purpose: “Where are my resource problems?”

Tab 4: Cost Recovery (empty)
Show:

- Cost drivers table (ranked)
  - Activity
  - Planned cost
  - Actual cost
  - Cost variance
- High-cost activities flagged
  Purpose: “Where is my money going wrong?”

DO NOT:

- Suggest fixes
- Modify schedules
- Add heuristics

You are EXTENDING the application with CONTROLLED RECOVERY.

IMPORTANT:
There are NO scenarios.
There is exactly ONE mutable workspace called:
“Recovery Workspace”.

---

## NON-NEGOTIABLE RULES

1. Baseline is IMMUTABLE
2. Current Forecast is READ-ONLY
3. All user-applied changes occur ONLY in Recovery Workspace
4. Recovery Workspace starts as a clone of Current Forecast
5. No auto-apply
6. No silent mutations
7. All changes must be visible, reversible only by reset

---

## GOAL

Help the user answer:
“What can I do to fix my project, and what will happen if I do it?”

Provide OPTIONS, not decisions.
Allow the user to APPLY selected options and SEE THE DELTA.

---

## INPUTS

- Root causes from MVP 7
- Forecast schedule & cost from MVP 4–6
- Resource data

Recovery actions can ONLY be generated from diagnosed problems.

---

## SUPPORTED RECOVERY ACTION TYPES (ONLY THESE)

1. Resource Swap
2. FTE Adjustment
3. Duration Compression
4. Fast-Tracking
5. Scope Deferral (flag only, no deletion)

---

## ACTION GENERATION RULES

A. Resource Swap

Trigger:

- Resource overload OR high cost driver

Logic:

- Identify alternate resources with:
  • Lower or comparable rate
  • Skill overlap
  • Availability in forecast window
  • Available FTE

For each option compute:

- Cost delta
- Schedule delta
- Risk increase (heuristic)

Example shown to user:
“Swap Resource R1 → R2 saves $42,000, +0 days, risk +5%”

---

B. FTE Adjustment

Trigger:

- Activity on critical path
- Remaining duration > 0

Logic:

- Increase or decrease FTE within resource_max_fte
- New remaining_duration = remaining_duration / fte_ratio
- Cost scales proportionally
- Propagate forward ONLY

---

C. Duration Compression

Trigger:

- High schedule slip
- Activity has float or parallel successors

Rules:

- Max compression = 20%
- Milestones NOT allowed
- Cost increases proportionally

Notes:
Slips do not always add
Only the critical predecessor delay propagates
Parallel delays don’t stack
Non-critical delays shouldn’t count at all

---

D. Fast-Tracking

Trigger:

- FS dependency on critical path

Rules:

- Convert FS → SS with lag
- Only if successor NOT started
- Risk flag mandatory

---

E. Scope Deferral

Trigger:

- Cost overrun AND low criticality

Rules:

- Flag task only
- Immediate cost reduction
- Schedule impact clearly shown
- Task remains in data (not deleted)

---

## ACTION PREVIEW (MANDATORY)

For EACH suggested action:

- Clone Current Forecast IN MEMORY
- Apply the action locally
- Propagate downstream ONLY
- Compute PREVIEW deltas:

  • Project finish date delta
  • Total cost delta
  • Peak resource load delta
  • Risk score delta

User must see:

- BEFORE (Current Forecast)
- AFTER (If Applied)

Side-by-side.

---

## APPLYING AN ACTION

When user clicks “Apply”:

- Apply the change to Recovery Workspace ONLY
- Log the change with:
  • change_id
  • change_type
  • affected_activity_ids
  • timestamp

- Recompute affected metrics forward only
- Update UI immediately

---

## UI OUTPUT

Tab 2: Schedule Recovery (Workbench)

- Suggested schedule-related actions
- Preview vs Current Forecast
- Apply button per action

Tab 3: Resource Recovery

- Resource swap & FTE options
- Side-by-side cost & load impact

Tab 4: Cost Recovery

- Cost reduction actions
- Finish impact clearly shown

---

## PROJECT DATA (CRITICAL)

Tab 6: Project Data

- Display Recovery Workspace data
- Highlight ALL changed cells:
  • resource_id
  • fte_allocation
  • remaining_duration
  • dependency

- Add audit columns:
  • last_change_type
  • last_change_id

- Highlighting MUST persist until export or reset

---

## EXPORT & RESET

Buttons:

- “Export Recovery CSV” → exports Recovery Workspace
- “Reset Recovery” → discard all applied actions and revert to Current Forecast

DO NOT:

- Create scenarios
- Allow multiple branches
- Modify baseline or current forecast
- Allow free-form edits
