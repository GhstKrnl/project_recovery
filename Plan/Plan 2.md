You are building a professional-grade Project Recovery & What-If Analysis engine for PMs and PMOs. This must be logically correct, graph-driven, calendar-aware(working days/non working days), and scenario-safe. Your job is to design the core computation engine + UI behavior, starting from two CSV inputs.

1. project_schedule.csv
2. resource_cost_unit.csv-containing resource information with their availability, cost, FTE, Dates available, calendar holidays, using this information the cost of projects will be calculated.

Introduce a global:- status_date (ISO YYYY-MM-DD), Status date represents:“The date up to which actual progress is known.”
Introduce a global:
Portfolio : All or select one
Project : All or select one
Activity : All or select one

# 1. project_schedule.csv

containing schedule of project including resource allocation of task and other below attribute, this file will come from Planisware or ms project containing below columns

**REQUIRED COLUMNS:**
portfolio_name
project_id
project_name
project_description
activity_id -(1,2, etc.)
activity_name
activity_type (Milestones, Activity)
planned_start- (ISO dates (YYYY-MM-DD))
planned_finish- (ISO dates (YYYY-MM-DD))
planned_duration –(working days, as per calendar, calculate this)
percent_complete (0–100)
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
risk_impact
probability_percent
cost_impact_of_risk
delay_impact_days

Just for the records columns, it will be coming from Planisware, dont touch this, this will coming from csv:
successor_id

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

COMPUTE
MVP1:Dependency Graph & DAG Engine with Valid dependency types:FS, SS, FF, SF and optional lags

Build a correct dependency graph (DAG) from validated schedule data.

Predecessor format:

- Multiple predecessors separated by `;`
- Each predecessor encoded as:
    <activity_id><dependency_type><optional_lag>

Examples:

- "3FS;2FS"
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

Include unit-test-like examples:

- Single activity, no predecessors
- Multiple FS predecessors
- Mixed FS / SS / FF / SF
- Negative lag
- Self-dependency (must fail)
- Simple cycle (A → B → A must fail)

MVP2:CPM Scheduling Engine

Implement correct CPM calculations (ES, EF, LS, LF, Float, Critical Path) on top of an already-validated dependency graph. All calculations live in a separate data structure

For each activity_id, compute:

- ES (Early Start, int)
- EF (Early Finish, int)
- LS (Late Start, int)
- LF (Late Finish, int)
- total_float_days (int)
- on_critical_path (bool)

ALGORITHM
Identify project start
Forward Pass
Backward Pass
You MUST implement dependency-specific logic.

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

MVP 3 : Calendar-Aware Scheduling Engine
   - Default calendar:
     • 5 working days per week
     • Saturday/Sunday = non-working

Convert integer-based CPM results into calendar-aware date calculations.

Convert all offsets into real dates while respecting calendars. (CPM results, Schedule metadata like project start date)

Working-day arithmetic MUST:
  • Skip weekends
  • Be deterministic
  • Be reversible

For example :If duration = 1 → EF_date = next working day.

Calendars are DATA, not graph structure.

DO NOT:

- Embed calendars into DAG nodes
- Store dates in the graph
- Hardcode weekends into logic outside calendar utilities

Dates must reflect WORKING DAYS, not calendar days.

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

MVP 4: Status Date & Actuals Integration

Introduce STATUS DATE and ACTUALS into the existing calendar-aware CPM engine. Status date represents: “The date up to which actual progress is known.”

INPUT ASSUMPTIONS
You receive, per activity:

- planned_start_date
- planned_finish_date
- planned_duration_days
- actual_start_date (optional)
- actual_finish_date (optional)
- percent_complete (0–100)
- ES_date / EF_date / LS_date / LF_date (from above computed values)

All dates are calendar-aware and validated.

CORE RULES

1. Actuals override forecast BEFORE status_date
2. Planned logic applies AFTER status_date
3. Nothing can finish before it starts
4. Negative float IS allowed

REMAINING DURATION LOGIC
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

FORECAST DATE RULES

- Forecast_start_date:  max(ES_date, actual_start_date, status_date)
- Forecast_finish_date:  add_working_days(forecast_start_date, remaining_duration)
- Successors must respect updated forecast_finish_date  via dependency propagation (reuse DAG logic)

EDGE CASES (YOU MUST HANDLE)

- Actual start exists but percent_complete = 0
- Percent complete > 0 but actual_start missing → ERROR
- Actual finish after status_date
- Activities not yet started but planned to have started
- Zero-duration milestones with actual dates

DO NOT recompute full CPM from scratch. Only propagate forward where constraints tighten.

THIS PRODUCES
Per activity:

- forecast_start_date
- forecast_finish_date
- remaining_duration_days
- schedule_slip_days (forecast_finish - planned_finish)

Project-level:

- current_forecast_finish
- total_schedule_variance_days

Include tests for:

- Task 50% complete at status date
- Task completed early
- Task started late, incomplete
- Successor pulled late by predecessor actual slip
- Negative float after actuals applied

MVP 5 : Resource Load & Cost Engine

Calculate resource load and cost using schedule forecasts and resource data provided by two csvs

Load:

- load_hours = working_days × resource_working_hours × fte_allocation
  Cost:
- cost = load_hours × resource_rate

CALCULATION RULES:
PLANNED:

- planned_load_hours =  planned_duration_days × resource_working_hours × fte
- planned_cost = planned_load_hours × rate

ACTUAL:If percent_complete > 0:

- actual_load_hours =  actual_duration_days × resource_working_hours × fte
- actual_cost = actual_load_hours × rate

FORECAST (REMAINING):If percent_complete < 100:

- remaining_load_hours =  remaining_duration_days × resource_working_hours × fte
- remaining_cost = remaining_load_hours × rate

EAC (Cost-only, no EVM yet):- EAC_cost = actual_cost + remaining_cost

RESOURCE AVAILABILITY RULES

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

WHAT THIS MVP PRODUCES

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

Include tests for:

- Single resource, single activity
- Partial completion activity
- Resource holiday inside activity window
- Resource overload detection
- Resource unavailable during forecast window

MVP 6 : Earned Value Management (EVM) Engine

Compute standard Earned Value Management (EVM) metrics using schedule and cost outputs from previous MVPs.

After calculating cost you need to do eva for below

1. PV
2. EV
3. AC
4. BAC
5. CV = EV – AC
6. SV = EV – PV
7. VAC= BAC – EAC
8. CPI= EV/AC
9. SPI= EV/PV
10. EAC (For this calculate all, but gives user option which one they want to see)
11. EAC= BAC/CPI (If the CPI is expected to be the same for the remainder of the project)
12. EAC= AC + BAC – EV (If future work will be accomplished at the planned rate)
13. EAC = AC + Bottom-up ETC (If the initial plan is no longer valid)
14. EAC = AC+ [(BAC – EV)/ (CPI x SPI)] (If both the CPI and SPI influence the remaining work)
15. ETC = EAC – AC
16. TCPI = (BAC– EV)/(BAC – AC)(The efficiency that must be maintained in order to complete on plan)
17. TCPI = (BAC – EV)/(EAC – AC)(The efficiency that must be maintained in order to complete the current EAC.)

MVP 7: Root Cause Analytics

EXTEND MVP 6. Produce a ranked, explainable Root Cause Table answering:
“Why are we late / over budget?”

ROOT CAUSE CATEGORIES
You MUST classify each cause as ONE of:

1. Critical Path Slippage
2. Negative Float / Logic Constraint
3. Resource Overallocation
4. Cost Overrun
5. Risk / Uncertainty (rule-based proxy)
   1. Flag activities where: - High remaining_duration, High cost exposure, Low float

CONFIDENCE SCORING (MANDATORY)
Each root cause must include:
confidence_percent (0–100)
Rules (example):

- Direct critical path slip → 90–100%
- Indirect resource overload → 60–80%
- Risk proxy → 40–60%
  Explainability > precision.

Rank root causes by:

1. Impact_days (schedule)
2. Impact_cost (cost)
3. Confidence_percent

Produce a single, unified ranked list.

# Computed delay metrics

Delay Carried In (days)
Total Schedule Delay (days)
Task-Created Delay (days)
Delay Absorbed (days)

Step 1: Calculate Delay Carried In (Inherited Delay)
Goal:Find how much delay is \*\*coming from predecessors

Delay from predecessor = MAX(0, Required Date − Baseline Constrained Date)
Then for the task: Delay Carried In = MAX(delay from all predecessors)

Step 2: Calculate Total Schedule Delay
Goal: Measure overall delay vs baseline, regardless of cause.

Total Schedule Delay = MAX( actual_start − baseline_start, actual_finish − baseline_finish )

- Shows “how late is this task vs plan”
- Captures the \*\*visible impact on schedule

Step 3: Calculate Task-Created Delay (Net Task Slip / Root Cause)
Goal: Measure how much delay this task itself added, ignoring inherited delay.

Task-Created Delay = MAX(0, Total Schedule Delay − Delay Carried In)

- Task-Created Delay > 0 → Root-Cause Task
- Task-Created Delay = 0 → Delay purely inherited

Step 4: Calculate Delay Absorbed (Optional / Insight)
Goal: Measure how much upstream delay the task absorbed

`Delay Absorbed = Delay Carried In − Task-Created Delay`

- Positive → task helped recover schedule
- Zero → task neutral
- Negative → task worsened schedule (rare)

Notes:
Slips do not always add
Only the critical predecessor delay propagates
Parallel delays don’t stack
Non-critical delays shouldn’t count at all

MVP 8: Heuristic Recovery Engine (No Optimization)

Generate and rank RECOVERY OPTIONS using deterministic heuristics based on diagnosed root causes.

GOAL

Answer the PM’s question:
“What can I do, and what happens if I do it?” Provide OPTIONS, not decisions.

RECOVERY ACTION TYPES

1. Resource Swap
2. FTE Adjustment
3. Duration Compression
4. Fast-Tracking
5. Scope Deferral (flag-only, no deletion)

ACTION GENERATION RULES

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
- Increase/decrease cost proportionally

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

For EACH recovery option:

- Clone data in-memory
- Apply change locally to Tab 7: Project data with highlighted cells
- Propagate effects forward only
- Compute deltas:
    • Finish date
    • Total cost
    • Peak resource load
    • Risk score

RANKING LOGIC

Rank recovery options using weighted score:

Score =(schedule_recovery_weight × finish_days_saved)

- (cost_recovery_weight × cost_delta)

* (risk_weight × risk_increase)

Weights are configurable constants. Explain every score component.

Include tests for:

- Single critical path recovery
- Cost-only recovery option
- Resource overload resolution
- Conflicting recovery options
- Recovery that improves cost but hurts schedule

MVP 9: Scenario Management & Change Control

Provide safe scenario cloning, change tracking, and delta comparison. You are EXTENDING MVP 8. You are NOT recalculating logic from scratch.
GOAL
Allow PMs to experiment without fear.
Scenarios must be:

- Isolated
- Switchable
- Comparable
- Exportable

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

SCENARIO RULES (NON-NEGOTIABLE)

- A scenario is created ONLY by cloning
- A scenario never modifies baseline
- All changes are additive and reversible
- No scenario affects another scenario

SUPPORTED CHANGE TYPES

ONLY allow changes generated by MVP 8:

- Resource Swap
- FTE Adjustment
- Duration Compression
- Fast-Tracking
- Scope Deferral (flag only)

DELTA CALCULATION

For each scenario, compute deltas vs:
A) Baseline
B) Current Forecast
Deltas must include:

- Finish date delta
- Total cost delta
- Peak resource load delta
- Risk score delta

UNDO / REDO BEHAVIOR

- Undo removes last change only
- Redo reapplies removed change
- Order matters
- No branching undo trees

Allow export via CSV

==========================================================================
UI
==========================================================================

1️⃣ TOP: PROJECT HEALTH STRIP (Sticky)
Metrics (single line, brutal clarity)
• 🔴 / 🟡 / 🟢 Overall Project Status
• Finish Variance (days)
• Cost Variance ($)
•	% Activities on Critical Path
•	Peak Resource Overload (max FTE > cap)
•	Risk Exposure ($ = probability × impact)

Clicking any metric jumps to the relevant tab

Universal Filters:
Portfolio : All or select one
Project : All or select one
Activity : All or select one

TAB 1: OVERVIEW (Read-only, diagnosis only)
A. Network Summary Panel
• Total activities
• Total dependencies
• DAG

B. Critical Path Viewer (Center)
• Dependency Graph (Interactive)
o Nodes = activities
o Red = critical
o Grey = non-critical
• Clicking a node highlights:
o Predecessors
o Successors
o Float
o Risk impact

Use MVP 7: Root Cause Analytics

TAB 2: SCHEDULE RECOVERY (This is where PMs live)
Schedule tab:
Every “bad schedule” boils down to four root causes:

1. Critical path slippage
2. Negative float
3. Broken logic (dependencies)
4. Unrealistic durations / calendars

What the Schedule Recovery tab must show (non-negotiable)
A. “Why is my project late?”
Show: "Computed delay metrics" data

B. “Which activities are killing me?”
For each activity:
• On critical path? (Y/N)
• Total float (negative included)
• Slip contribution (days)
• Downstream impact count
• Dependency type causing constraint (FS/SS/FF/SF)

C: Show DAG

Schedule Recovery Workbench
Answer the PM’s question:
“What can I do, and what happens if I do it?”

Use MVP 8

TAB 3: RESOURCE RECOVERY
Resource Load Panel
A. Resource Utilization Heatmap
• Rows: Resources
• Columns: Time (week/month)
• Red = overload
• Blue = underutilized
Click a red cell → auto-filter task list.

B. Resource Assignment Table
Columns:
• Resource Name
• Role
• Max FTE
• Assigned FTE
• Overload %
• Cost Rate

Resource Recovery Workbench

Use MVP 8 related to recovery of resources
A. Suggested Resource Fixes
Each row:
• Action Type (Replace / Split / Reduce FTE)
• From Resource → To Resource
• Skill Match %
• Cost Delta
• Schedule Impact
Your app MUST justify replacements via:
• resource_skills
• resource_max_fte
• availability window

TAB 4: COST RECOVERY
Cost Breakdown
A. Cost Waterfall
• Baseline → Planned → Actual → EAC
• Variance bars
B. Cost Drivers Table
Ranked by impact:
• Activity ID
• Resource
• Cost Overrun
• Root Cause

Cost Recovery Actions
A. Cost Reduction Options

Trigger
• Forecast cost > budget
Use MVP 8, related to cost calculations
Rows like:
• Replace Resource A ($120/hr) → B ($80/hr)
• Reduce allocation from 1.0 → 0.7
• Defer Task X (Phase 2)
Each action shows:
• Cost Delta
• Finish Impact
• Risk Impact

TAB 5: EVA, use MVP 6

1. PV
2. EV
3. AC
4. BAC
5. CV = EV – AC
6. SV = EV – PV
7. VAC= BAC – EAC
8. CPI= EV/AC
9. SPI= EV/PV
10. EAC (For this calculate all, but gives user option which one they want to see)
11. EAC= BAC/CPI (If the CPI is expected to be the same for the remainder of the project)
12. EAC= AC + BAC – EV (If future work will be accomplished at the planned rate)
13. EAC = AC + Bottom-up ETC (If the initial plan is no longer valid)
14. EAC = AC+ [(BAC – EV)/ (CPI x SPI)] (If both the CPI and SPI influence the remaining work)
15. ETC = EAC – AC
16. TCPI = (BAC– EV)/(BAC – AC)(The efficiency that must be maintained in order to complete on plan)
17. TCPI = (BAC – EV)/(EAC – AC)(The efficiency that must be maintained in order to complete the current EAC.)

Tab 6: Project data

When we import csv, project_schedule.csv, the data should appear here with all columns from csv, plus all calculated columns by application engine

Whenever I apply recovery from each tab, the table here should be highlighted, what got changed, and ready to be exported, if extra columns needed to track changes, like what kind of change, change id, add addtional column after what if scenario or recovery button.

Highlighting is important, for example from resource tab u replaced the resource, so user should see resource id highlighted, if fte is changed the fte cell should be highlighted

Change Preview bar
• of modified activities
• Finish date delta
• Cost delta
• Risk delta
etc.

Buttons:
• 📤 Export CSV

Tab 7: Resource data

this is just a table containing the import of csv file resource_cost_unit.

Tab 8: Use MVP 9
