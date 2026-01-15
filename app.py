import streamlit as st
import pandas as pd
import utils
import dag_engine
import cpm_engine
import forecasting_engine
import cost_engine
import evm_engine
import root_cause_engine
import recovery_engine
import graphviz

# --- Phase 1: Configuration ---
st.set_page_config(
    page_title="Project Recovery & What-If Engine",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for "Premium" look
st.markdown("""
<style>
    /* Slight adjustments to make it look cleaner */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    /* Metric styling */
    div[data-testid="stMetricValue"] {
        font-size: 1.2rem;
    }
</style>
""", unsafe_allow_html=True)

# --- Phase 2: Sidebar (Upload & Global Filters) ---
st.sidebar.title("Input & Settings")

st.sidebar.markdown("### 1. Upload Project Schedule")
st.sidebar.info("Upload `project_schedule.csv` (Tasks, Dates, Dependencies)")
uploaded_schedule = st.sidebar.file_uploader("Select Schedule File", type=["csv"], key="schedule_uploader")

st.sidebar.markdown("### 2. Upload Resource Data")
st.sidebar.info("Upload `resource_cost_unit.csv` (Rates, FTEs, Calendars)")
uploaded_resource = st.sidebar.file_uploader("Select Resource File", type=["csv"], key="resource_uploader")

st.sidebar.markdown("---")

# --- Data Loading (Moved up for filters) ---
df_schedule = None
df_resource = None
schedule_errors = []
resource_errors = []
resource_stats = {}
cost_df_results = pd.DataFrame()
rc_df = pd.DataFrame()

# Session State for Analysis
if 'analyzed' not in st.session_state:
    st.session_state['analyzed'] = False
    
if 'recovery_schedule' not in st.session_state:
    st.session_state['recovery_schedule'] = None
    
if 'generated_actions' not in st.session_state:
    st.session_state['generated_actions'] = []

if 'applied_actions' not in st.session_state:
    st.session_state['applied_actions'] = set()

# Load Schedule early to populate filters
if uploaded_schedule:
    try:
        df_schedule = pd.read_csv(uploaded_schedule)
    except Exception as e:
        st.error(f"Error reading project_schedule.csv: {e}")

if uploaded_resource:
    try:
        df_resource = pd.read_csv(uploaded_resource)
    except Exception as e:
        # Check later for full error handling
        pass

# --- Global Filters ---
st.sidebar.subheader("3. Global Filters")

# Defaults
portfolios = ["All"]
projects = ["All"]
activities = ["All"]

if df_schedule is not None:
    # Populate filters dynamically if columns exist
    if "portfolio_name" in df_schedule.columns:
        portfolios = ["All"] + sorted(df_schedule["portfolio_name"].dropna().unique().tolist())
    
    if "project_id" in df_schedule.columns:
        projects = ["All"] + sorted(df_schedule["project_id"].dropna().unique().astype(str).tolist())
        
    if "activity_id" in df_schedule.columns:
        activities = ["All"] + sorted(df_schedule["activity_id"].dropna().unique().astype(str).tolist())

portfolio_filter = st.sidebar.selectbox("Portfolio (from CSV)", portfolios)
project_filter = st.sidebar.selectbox("Project (from CSV)", projects)
activity_filter = st.sidebar.selectbox("Activity (from CSV)", activities)
        
st.sidebar.markdown("---")
if st.sidebar.button("Run Analysis", type="primary"):
    st.session_state['analyzed'] = True
    st.session_state['recovery_schedule'] = None
    st.session_state['generated_actions'] = []
    st.session_state['applied_actions'] = set()

# --- Validation & DAG Logic ---
if st.session_state['analyzed'] and df_schedule is not None:
    try:
        # Type Validation (Columns that must be dates)
        date_cols = ["planned_start", "planned_finish", "baseline_1_start", "baseline_1_finish", "actual_start", "actual_finish", "constraint_date"]
        schedule_errors.extend(utils.validate_iso_dates(df_schedule, date_cols, "project_schedule.csv"))
        
        # Numeric Validation
        num_cols = ["fte_allocation", "percent_complete", "probability_percent"]
        schedule_errors.extend(utils.validate_numeric(df_schedule, num_cols, "project_schedule.csv"))
        
        # --- MVP 1: Dependency Parsing & DAG ---
        # Build DAG and get validation status
        dag_graph, dep_validation = dag_engine.build_dag_and_validate(df_schedule)
        
        # Add Validation Column
        # Map validation results back to dataframe. 
        # using map on activity_id.
        # Ensure activity_id is int for mapping
        try:
             # Create a temp column for mapping to avoid modifying original if it fails
             df_schedule["_temp_id"] = pd.to_numeric(df_schedule["activity_id"], errors='coerce')
             df_schedule["dependency_validation_status"] = df_schedule["_temp_id"].map(dep_validation)
             # Fill NaNs with "OK" (orphans with no preds/succs are just nodes, unless they had errors in map)
             df_schedule["dependency_validation_status"] = df_schedule["dependency_validation_status"].fillna("OK")
             
             # Fallback for Malformed IDs
             mask_invalid_id = df_schedule["_temp_id"].isna()
             df_schedule.loc[mask_invalid_id, "dependency_validation_status"] = "ERROR: Invalid Activity ID"
             
             # --- MVP 2: CPM Calculation ---
             cpm_results = {}
             try:
                 # Only run CPM if DAG is valid (no cycles)
                 # dag_engine validates cycles in map. Check if ANY error contains "Cycle"
                 has_cycles = any("Cycle" in str(val) for val in dep_validation.values())
                 
                 if not has_cycles:
                     # 1. Run CPM (Integer Days)
                     cpm_results = cpm_engine.run_cpm(df_schedule, dag_graph)
                     
                     # 2. Get Durations (for date calc)
                     # (Calculated inside run_cpm but local. We can re-calc or make run_cpm return it.
                     # Re-calc is cheap enough for now)
                     durations_map = cpm_engine.calculate_durations(df_schedule)
                     
                     # 3. Determine Project Start (Anchor)
                     # Min of planned_start.
                     # Handle NaTs
                     if "planned_start" in df_schedule.columns:
                         valid_starts = pd.to_datetime(df_schedule["planned_start"], errors='coerce').dropna()
                         if not valid_starts.empty:
                             project_start = valid_starts.min()
                             
                             # 4. Convert Offsets to Dates
                             cpm_results = cpm_engine.convert_offsets_to_dates(cpm_results, project_start, durations_map)
                         else:
                             st.warning("No valid 'planned_start' dates found. Using today as anchor.")
                             cpm_results = cpm_engine.convert_offsets_to_dates(cpm_results, pd.Timestamp.today(), durations_map)
                     
                     # Map CPM results to DataFrame
                     cpm_df = pd.DataFrame.from_dict(cpm_results, orient='index')
                     
                     # Join on index (activity_id)
                     deps_cols = ["ES", "EF", "LS", "LF", "total_float_days", "on_critical_path", 
                                  "ES_date", "EF_date", "LS_date", "LF_date", "planned_duration"]
                                  
                     for col in deps_cols:
                         if col in cpm_df.columns:
                            df_schedule[col] = df_schedule["_temp_id"].map(cpm_df[col])
                            
                     # --- MVP 4: Forecasting ---
                     # Ensure we run this inside the no-cycle block
                     try:
                         forecast_results = forecasting_engine.calculate_forecasts(df_schedule, dag_graph)
                         fc_df = pd.DataFrame.from_dict(forecast_results, orient='index')
                         
                         fc_cols = ["percent_complete", "actual_duration", "baseline_1_duration", "remaining_duration_days",
                                    "forecast_start_date", "forecast_finish_date", 
                                    "delay_carried_in", "total_schedule_delay", 
                                    "task_created_delay", "delay_absorbed"]
                                    
                         for col in fc_cols:
                             if col in fc_df.columns:
                                 df_schedule[col] = df_schedule["_temp_id"].map(fc_df[col])
                                 

                             
                     except Exception as e:
                         st.warning(f"Forecasting Engine Error: {e}")

                     # --- MVP 5: Resources & Costs ---
                     cost_df_results = pd.DataFrame()
                     resource_stats = {}
                     
                     if df_resource is not None:
                         try:
                             cost_df_results = cost_engine.calculate_costs(df_schedule, df_resource)
                             
                             # Map back to df_schedule (activity_id is key)
                             # Create lookup
                             cost_lookup = cost_df_results.set_index("activity_id")
                             
                             cost_cols = ["planned_load_hours", "planned_cost", "actual_load_hours", 
                                          "actual_cost", "remaining_load_hours", "remaining_cost", "eac_cost"]
                             
                             for col in cost_cols:
                                 if col in cost_lookup.columns:
                                     # Safe mapping
                                     df_schedule[col] = df_schedule["activity_id"].map(cost_lookup[col])
                                     
                             # Overload Check
                             resource_stats = cost_engine.check_resource_availability(cost_df_results)
                             
                         except Exception as ce:
                             st.warning(f"Cost Engine Error: {ce}")

                         # --- MVP 7: Root Cause ---
                         try:
                             rc_df = root_cause_engine.execute_root_cause_analysis(df_schedule, resource_stats)
                             
                             # --- MVP 8: Recovery Action Generation ---
                             # Init workspace if not present (or reset on new analysis?)
                             # If we want to KEEP changes across runs, checking None is good. 
                             # But "No mutations... All changes reversible...".
                             # If user clicks "Run Analysis", likely resetting baseline state or re-calcing.
                             # Let's Refresh Recovery Workspace on Run Analysis to sync with new data.
                             if st.session_state['recovery_schedule'] is None:
                                 st.session_state['recovery_schedule'] = recovery_engine.init_recovery_workspace(df_schedule)
                             
                             # Generate Actions ONLY if not already present (Preserve list for UI persistence)
                             if not st.session_state['generated_actions']:
                                 actions = recovery_engine.generate_actions(
                                     st.session_state['recovery_schedule'], 
                                     resource_stats, 
                                     df_resource, 
                                     rc_df
                                 )
                                 st.session_state['generated_actions'] = actions
                             
                         except Exception as re:
                             st.warning(f"Root Cause Engine Error: {re}")
                             rc_df = pd.DataFrame()

             except Exception as cpm_e:
                 st.error(f"Error in CPM Logic: {cpm_e}")

             del df_schedule["_temp_id"]
             
        except Exception as e:
             st.error(f"Error applying dependency validation: {e}")

    except Exception as e:
        st.error(f"Error processing project_schedule.csv: {e}")

# Load Resource (Validation)
if df_resource is not None:
    try:
        # Validation
        resource_errors.extend(utils.validate_columns(df_resource, utils.REQUIRED_COLUMNS_RESOURCE, "resource_cost_unit.csv"))
        
        # Type Validation
        res_date_cols = ["resource_start_date", "resource_end_date"]
        resource_errors.extend(utils.validate_iso_dates(df_resource, res_date_cols, "resource_cost_unit.csv"))
        
        res_num_cols = ["resource_rate", "resource_max_fte", "resource_working_hours"]
        resource_errors.extend(utils.validate_numeric(df_resource, res_num_cols, "resource_cost_unit.csv"))

    except Exception as e:
        st.error(f"Error validating resource_cost_unit.csv: {e}")


# --- Tab Content ---
tabs = st.tabs([
    "Overview", 
    "Schedule Recovery", 
    "Resource Recovery", 
    "Cost Recovery", 
    "EVM", 
    "Project Data", 
    "Resource Data", 
    "Scenarios"
])

with tabs[0]: # Overview
    if not rc_df.empty:
        st.subheader("Root Cause Diagnostics (Ranked)")
        
        # Styling for certainties?
        st.dataframe(rc_df, use_container_width=True)
        st.divider()

    st.subheader("Project Network Diagram")
    if df_schedule is not None and 'dag_graph' in locals() and dag_graph:
        try:
            # Create Graphviz object
            dot = graphviz.Digraph()
            dot.attr(rankdir='LR') # Left to right layout
            
            # Add nodes
            # Color code based on validation?
            # Critical path in RED
            
            # Helper to check critical status from df
            # We need to look up rows by activity_id (int)
            # Create a lookup for speed
            crit_lookup = {}
            if "on_critical_path" in df_schedule.columns:
                 # Convert to dict: id -> bool
                 # activity_id might be mixed type, force int for lookup keys
                 # We need to handle potential parse errors again or assume clean from before
                 temp_lookup = df_schedule.set_index("activity_id")["on_critical_path"].to_dict()
                 # Ensure keys are ints if possible
                 for k, v in temp_lookup.items():
                     try:
                         crit_lookup[int(k)] = v
                     except:
                         pass

            for node in dag_graph.nodes:
                # check status
                status = dep_validation.get(node, "OK")
                is_crit = crit_lookup.get(node, False)
                
                color = "black"
                if "ERROR" in status:
                    color = "orange" # Separate error from critical
                elif is_crit:
                    color = "red"
                    
                label = str(node)
                # optionally show float in label?
                # float_val = df_schedule.loc[...]["total_float_days"] ... too complex for simple lookup
                
                dot.node(str(node), label=label, color=color, fontcolor=color, penwidth="2" if is_crit else "1")
            
            # Add edges
            for u, v, data in dag_graph.edges(data=True):
                edge_label = f"{data.get('type')}"
                lag = data.get('lag', 0)
                if lag != 0:
                    edge_label += f"{lag:+d}d"
                
                # Critical Edge?
                # If both U and V are critical, AND the relationship drives it?
                # Simplification: If both are critical, color red.
                # (Accurate CPM viz requires float check on edge, but for MVP this is okay)
                u_crit = crit_lookup.get(u, False)
                v_crit = crit_lookup.get(v, False)
                
                edge_color = "black"
                penwidth = "1"
                if u_crit and v_crit:
                    edge_color = "red"
                    penwidth = "2"
                    
                dot.edge(str(u), str(v), label=edge_label, color=edge_color, penwidth=penwidth)
                
            st.graphviz_chart(dot, use_container_width=True)
            
            # Summary stats
            st.caption(f"Dependency Graph: {dag_graph.number_of_nodes()} Activities, {dag_graph.number_of_edges()} Dependencies")
            
        except Exception as e:
            st.warning(f"Could not render graph: {e}")
            st.info("Ensure Graphviz is installed on your system.")
    else:
        if not uploaded_schedule:
             st.info("Upload 'project_schedule.csv' to view the Network Diagram.")
        else:
             st.info("No dependencies found or graph could not be built.")

with tabs[1]: # Schedule Recovery
    st.subheader("Schedule Recovery Diagnostics")
    if df_schedule is not None and st.session_state['analyzed']:
        # Columns: Activity, Critical?, Float, Total Delay, Task-Created, Carried In, Dependencies?
        # Dependency type is hard to list in one row if multiple. "Dependency type causing constraint" -> Predecessors?
        
        cols_to_show = ["activity_id", "on_critical_path", "total_float_days", 
                        "total_schedule_delay", "task_created_delay", "delay_carried_in"]
        
        # Check existence
        disp_cols = [c for c in cols_to_show if c in df_schedule.columns]
        
        req_df = df_schedule[disp_cols].copy()
        # Formatting
        req_df["on_critical_path"] = req_df["on_critical_path"].apply(lambda x: "YES" if x else "NO")
        
        st.dataframe(req_df, use_container_width=True)
        
        st.divider()
        st.divider()
        st.subheader("Schedule Recovery Actions")
        
        with st.expander("ℹ️ Understanding the Recovery Logic (Click to Expand)"):
            st.markdown("""
            **Condition for Duration Compression:**
            *   **Trigger**: Critical Path Slippage.
            *   **Rule**: Reduce task duration by up to **20%** (if remaining duration ≥ 2 days).
            *   *Goal*: Recover time by adding effort/resources (implied).

            **Condition for Fast-Tracking:**
            *   **Trigger**: Critical Path task with FS (Finish-to-Start) dependency.
            *   **Rule**: Convert Predecessor relationship to **SS (Start-to-Start) + Lag**.
            *   *Goal*: Execute tasks in parallel to shorten the schedule.
            """)
        
        # Filter Actions
        all_actions = st.session_state.get('generated_actions', [])
        compressions = [a for a in all_actions if a['type'] == recovery_engine.ACTION_COMPRESS]
        fast_tracks = [a for a in all_actions if a['type'] == recovery_engine.ACTION_FAST_TRACK]
        
        col_sch1, col_sch2 = st.columns(2)
        
        with col_sch1:
            st.markdown("#### Duration Compression")
            if compressions:
                for i, action in enumerate(compressions):
                     with st.expander(f"{action.get('activity_id')} (Compression)", expanded=True):
                        st.info(action['description'])
                        if st.button("Apply Compression", key=f"btn_comp_{i}", type="primary", use_container_width=True):
                            success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                            if success:
                                st.success(msg)
                                st.session_state['applied_actions'].add(action.get('id', 'temp'))
                                st.rerun()
                            else:
                                st.error(msg)
            else:
                st.info("No compression opportunities found.")

        with col_sch2:
            st.markdown("#### Fast-Tracking")
            if fast_tracks:
                for i, action in enumerate(fast_tracks):
                     with st.expander(f"{action.get('activity_id')} (Fast-Track)", expanded=True):
                        st.info(action['description'])
                        if st.button("Apply Fast-Track", key=f"btn_fast_{i}", type="primary", use_container_width=True):
                            success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                            if success:
                                st.success(msg)
                                st.session_state['applied_actions'].add(action.get('id', 'temp'))
                                st.rerun()
                            else:
                                st.error(msg)
            else:
                st.info("No fast-tracking opportunities found.")

with tabs[2]: # Resource Recovery
    st.subheader("Resource Recovery Diagnostics")
    if resource_stats:
        # Resource Table: Resource, Max FTE, Peak FTE, Overload Days, Assigned Activities
        # We need to scan df_schedule or cost_df_results to find assignments for this resource
        # cost_df_results has 'resource_id' and 'activity_id'.
        
        # Create map: details = { 'RES_1': ['Act A', 'Act B'] }
        res_activity_map = {}
        if not cost_df_results.empty:
            for rid, grp in cost_df_results.groupby("resource_id"):
                res_activity_map[str(rid)] = grp["activity_id"].unique().tolist()
        
        res_data = []
        for rid, stats in resource_stats.items():
            peak = stats.get("peak_fte", 0)
            overload = stats.get("overload_days_count", 0)
            assignments = res_activity_map.get(str(rid), [])
            # Truncate if too long
            assign_str = ", ".join(map(str, assignments))

            # Lookup Project IDs for these activities
            # assignments contains activity_ids. 
            # We filter df_schedule for these IDs and get unique project_ids.
            proj_str = ""
            if assignments and df_schedule is not None:
                # Ensure activity_id is string or matching type
                # assignments likely from cost engine which might use string or int
                rel_df = df_schedule[df_schedule["activity_id"].astype(str).isin([str(x) for x in assignments])]
                if "project_id" in rel_df.columns:
                    unique_projs = rel_df["project_id"].dropna().unique()
                    proj_str = ", ".join(map(str, unique_projs))
            
            # Lookup Resource Name
            r_name = "Unknown"
            if df_resource is not None:
                # Ensure type match for lookup
                r_row = df_resource[df_resource["resource_id"].astype(str) == str(rid)]
                if not r_row.empty:
                    r_name = r_row.iloc[0].get("resource_name", "Unknown")

            res_data.append({
                "Resource ID": rid,
                "Resource Name": r_name,
                "Project ID": proj_str,
                "Peak Assigned FTE": peak,
                "Overload Days": overload,
                "Assigned Activities": assign_str
            })
            
        res_diag_df = pd.DataFrame(res_data)
        st.dataframe(res_diag_df, use_container_width=True)
        
        total_overloads = res_diag_df["Overload Days"].sum()
        st.metric("Total Resource Overload Days", f"{total_overloads} days")
        
        st.divider()
        # --- Resource Recovery Global Options ---
        st.subheader("Resource Recovery Options")
        
        with st.expander("ℹ️ Understanding the Recovery Logic (Click to Expand)"):
            st.markdown("""
            **Strict Conditions for Resource Swaps:**
            1.  **💰 Strictly Lower Cost**: `Candidate Rate < Current Rate` (Must save money).
            2.  **🧠 Skill Match (≥ 60%)**: Candidate must have >= 60% of required skills. (If 2 skills required, need both).
            3.  **📅 100% Availability**: Zero conflicting tasks in the current project schedule.
            4.  **🚧 Active Task**: Task must not be completed.

            **Strict Conditions for FTE Adjustments:**
            1.  **🚨 Critical Path**: Task must be on critical path.
            2.  **🔋 Capacity**: Current FTE < Max FTE.
            3.  **⏳ Active**: Remaining Duration > 0.
            """)
        
        all_res_actions = st.session_state.get('generated_actions', [])
        
        # Split by type
        swaps = [a for a in all_res_actions if a['type'] == recovery_engine.ACTION_RES_SWAP]
        fte_adjs = [a for a in all_res_actions if a['type'] == recovery_engine.ACTION_FTE_ADJ]
        
        # --- Section 1: Resource Swaps ---
        total_swap_savings = sum([a['parameters'].get('savings', 0) for a in swaps])
        
        st.markdown(f"#### Resource Swaps (Total Potential Savings: **${total_swap_savings:,.2f}**)")
        if swaps:
            for i, action in enumerate(swaps):
                # Card Styling
                with st.expander(f"{action.get('project_name')} | {action.get('resource_name')} (Swap Opportunity)", expanded=False):
                    
                    # 1. Narrative Story
                    if "narrative" in action:
                        st.info(action["narrative"])
                    
                    # 2. Before / After Comparison
                    p = action["parameters"]
                    c1, c2, c3 = st.columns([1, 0.2, 1])
                    
                    with c1:
                        st.markdown("**🔴 Before (Current)**")
                        st.text(f"Resource: {p.get('old_name')}")
                        st.text(f"Rate: ${p.get('old_rate', 0)}/hr")
                        st.text("Status: Allocated")
                        
                    with c2:
                        st.markdown("<h3 style='text-align: center; margin-top: 20px'>➡</h3>", unsafe_allow_html=True)
                        
                    with c3:
                        st.markdown("**🟢 After (Proposed)**")
                        st.text(f"Resource: {p.get('new_name')}")
                        st.text(f"Rate: ${p.get('new_rate', 0)}/hr")
                        st.text(f"Skill Match: {p.get('match_pct')}%")
                    
                    st.divider()
                    
                    # Footer: Metric + Compact Button
                    cf1, cf2 = st.columns([0.7, 0.3], gap="medium")
                    
                    with cf1:
                        st.metric("💰 Cost Savings", f"${p.get('savings', 0):,.2f}", delta="Project Benefit")
                        
                    with cf2:
                        # Space to align with metric value
                        st.write("") 
                        action_id = action.get('id')
                        is_applied = action_id in st.session_state['applied_actions']
                        
                        if is_applied:
                            st.button("Applied ✅", key=f"btn_swap_{i}", disabled=True, use_container_width=True)
                        else:
                            if st.button("Apply Swap", key=f"btn_swap_{i}", type="primary", use_container_width=True):
                                 success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                                 if success:
                                     st.success(msg)
                                     if action_id:
                                         st.session_state['applied_actions'].add(action_id)
                                     st.rerun()
                                 else:
                                     st.error(msg)
        else:
            st.info("No resource swap opportunities found (Skill/Cost/Availability constraints).")

        # --- Section 2: FTE Adjustments ---
        st.markdown("#### FTE Adjustments")
        if fte_adjs:
            for i, action in enumerate(fte_adjs):
                # Header Format
                with st.expander(f"{action.get('project_name')} | {action.get('resource_name')} (Critical Path Recovery)", expanded=True):
                    
                    # 1. Narrative
                    if "narrative" in action:
                        st.warning(action["narrative"])
                        
                    # 2. Comparison
                    p = action["parameters"]
                    c1, c2, c3 = st.columns([1, 0.2, 1])
                    
                    with c1:
                        st.markdown("**⏱ Current Allocation**")
                        st.metric("FTE", f"{p.get('old_fte')} FTE")
                        st.metric("Duration", f"{p.get('old_dur'):.1f} Days")
                        
                    with c2:
                         st.markdown("<h3 style='text-align: center; margin-top: 20px'>➡</h3>", unsafe_allow_html=True)

                    with c3:
                        st.markdown("**🚀 Optimized Allocation**")
                        st.metric("Max FTE", f"{p.get('new_fte')} FTE")
                        st.metric("New Duration", f"{p.get('new_dur'):.1f} Days")
                        
                    st.divider()
                    
                    # Footer: Metric + Compact Button
                    cf1, cf2 = st.columns([0.7, 0.3], gap="medium")
                    
                    with cf1:
                        st.metric("📅 Recovered", f"{p.get('saved_days'):.1f} Days", delta="Time Saved")
                        
                    with cf2:
                         st.write("")
                         action_id = action.get('id')
                         is_applied = action_id in st.session_state['applied_actions']
                         
                         if is_applied:
                             st.button("Applied ✅", key=f"btn_fte_{i}", disabled=True, use_container_width=True)
                         else:
                             if st.button("Apply FTE", key=f"btn_fte_{i}", type="primary", use_container_width=True):
                                  success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                                  if success:
                                      st.success(msg)
                                      if action_id:
                                          st.session_state['applied_actions'].add(action_id)
                                      st.rerun()
                                  else:
                                      st.error(msg)
        else:
            st.info("No FTE adjustment opportunities found (Critical Path/Max FTE constraints).")
            
        st.markdown("""
        **Triggers:**
        *   **Resource Swap**: >60% Skill Match, 100% Availability, and Positive Cost Savings.
        *   **FTE Adjustment**: Critical Path tasks where Current FTE < Max FTE.
        """)
    else:
        st.info("No Resource Data Loaded or No Overloads Detected.")

with tabs[3]: # Cost Recovery
    st.subheader("Cost Recovery Diagnostics")
    if not cost_df_results.empty:
        # Columns: Activity, Planned Cost, Actual Cost, Cost Variance
        
        cost_diag = df_schedule[["activity_id", "planned_cost", "actual_cost"]].copy()
        
        # Ensure numeric
        cost_diag["planned_cost"] = pd.to_numeric(cost_diag["planned_cost"], errors='coerce').fillna(0)
        cost_diag["actual_cost"] = pd.to_numeric(cost_diag["actual_cost"], errors='coerce').fillna(0)
        
        cost_diag["Cost Variance"] = cost_diag["planned_cost"] - cost_diag["actual_cost"]
        cost_diag["Abs Variance"] = cost_diag["Cost Variance"].abs()
        
        # Filter: Only show variance if Actual > 0 or Status suggests it began?
        # User Feedback: "I see values for those where actual cost is not present."
        # If Actual is 0 and Planned is 1000, Variance is 1000 (Under budget).
        # We should clearly label it. Or separate "Completed/Started" from "Not Started".
        # Let's filter diagnostic list to only those with Variance != Planned (meaning some actuals happened)
        # OR just rename column to "Budget Variance (Current)".
        # Better: Filter where Actual > 0.
        
        mask_started = cost_diag["actual_cost"] > 0
        cost_diag_started = cost_diag[mask_started].copy()
        
        if not cost_diag_started.empty:
             # Rank by Abs Variance
             cost_diag_final = cost_diag_started.sort_values("Abs Variance", ascending=False).drop(columns=["Abs Variance"])
             st.dataframe(cost_diag_final, use_container_width=True)
        else:
             st.info("No actual costs recorded yet. Variance analysis requires actuals.")
        
        # Rankings logic used ALL?
        
        # Metrics (Global)
        c1, c2, c3 = st.columns(3)
        total_p = cost_diag["planned_cost"].sum()
        total_a = cost_diag["actual_cost"].sum()
        c1.metric("Total Plans", f"${total_p:,.2f}")
        c2.metric("Total Actuals", f"${total_a:,.2f}")
        c3.metric("Net Variance", f"${total_p - total_a:,.2f}")

        st.divider()
        st.divider()
        st.subheader("Cost Recovery Actions")
        
        with st.expander("ℹ️ Understanding the Recovery Logic (Click to Expand)"):
            st.markdown("""
            **Condition for Scope Deferral:**
            *   **Trigger**: Activity flagged with significant **Cost Overrun** or **Risk**.
            *   **Rule**: Mark activity as 'Deferred' (Remaining Duration set to 0).
            *   **Goal**: Reduce projected spend by removing low-priority or high-risk scope from the immediate baseline.
            """)
        
        cost_actions = [a for a in st.session_state.get('generated_actions', []) 
                        if a['type'] == recovery_engine.ACTION_DEFERRAL]
                        
        st.markdown("#### Scope Deferral")
        if cost_actions:
             for i, action in enumerate(cost_actions):
                with st.expander(f"{action.get('activity_id')} (Potential Deferral)"):
                    st.warning(action['description'])
                    
                    if st.button("Defer Scope", key=f"btn_cost_{i}", type="primary"):
                         success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                         if success:
                             st.success(msg)
                             st.rerun()
                         else:
                             st.error(msg)
        else:
            st.info("No scope deferral opportunities found (based on Cost/Risk triggers).")
            
    else:
        st.info("No Cost Data Calculated.")

with tabs[4]: # EVM
    st.subheader("Earned Value Management Analysis")
    
    if st.session_state['analyzed'] and not cost_df_results.empty:
        
        # 1. EAC Selection
        st.markdown("##### EAC Formula Selection")
        eac_options = {
            0: "EAC = AC + Remaining Cost (Bottom-up)",
            1: "EAC = BAC / CPI",
            2: "EAC = AC + (BAC - EV)",
            3: "EAC = AC + Bottom-up ETC",
            4: "EAC = AC + [(BAC - EV) / (CPI * SPI)]"
        }
        
        # Key must be unique to avoid conflict
        # Use selectbox returning index?
        selected_eac_label = st.selectbox("Select Estimate At Completion (EAC) Method:", list(eac_options.values()), index=0)
        
        # Reverse lookup index
        selected_index = 0
        for k, v in eac_options.items():
            if v == selected_eac_label:
                selected_index = k
                break
        
        # Calculate
        try:
            evm_metrics = evm_engine.calculate_evm_metrics(df_schedule, status_date=None, eac_method_index=selected_index)
            
            # Display Metrics
            # Group 1: Basic
            st.divider()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("PV (Planned Value)", f"${evm_metrics['PV']:,.2f}")
            c2.metric("EV (Earned Value)", f"${evm_metrics['EV']:,.2f}")
            c3.metric("AC (Actual Cost)", f"${evm_metrics['AC']:,.2f}")
            c4.metric("BAC (Budget at Completion)", f"${evm_metrics['BAC']:,.2f}")
            
            # Group 2: Indices & Variance
            st.divider()
            c1, c2, c3, c4 = st.columns(4)
            
            # Helper for formatting strings with "—" on Div/0
            def fmt_idx(val):
                if val == float('inf'): return "—"
                return f"{val:.2f}"
                
            c1.metric("CPI (Cost Perf.)", fmt_idx(evm_metrics['CPI']), delta=f"{evm_metrics['CPI']-1:.2f}" if evm_metrics['CPI']!=float('inf') else None)
            c2.metric("SPI (Sched Perf.)", fmt_idx(evm_metrics['SPI']), delta=f"{evm_metrics['SPI']-1:.2f}" if evm_metrics['SPI']!=float('inf') else None)
            c3.metric("CV (Cost Variance)", f"${evm_metrics['CV']:,.2f}")
            c4.metric("SV (Sched Variance)", f"${evm_metrics['SV']:,.2f}")
            
            # Group 3: Forecasting
            st.divider()
            st.markdown(f"**Forecasting (Method: {selected_eac_label})**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("EAC (Estimate At Comp.)", f"${evm_metrics['EAC']:,.2f}")
            c2.metric("ETC (Est. To Complete)", f"${evm_metrics['ETC']:,.2f}")
            c3.metric("VAC (Variance At Comp.)", f"${evm_metrics['VAC']:,.2f}", delta_color="normal")
            c4.metric("TCPI (to BAC)", fmt_idx(evm_metrics['TCPI_BAC']))
            
        except Exception as e:
            st.error(f"EVM Calculation Error: {e}")
            
    else:
        st.info("Run Analysis to view EVM Metrics.")

with tabs[5]: # Project Data
    st.subheader("Project Schedule Data")
    
    # Show Recovery Workspace if active
    if st.session_state.get('recovery_schedule') is not None:
        st.info("Displaying RECOVERY WORKSPACE (Includes applied changes).")
        
        # Helper for styling
        # Since we can't easily iterate styles row by row efficiently without Styler which returns Styler obj,
        # we will use st.dataframe(styler).
        
        def highlight_changes(row):
            # Default style (no highlight)
            styles = [''] * len(row)
            
            lct = str(row.get("last_change_type", "None"))
            if lct in ["None", "nan", ""]:
                return styles
                
            # Define highlight style
            highlight = 'background-color: #ffff00; color: black'
            
            # Helper to find column index
            def mark_col(col_name):
                try:
                    idx = row.index.get_loc(col_name)
                    styles[idx] = highlight
                except KeyError:
                    pass
            
            # Apply based on type
            if lct == "Resource Swap":
                mark_col("resource_id")
            elif lct == "FTE Adjustment":
                mark_col("fte_allocation")
                mark_col("remaining_duration_days")
            elif lct == "Duration Compression":
                mark_col("remaining_duration_days")
            elif lct == "Fast-Tracking":
                mark_col("predecessors")
                mark_col("predecessor_id")
            elif lct == "Scope Deferral":
                mark_col("is_deferred")
                mark_col("remaining_duration_days")
                mark_col("remaining_cost")
            
            # Also highlight the change tracking columns so they find the row easily?
            # User said "only highlight particular cell which changed". i'll stick to that.
            
            return styles

        rec_df = st.session_state['recovery_schedule']
        
        # Debug: Check if any changes exist
        changes_count = rec_df["last_change_type"].notna().sum()
        if changes_count > 0:
            st.success(f"{changes_count} changes applied in Recovery Workspace. Highlighted below.")
            
            # Show Audit Table
            with st.expander("View Change Log (Audit Trail)", expanded=True):
                 # Filter only changed rows
                 audit_df = rec_df[rec_df["last_change_type"].notna()][["activity_id", "last_change_type", "last_change_id", "resource_id", "remaining_duration_days"]]
                 st.dataframe(audit_df, use_container_width=True)
        else:
            st.info("No changes applied yet. Workspace matches Baseline.")
        
        # Add Reset Button
        if st.button("Reset Recovery Workspace", type="secondary"):
            st.session_state['recovery_schedule'] = recovery_engine.init_recovery_workspace(df_schedule)
            st.rerun()

        # Apply style
        try:
             st.dataframe(rec_df.style.apply(highlight_changes, axis=1), use_container_width=True)
        except Exception as e:
             st.warning(f"Could not apply styling: {e}")
             st.dataframe(rec_df, use_container_width=True)
             
        st.caption(f"Rows: {len(rec_df)} | Columns: {len(rec_df.columns)}")
            
    elif df_schedule is not None:
        st.dataframe(df_schedule, use_container_width=True)
        st.caption(f"Rows: {len(df_schedule)} | Columns: {len(df_schedule.columns)}")
    else:
        st.info("Please upload 'project_schedule.csv' in the sidebar.")

with tabs[6]: # Resource Data
    st.subheader("Resource Cost & Unit Data")
    if df_resource is not None:
        st.dataframe(df_resource, use_container_width=True)
        st.caption(f"Rows: {len(df_resource)} | Columns: {len(df_resource.columns)}")
    else:
        st.info("Please upload 'resource_cost_unit.csv' in the sidebar.")

with tabs[7]: # Scenarios
    st.info("Scenario Management - Coming Soon")

# --- Validation Panel (Always visible if errors exist) ---
all_errors = schedule_errors + resource_errors
if all_errors:
    st.markdown("### ⚠️ Data Validation Issues")
    with st.expander("View Validation Errors", expanded=True):
        for err in all_errors:
            st.error(err)
    
    st.warning("Please correct the CSV files and re-upload. The engine will not calculate correctly with invalid data.")
