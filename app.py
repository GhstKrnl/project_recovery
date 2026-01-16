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
                                 
                         # DEBUG: Verify Logic Update
                         max_delay_carry = 0
                         if "delay_carried_in" in df_schedule.columns:
                             max_delay_carry = df_schedule["delay_carried_in"].max()
                         
                         if max_delay_carry == 0:
                             # Extra check: print raw fc_df stats
                             raw_max = fc_df["delay_carried_in"].max() if "delay_carried_in" in fc_df.columns else -1
                             st.toast(f"Debug: App sees Max Delay Carried In = {max_delay_carry}. Raw Engine Max: {raw_max}")
                         else:
                             st.toast(f"Debug: Logic Active. Max Delay Carried In = {max_delay_carry}")
                                 

                             
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
    st.markdown("## 📊 Executive Project Dashboard")
    
    if df_schedule is not None:
        # --- 1. Top-Level KPIs ---
        # Calculate Aggregates
        total_tasks = len(df_schedule)
        pct_complete = df_schedule["percent_complete"].mean() if "percent_complete" in df_schedule.columns else 0
        if pd.isna(pct_complete):
            pct_complete = 0
        
        # Schedule Variance
        forecast_fin = None
        plan_fin = None
        sched_var = 0.0
        if "forecast_finish_date" in df_schedule.columns:
            forecast_fin = pd.to_datetime(df_schedule["forecast_finish_date"]).max()
        if "planned_finish" in df_schedule.columns:
            plan_fin = pd.to_datetime(df_schedule["planned_finish"]).max()
            
        if forecast_fin and plan_fin:
            sched_var = (forecast_fin - plan_fin).days
            
        # Cost Metrics (if available)
        cpi = 0.0
        spi = 0.0
        cv = 0.0
        sv = 0.0
        eac = 0.0
        bac = 0.0
        
        if df_resource is not None and not cost_df_results.empty:
            # Aggregate EVM
            # Just summing up task level logic isn't strictly Project EVM but close enough for Dashboard
            # Let's check if we have project level EVM? We don't. We sum.
            # Assuming cost_df_results has 'earned_value', 'actual_cost', 'planned_value'? 
            # cost_engine.calculate_costs returns basic cols.
            # Let's check MVP 6 EVM engine... evm_engine.calculate_evm()
            # We haven't run evm_engine at global scope yet.
            pass
            
        # Layout: 4 Columns
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Project Status", "Active", f"{total_tasks} Tasks")
        k1.progress(int(pct_complete)/100)
        
        # Schedule KPI
        s_delta = f"{sched_var:+.1f} Days"
        s_color = "normal"
        if sched_var > 0: s_color = "inverse" # Red
        k2.metric("Forecast Finish", forecast_fin.strftime("%Y-%m-%d") if forecast_fin else "N/A", delta=s_delta, delta_color=s_color)
        
        # Cost KPI (Placeholder or calculated)
        # Using simple sums from df_schedule if cost cols exist
        if "remaining_cost" in df_schedule.columns:
            rem_cost = df_schedule["remaining_cost"].sum()
            act_cost = df_schedule["actual_cost"].sum() if "actual_cost" in df_schedule.columns else 0
            eac_est = act_cost + rem_cost
            k3.metric("Est. At Completion (EAC)", f"${eac_est:,.0f}")
        else:
            k3.metric("Est. At Completion (EAC)", "N/A")

        # Risk KPI
        risk_count = 0
        if not rc_df.empty:
            risk_count = len(rc_df)
        k4.metric("Risk Factors Identified", str(risk_count), delta=f"{risk_count} Root Causes", delta_color="inverse")
        
        st.divider()

        # --- 2. Actionable Insights Summary ---
        c_left, c_right = st.columns([2, 1])
        
        with c_left:
            st.subheader("⚠️ Top Root Cause Drivers")
            
            # Categories (Defined in root_cause_engine, but listed here for order)
            all_cats = [
                "Critical Path Slippage",
                "Resource Overallocation",
                "Cost Overrun",
                "Risk / Uncertainty (Proxy)"
            ]
            
            if not rc_df.empty:
                # Aggregate by Category and Reindex
                rc_counts = rc_df["Root Cause Category"].value_counts()
                rc_counts = rc_counts.reindex(all_cats, fill_value=0).reset_index()
                rc_counts.columns = ["Category", "Count"]
                
                # Simple Bar Chart
                st.bar_chart(rc_counts.set_index("Category"), color="#ff4b4b", use_container_width=True)
            else:
                 # Show empty placeholders
                 rc_counts = pd.DataFrame({"Category": all_cats, "Count": [0]*4})
                 st.bar_chart(rc_counts.set_index("Category"), color="#ff4b4b", use_container_width=True)
                 st.success("No significant root causes detected. Project running smoothly.")

            with st.expander("ℹ️ Understanding Categories", expanded=False):
                st.markdown("""
                *   **Critical Path Slippage**: Tasks on the critical path that are delayed (Forecast Finish > Planned Finish), directly pushing out the project end date.
                *   **Resource Overallocation**: Tasks where the assigned resource is booked beyond their Max FTE capacity (e.g., > 1.0 FTE).
                *   **Cost Overrun**: Tasks where Actual Cost + Remaining Cost (EAC) exceeds the Planned Budget.
                *   **Risk / Uncertainty**: Tasks flagged with High Risk or inferred uncertainty based on duration variance.
                """)

        with c_right:
            st.subheader("🛠️ Recovery Opportunities")
            # Count generated actions
            gen_actions = st.session_state.get('generated_actions', [])
            n_ft = len([a for a in gen_actions if a['type'] == recovery_engine.ACTION_FAST_TRACK])
            n_cr = len([a for a in gen_actions if a['type'] == recovery_engine.ACTION_CRASHING])
            n_cp = len([a for a in gen_actions if a['type'] == recovery_engine.ACTION_COMPRESS])
            n_sw = len([a for a in gen_actions if a['type'] == recovery_engine.ACTION_RES_SWAP])
            n_fte = len([a for a in gen_actions if a['type'] == recovery_engine.ACTION_FTE_ADJ])
            
            # Simple List
            st.markdown(f"""
            *   **{n_ft}** Fast-Track Candidates
            *   **{n_cr}** Crashing Opportunities
            *   **{n_cp}** Duration Compressions
            *   **{n_sw}** Resource Swaps
            *   **{n_fte}** FTE Adjustments
            """)
            
            if len(gen_actions) > 0:
                st.info("Go to **Schedule / Resource Recovery** tabs to apply.")
            else:
                st.caption("No recommended actions at this time.")

    # --- 3. Collapsable Network Diagram ---
    with st.expander("🕸️ Project Network Diagram (Click to View)", expanded=False):
        if df_schedule is not None and 'dag_graph' in locals() and dag_graph:
            try:
                # Create Graphviz object
                dot = graphviz.Digraph()
                dot.attr(rankdir='LR') # Left to right layout
                dot.attr(bgcolor='white')
                
                # Add nodes
                crit_lookup = {}
                if "on_critical_path" in df_schedule.columns:
                     temp_lookup = df_schedule.set_index("activity_id")["on_critical_path"].to_dict()
                     for k, v in temp_lookup.items():
                         try: crit_lookup[int(k)] = v
                         except: pass
    
                for node in dag_graph.nodes:
                    status = dep_validation.get(node, "OK")
                    is_crit = crit_lookup.get(node, False)
                    
                    color = "black"
                    fill = "white"
                    if "ERROR" in status:
                        color = "orange"
                    elif is_crit:
                        color = "red"
                        fill = "#ffe6e6" # Light red
                        
                    label = str(node)
                    
                    dot.node(str(node), label=label, color=color, fontcolor=color, fillcolor=fill, style='filled', penwidth="2" if is_crit else "1")
                
                # Add edges
                for u, v, data in dag_graph.edges(data=True):
                    edge_label = f"{data.get('type')}"
                    lag = data.get('lag', 0)
                    if lag != 0:
                        edge_label += f"{lag:+d}d"
                    
                    u_crit = crit_lookup.get(u, False)
                    v_crit = crit_lookup.get(v, False)
                    
                    edge_color = "gray"
                    penwidth = "1"
                    if u_crit and v_crit:
                        edge_color = "red"
                        penwidth = "2"
                        
                    dot.edge(str(u), str(v), label=edge_label, color=edge_color, penwidth=penwidth, fontcolor="gray")
                    
                st.graphviz_chart(dot, use_container_width=True)
                st.caption(f"Dependency Graph: {dag_graph.number_of_nodes()} Activities, {dag_graph.number_of_edges()} Dependencies. Red path is Critical.")
                
            except Exception as e:
                st.warning(f"Could not render graph: {e}")
        else:
             st.info("Network Diagram unavailable. Ensure schedule is uploaded and dependencies are valid.")

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
        
        with st.expander("ℹ️ Understanding the Recovery Logic (Click to Expand)", expanded=False):
            st.markdown("""
            **Enhanced Recovery Strategies:**
            
            1.  **🚀 Fast-Tracking (Parallel Processing)**
                *   *Trigger*: Critical tasks waiting for a predecessor to Finish.
                *   *Action*: Overlap tasks by changing dependency to **Start-to-Start (SS) + 2 days Lag**.
                *   *Benefit*: Successor starts much earlier (only 2 days after predecessor starts), saving significant time.

            2.  **💥 Task Crashing (Aggressive)**
                *   *Trigger*: Critical tasks needing immediate acceleration.
                *   *Action*: **Double the FTE** (Overtime/Double Shift).
                *   *Benefit*: Cuts duration in half but likely **overloads resources** and increases cost.

            3.  **📉 Duration Compression (Optimization)**
                *   *Trigger*: Critical tasks that are Active or Planned but delayed.
                *   *Action*: Reduce duration by up to **20%** (or 1 day min).
                *   *Benefit*: Recovers time directly on the driving path. Matches well with resource addition.
            """)
        
        # Filter Actions
        all_actions = st.session_state.get('generated_actions', [])
        compressions = [a for a in all_actions if a['type'] == recovery_engine.ACTION_COMPRESS]
        fast_tracks = [a for a in all_actions if a['type'] == recovery_engine.ACTION_FAST_TRACK]
        crashings = [a for a in all_actions if a['type'] == recovery_engine.ACTION_CRASHING]
        
        # --- SECTION 1: FAST-TRACKING (Result First) ---
        total_ft_savings = sum([a['parameters'].get('estimated_savings', 0) for a in fast_tracks])
        
        st.markdown(f"#### 1. Fast-Tracking (Potential Savings: **{total_ft_savings} Days**)")
        
        if fast_tracks:
            for i, action in enumerate(fast_tracks):
                # State
                action_id = action.get('id')
                is_applied = action_id in st.session_state['applied_actions']
                params = action.get('parameters', {})
                act_id = action.get('activity_id')
                pred_id = params.get('related_pred_id', 'Unknown')
                saving = params.get('estimated_savings', 0)
                
                title = f"{act_id}: Critical Path | Waiting for {pred_id}"
                
                with st.expander(title, expanded=False):
                    # Narrative
                    if "narrative" in action:
                        st.warning(action["narrative"])
                    else:
                        st.info(action['description'])
                    
                    st.divider()
                    
                    # Metrics
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Dependency", f"{pred_id} → {act_id}")
                    c1.caption("Current: Finish-to-Start")
                    
                    c2.metric("New Logic", "SS + 2d")
                    c2.caption(f"Start 2 days after {pred_id} starts")
                    
                    c3.metric("Est. Savings", f"{saving} days", delta="Schedule Gain")
                    
                    st.divider()
                    
                    # Button (Compact)
                    # Use columns to shrink button
                    b1, b2, b3 = st.columns([1, 1, 2])
                    with b1:
                        if is_applied:
                            st.button("Applied ✅", key=f"btn_ft_{i}", disabled=True)
                        else:
                            if st.button("Apply Fast-Track", key=f"btn_ft_{i}", type="primary"):
                                success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                                if success:
                                    st.success(msg)
                                    st.session_state['applied_actions'].add(action_id)
                                    st.rerun()
                                else:
                                    st.error(msg)
        else:
             st.info("No fast-tracking opportunities found (All critical dependencies are already parallel or invalid).")
             
        st.divider()

        # --- SECTION 2: TASK CRASHING (Aggressive) ---
        total_crash_sav = sum([float(a['parameters'].get('saved_days', 0)) for a in crashings])
        st.markdown(f"#### 2. Task Crashing (Potential Savings: **{total_crash_sav:.1f} Days**) ⚠️ Overload Risk")
        
        if crashings:
            for i, action in enumerate(crashings):
                 # State Check
                 action_id = action.get('id')
                 is_applied = action_id in st.session_state['applied_actions']
                 
                 params = action.get('parameters', {})
                 act_id = action.get('activity_id')
                 is_ov = params.get('is_overloaded', False)
                 
                 title = f"{act_id}: Critical | Save {params.get('saved_days', 0):.1f}d {'🔥' if is_ov else ''}"
                 
                 with st.expander(title, expanded=False):
                    if "narrative" in action:
                        st.warning(action["narrative"])
                        
                    st.divider()
                    
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Old FTE", f"{params.get('old_fte'):.1f}")
                    c2.metric("New FTE", f"{params.get('new_fte'):.1f}", delta="Double Effort")
                    c3.metric("Duration", f"{params.get('old_dur'):.1f} → {params.get('new_dur'):.1f}")
                    
                    st.divider()
                    
                    b1, b2, b3 = st.columns([1,1,2])
                    with b1:
                        if is_applied:
                             st.button("Applied ✅", key=f"btn_crash_{i}", disabled=True)
                        else:
                             if st.button("Apply Crash", key=f"btn_crash_{i}", type="primary", help="Doubles FTE. May cause overload."):
                                 success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                                 if success:
                                     st.success(msg)
                                     st.session_state['applied_actions'].add(action_id)
                                     st.rerun()
                                 else:
                                     st.error(msg)
        else:
            st.info("No crashing opportunities found.")
            
        st.divider()

        # --- SECTION 3: DURATION COMPRESSION (Result Second) ---
        total_cmp_rec = sum([int(a['parameters'].get('reduce_by_days', 0)) for a in compressions])
        
        st.markdown(f"#### 3. Duration Compression (Potential Recovery: **{total_cmp_rec} Days**)")
            
        if compressions:
            for i, action in enumerate(compressions):
                 # State Check
                 action_id = action.get('id')
                 is_applied = action_id in st.session_state['applied_actions']
                 
                 params = action.get('parameters', {})
                 act_id = action.get('activity_id')
                 delay_in = params.get('delay_carried_in', 0)
                 old_dur = params.get('old_dur', 0)
                 rec_red = params.get('reduce_by_days', 0)
                 
                 # Expander Title
                 title = f"{act_id}: Critical Path | {delay_in:.1f}d Delay"
                 
                 with st.expander(title, expanded=False):
                    # Row 1: Narrative
                    if "narrative" in action:
                        st.warning(action["narrative"])
                    else:
                        st.info(action['description'])
                        
                    st.divider()
                    
                    # Row 2: Metrics
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Current Duration", f"{int(old_dur)} days")
                    c1.caption("Planned Duration")
                    
                    c2.metric("Delay Impact", f"{delay_in:.1f} days")
                    c2.caption("Carried-In Delay")
                    
                    # Row 3: Input for New Duration
                    # Default to recommended (Old - Rec)
                    def_new = max(1, int(old_dur - rec_red))
                    
                    # We use a key based on action_id to persist input state
                    new_dur_input = c3.number_input(
                        "New Duration (Days)", 
                        min_value=1, 
                        max_value=int(old_dur),
                        value=def_new,
                        key=f"num_dur_{action_id}",
                        disabled=is_applied,
                        help="Manually override the new duration found by the engine."
                    )
                    
                    # Calculate Impact of User Input
                    user_savings = int(old_dur - new_dur_input)
                    c3.caption(f"Saving: {user_savings} days")

                    # Row 4: Impact Preview (Successors)
                    # Check graph for successors
                    dag = st.session_state.get('dag_graph_active')
                    if dag:
                        try:
                            # Ensure ID type match (int vs str)
                            node_id = act_id
                            if node_id not in dag.nodes:
                                 try: node_id = int(act_id)
                                 except: pass
                                 
                            if node_id in dag.nodes:
                                succs = list(dag.successors(node_id))
                                if succs:
                                    st.markdown(f"**📉 Impact Preview:** Modifying this task will affect **{len(succs)}** immediate successors: *{', '.join(map(str, succs[:5]))}{'...' if len(succs)>5 else ''}*")
                                else:
                                    st.caption("No immediate successors found.")
                        except Exception as e:
                            pass

                    st.divider()

                    # Row 5: Action Button (Compact)
                    b1, b2, b3 = st.columns([1, 1, 2])
                    with b1:
                        if is_applied:
                            st.button("Applied ✅", key=f"btn_comp_{i}", disabled=True)
                        else:
                            if st.button("Apply Compression", key=f"btn_comp_{i}", type="primary"):
                                # Inject user input into action parameters before applying
                                action['parameters']['new_dur_input'] = new_dur_input
                                
                                success, msg = recovery_engine.apply_action(st.session_state['recovery_schedule'], action)
                                if success:
                                    st.success(msg)
                                    st.session_state['applied_actions'].add(action_id)
                                    st.rerun()
                                else:
                                    st.error(msg)
        else:
            st.info("No compression opportunities found satisfying strict criteria (Active/Delayed/Critical).")


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
                        # Use markdown to properly render bold text and formatting
                        st.markdown(action["narrative"])
                    
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
        total_saved_days = sum([a['parameters'].get('saved_days', 0) for a in fte_adjs])
        
        st.markdown(f"#### FTE Adjustments (Total Days Recovered: **{total_saved_days:.1f} Days**)")
        if fte_adjs:
            for i, action in enumerate(fte_adjs):
                # Header Format
                with st.expander(f"{action.get('project_name')} | {action.get('resource_name')} (Critical Path Recovery)", expanded=False):
                    
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
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("📅 Recovered", f"{p.get('saved_days'):.1f} Days", delta="Time Saved")
                        with col2:
                            cost_impact = p.get('cost_impact', 0)
                            old_fte = p.get('old_fte', 0)
                            new_fte = p.get('new_fte', 0)
                            old_dur = p.get('old_dur', 0)
                            new_dur = p.get('new_dur', 0)
                            rate = p.get('resource_rate', 0)
                            work_hours = p.get('work_hours', 8)
                            old_cost = p.get('old_cost', 0)
                            new_cost = p.get('new_cost', 0)
                            
                            # Calculate total hours for display
                            old_total_hours = old_dur * old_fte * work_hours
                            new_total_hours = new_dur * new_fte * work_hours
                            
                            # Show cost impact with tooltip
                            if abs(cost_impact) > 0.01:  # Only show if significant difference
                                metric_col, help_col = st.columns([0.9, 0.1])
                                with metric_col:
                                    st.metric("💰 Cost Impact", f"${cost_impact:,.2f}", delta="Cost Change", delta_color="inverse" if cost_impact > 0 else "normal")
                                with help_col:
                                    st.markdown("<br>", unsafe_allow_html=True)
                                    with st.popover("ℹ️"):
                                        st.markdown("**Cost Calculation Details**")
                                        st.markdown("**Scenario:**")
                                        st.markdown(f"- **Old:** {old_dur:.1f} days × {old_fte} FTE = {old_total_hours:.1f} total hours")
                                        st.markdown(f"- **New:** {new_dur:.1f} days × {new_fte} FTE = {new_total_hours:.1f} total hours")
                                        st.markdown("")
                                        st.markdown("**Cost Calculation:**")
                                        st.markdown(f"- **Old Cost:** {old_dur:.1f} days × {work_hours:.1f} hrs/day × {old_fte} FTE × ${rate:.2f}/hr = **${old_cost:,.2f}**")
                                        st.markdown(f"- **New Cost:** {new_dur:.1f} days × {work_hours:.1f} hrs/day × {new_fte} FTE × ${rate:.2f}/hr = **${new_cost:,.2f}**")
                                        st.markdown(f"- **Cost Difference:** **${cost_impact:,.2f}**")
                            else:
                                # Cost remains same (total hours unchanged, just burned faster)
                                metric_col, help_col = st.columns([0.9, 0.1])
                                with metric_col:
                                    st.metric("💰 Cost Impact", "Same", delta="Total hours unchanged")
                                with help_col:
                                    st.markdown("<br>", unsafe_allow_html=True)
                                    with st.popover("ℹ️"):
                                        st.markdown("**Cost Calculation Details**")
                                        st.markdown("**Scenario:**")
                                        st.markdown(f"- **Old:** {old_dur:.1f} days × {old_fte} FTE = {old_total_hours:.1f} total hours")
                                        st.markdown(f"- **New:** {new_dur:.1f} days × {new_fte} FTE = {new_total_hours:.1f} total hours")
                                        st.markdown("")
                                        st.markdown("**Cost Calculation:**")
                                        st.markdown(f"- **Old Cost:** {old_dur:.1f} days × {work_hours:.1f} hrs/day × {old_fte} FTE × ${rate:.2f}/hr = **${old_cost:,.2f}**")
                                        st.markdown(f"- **New Cost:** {new_dur:.1f} days × {work_hours:.1f} hrs/day × {new_fte} FTE × ${rate:.2f}/hr = **${new_cost:,.2f}**")
                                        st.markdown(f"- **Cost Difference:** **${cost_impact:,.2f}**")
                                        st.markdown("")
                                        st.markdown("**Why Cost Stays the Same:**")
                                        st.markdown("When FTE increases and duration decreases proportionally:")
                                        st.markdown(f"- Total hours worked remain the same ({old_total_hours:.1f} hours in both cases)")
                                        st.markdown("- You're doing the same work, just faster")
                                        st.markdown("- Cost = Hours × Rate, so cost stays the same")
                        
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

    # Debug: Check columns
    # with st.expander("Debug: Column Types"):
    #     if st.session_state.get('recovery_schedule') is not None:
    #         st.write(st.session_state['recovery_schedule'].dtypes.astype(str))
    
    # Define Tooltips for Calculated Columns
    column_config = {
        "total_float_days": st.column_config.NumberColumn(
            "Total Float ℹ️",
            help="Total Float = LS - ES. Delay allowance before project finish is impacted."
        ),
        "on_critical_path": st.column_config.TextColumn(
            "Critical?",
            help="True if Total Float is zero (or minimal). These tasks drive the project finish date."
        ),
        "ES_date": st.column_config.DateColumn(
            "Early Start",
            help="Earliest date the activity can start based on predecessors."
        ),
        "EF_date": st.column_config.DateColumn(
            "Early Finish",
            help="Earliest date the activity can finish."
        ),
        "LS_date": st.column_config.DateColumn(
            "Late Start",
            help="Latest date the activity can start without delaying the project."
        ),
         "LF_date": st.column_config.DateColumn(
            "Late Finish",
            help="Latest date the activity can finish without delaying the project."
        ),
        "forecast_start_date": st.column_config.DateColumn(
            "Forecast Start",
            help="Projected start date based on actual progress and remaining duration."
        ),
         "forecast_finish_date": st.column_config.DateColumn(
            "Forecast Finish",
            help="Projected finish date based on actual progress and remaining duration."
        ),
        "remaining_duration_days": st.column_config.NumberColumn(
            "Rem. Dur.",
            help="Remaining work days calculated from Percent Complete or manual overrides."
        ),
        "planned_duration": st.column_config.NumberColumn(
            "Planned Dur.",
            help="Baseline duration from the schedule."
        )
    }

    # Show Recovery Workspace if active
    if st.session_state.get('recovery_schedule') is not None:
        st.info("Displaying RECOVERY WORKSPACE (Includes applied changes).")
        
        # Helper for styling
        # Since we can't easily iterate styles row by row efficiently without Styler which returns Styler obj,
        # we will use st.dataframe(styler).
        def highlight_changes(row):
            # Default style (no highlight)
            styles = [''] * len(row)
            
            # --- 1. Identify Source of Change (Action Metadata) ---
            lct = str(row.get("last_change_type", "None"))
            
            highlight_source = 'background-color: #ffeba1; color: black; font-weight: bold' # Light Orange for Action Source
            highlight_diff = 'background-color: #ffffdd; color: black'   # Light Yellow for Propagated Change
            
            # Helper to mark col
            def mark_col(col_name, style):
                try:
                    idx = row.index.get_loc(col_name)
                    styles[idx] = style
                except KeyError:
                    pass

            # Highlight Action Inputs based on Type
            # Highlight Action Inputs based on Type
            if lct == recovery_engine.ACTION_FTE_ADJ:
                mark_col("fte_allocation", highlight_source)
                mark_col("remaining_duration_days", highlight_source)
                # Highlight Cost Impacts for FTE (User Req)
                mark_col("remaining_cost", highlight_source)
                mark_col("eac_cost", highlight_source)
                mark_col("planned_cost", highlight_source)
            elif lct == recovery_engine.ACTION_CRASHING:
                mark_col("fte_allocation", highlight_source) # Changed field
                mark_col("remaining_duration_days", highlight_source)
                mark_col("remaining_cost", highlight_source) # Cost/Overload impact
            elif lct == recovery_engine.ACTION_COMPRESS:
                mark_col("remaining_duration_days", highlight_source)
                mark_col("planned_duration", highlight_source)
            elif lct == recovery_engine.ACTION_RES_SWAP:
                mark_col("resource_id", highlight_source) # Important
                mark_col("resource_name", highlight_source)
                mark_col("cost_per_hour", highlight_source)
                mark_col("remaining_cost", highlight_source) # Cost savings
                mark_col("planned_cost", highlight_source) # New Rate * Planned Hours

            # --- 2. Generic Diff vs Baseline (Propagation) ---
            # Compare this row against the original df_schedule
            # We assume df_schedule is available in local scope (it is, from top of script)
            if df_schedule is not None:
                try:
                    # Find baseline row by activity_id
                    # Optimization: create lookup dict outside if slow, but for MVP:
                    act_id = float(row["activity_id"])
                    
                    # Ensure types match
                    # baseline might have int index or activity_id column
                    # Let's filter
                    base_row = df_schedule[df_schedule["activity_id"] == act_id]
                    
                    if not base_row.empty:
                        base_row = base_row.iloc[0]
                        
                        # Columns to check for diffs (Expanded to cover ALL recalculations)
                        check_cols = [
                            "remaining_duration_days", "planned_duration",
                            "planned_finish", "ES_date", "EF_date", "LS_date", "LF_date", 
                            "forecast_finish_date", "forecast_start_date",
                            "remaining_cost", "eac_cost", "planned_cost", "actual_cost",
                            "remaining_load_hours", "planned_load_hours", "actual_load_hours",
                            "total_float_days"
                        ]
                        
                        for col in check_cols:
                            if col in row and col in base_row:
                                val_new = row[col]
                                val_old = base_row[col]
                                
                                # Compare
                                is_diff = False
                                try:
                                    # Normalize for comparison
                                    v1 = str(val_new).strip().lower().replace(".0", "")
                                    v2 = str(val_old).strip().lower().replace(".0", "")
                                    if v1 != v2 and v1 != "nan" and v1 != "none":
                                        is_diff = True
                                except:
                                    pass
                                
                                if is_diff:
                                    # Don't overwrite Source Highlight
                                    curr_style = styles[row.index.get_loc(col)]
                                    if not curr_style:
                                        mark_col(col, highlight_diff)

                except Exception as e:
                    pass
            
            # User said "only highlight particular cell which changed". i'll stick to that.
            
            return styles

        rec_df = st.session_state['recovery_schedule'].copy()
        
        # Recalculate costs for recovery schedule if resource data is available
        if df_resource is not None:
            try:
                cost_df_results = cost_engine.calculate_costs(rec_df, df_resource)
                cost_lookup = cost_df_results.set_index("activity_id")
                
                cost_cols = ["planned_load_hours", "planned_cost", "actual_load_hours", 
                             "actual_cost", "remaining_load_hours", "remaining_cost", "eac_cost"]
                
                for col in cost_cols:
                    if col in cost_lookup.columns:
                        rec_df[col] = rec_df["activity_id"].map(cost_lookup[col])
            except Exception as ce:
                st.warning(f"Cost recalculation error: {ce}")
        
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
             st.dataframe(
                 rec_df.style.apply(highlight_changes, axis=1), 
                 use_container_width=True,
                 column_config=column_config
             )
        except Exception as e:
             st.warning(f"Could not apply styling: {e}")
             st.dataframe(rec_df, use_container_width=True)
             
        st.caption(f"Rows: {len(rec_df)} | Columns: {len(rec_df.columns)}")
            
    # The original elif/else block for df_schedule is now moved into tabs[5]
    # and will be handled there.

with tabs[5]: # Project Data
    st.subheader("Project Schedule Data")
    
    with st.expander("ℹ️ Data Dictionary & Calculations", expanded=False):
        st.markdown("""
        **Critical Path Method (CPM) Metrics:**
        *   **ES / EF (Early Start/Finish)**: The earliest dates the task can start/finish based on predecessor logic.
        *   **LS / LF (Late Start/Finish)**: The latest dates the task can start/finish without delaying the project.
        *   **Total Float**: `LF - EF`. The amount of time a task can slip before impacting the finish date. Zero Float = Critical Path.
        
        **Forecasting Metrics:**
        *   **Forecast Finish**: `Actual Start + Actual Duration + Remaining Duration`. Updates dynamically as progress is reported.
        *   **Delay Carried In**: Delay passed down from predecessors.
        *   **Total Schedule Delay**: `Forecast Finish - Planned Finish`.
        
        **Cost & EVM Metrics:**
        *   **Planned Cost**: `Planned Duration * Planned FTE * Resource Rate`. Budgeted cost.
        *   **Actual Cost**: `Actual Duration * Planned FTE * Resource Rate`. Cost incurred so far.
        *   **Remaining Cost**: `Remaining Duration * Current FTE * Resource Rate`. Cost to complete.
        *   **EAC (Estimate at Completion)**: `Actual Cost + Remaining Cost`. Total projected cost.
        *   **Cost Variance**: `Planned Cost - EAC`. Negative means over budget.
        """)

    if df_schedule is not None:
        st.dataframe(df_schedule, use_container_width=True, column_config=column_config)
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
