import streamlit as st
import pandas as pd
import utils
import dag_engine
import cpm_engine
import forecasting_engine
import cost_engine
import evm_engine
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

# Session State for Analysis
if 'analyzed' not in st.session_state:
    st.session_state['analyzed'] = False

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
    st.info("Schedule Recovery Module Placeholder")

with tabs[2]: # Resource Recovery
    st.subheader("Resource Recovery & Overload Analysis")
    if resource_stats:
        # Display Overloads
        res_df = pd.DataFrame.from_dict(resource_stats, orient='index').reset_index().rename(columns={"index": "Resource ID"})
        st.dataframe(res_df)
        
        total_overloads = res_df["overload_days_count"].sum()
        st.metric("Total Resource Overload Days", f"{total_overloads} days")
    else:
        st.info("No Resource Data Loaded or No Overloads Detected.")

with tabs[3]: # Cost Recovery
    st.subheader("Project Cost Recovery")
    if not cost_df_results.empty:
        # Aggregations
        total_planned = cost_df_results["planned_cost"].sum()
        total_actual = cost_df_results["actual_cost"].sum()
        total_eac = cost_df_results["eac_cost"].sum()
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Planned Cost", f"${total_planned:,.2f}")
        c2.metric("Total Actual Cost", f"${total_actual:,.2f}")
        c3.metric("EAC (Estimate At Completion)", f"${total_eac:,.2f}", delta=f"{total_planned - total_eac:,.2f}")
        
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
    if df_schedule is not None:
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
