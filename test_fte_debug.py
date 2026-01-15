import pandas as pd
import recovery_engine
import cost_engine
import forecasting_engine
import os
import uuid

def debug_fte():
    base_dir = r"c:/Users/Adnan/OneDrive - I2e Consulting/Documents/Apps/project_recovery/csv"
    sched_path = os.path.join(base_dir, "project_schedule.csv")
    res_path = os.path.join(base_dir, "resource_cost_unit.csv")
    
    print(f"Loading Schedule: {sched_path}")
    df_schedule = pd.read_csv(sched_path)
    
    print(f"Loading Resources: {res_path}")
    df_resource = pd.read_csv(res_path)
    
    # Init temp ID
    if "_temp_id" not in df_schedule.columns:
        df_schedule["_temp_id"] = df_schedule.index
        
    # 0. Need CPM for Critical Path
    # 0. Need CPM for Critical Path
    import cpm_engine
    import dag_engine
    import utils
    
    print("Running CPM Engine to populate 'on_critical_path'...")
    try:
        # Build Graph
        dag, _ = dag_engine.build_dag_and_validate(df_schedule)
        # Calc Dates
        cpm_results = cpm_engine.run_cpm(df_schedule, dag)
        cpm_df = pd.DataFrame.from_dict(cpm_results, orient='index')
        
        # Merge 'on_critical_path'
        if "on_critical_path" in cpm_df.columns:
            df_schedule["on_critical_path"] = df_schedule["_temp_id"].map(cpm_df["on_critical_path"])
        
        # MOCK Remaining Duration (simulate 0 progress)
        if "remaining_duration_days" not in df_schedule.columns:
            df_schedule["remaining_duration_days"] = df_schedule["planned_duration"]
            
    except Exception as e:
        print(f"CPM Error: {e}")
        return

    # 1. Inspect CSV's critical path column directly first
    print("\nCalculated 'on_critical_path' values:")
    print(df_schedule[["activity_id", "on_critical_path", "fte_allocation", "remaining_duration_days"]].head(10))
    
    # 2. Mock Recovery Generation
    # We will manually call the loop logic for one target row to see why it fails
    # Let's find a candidate: 0.5 FTE
    
    candidates = df_schedule[df_schedule["fte_allocation"] < 1.0]
    print(f"\nCandidates (< 1.0 FTE): {len(candidates)}")
    if not candidates.empty:
        print(candidates[["activity_id", "fte_allocation", "on_critical_path", "remaining_duration_days"]])
        
    # Pick one to debug
        # target = candidates.iloc[0] # Old logic
        
        # New Logic: Target Activity 9
        target_df = candidates[candidates["activity_id"].astype(str) == "9"]
        if target_df.empty:
            print("Activity 9 is not in candidate list (maybe FTE >= 1.0?)")
            return
            
        target = target_df.iloc[0]
        act_id = target["activity_id"]
        curr_res = target["resource_id"]
        curr_fte = target["fte_allocation"]
        on_crit = target.get("on_critical_path", "N/A")
        rem_dur = target.get("remaining_duration_days", "N/A")
        
        print(f"\nDebugging Activity: {act_id}")
        print(f"Resource: {curr_res}")
        print(f"FTE: {curr_fte}")
        print(f"Critical: {on_crit}")
        print(f"Remaining Duration: {rem_dur}")
        
        # Also print Float if available (need to map it from cpm_results)
        # We didn't map float in the script earlier, let's look at cpm_df for this ID
        if str(act_id) in cpm_results:
             print(f"CPM Data: {cpm_results[str(act_id)]}")
        elif int(act_id) in cpm_results:
             print(f"CPM Data: {cpm_results[int(act_id)]}")
             
        # Test Resource Lookup Logic explicitly
        print("\nTest Resource Lookup:")
        # CURRENT CODE LOGIC:
        # r_row = df_resource[df_resource["resource_id"] == current_res]
        
        # Case 1: Raw types
        r_row = df_resource[df_resource["resource_id"] == curr_res]
        print(f"Direct Match Rows: {len(r_row)}")
        
        # Case 2: String types (What we likely need)
        r_row_str = df_resource[df_resource["resource_id"].astype(str).str.strip() == str(curr_res).strip()]
        print(f"String Clean Match Rows: {len(r_row_str)}")
        
        if not r_row.empty:
             val = r_row.iloc[0]["resource_max_fte"]
             print(f"Max FTE (Direct): {val} (Type: {type(val)})")
             
        if not r_row_str.empty:
             val = r_row_str.iloc[0]["resource_max_fte"]
             print(f"Max FTE (String): {val} (Type: {type(val)})")
             
        # Test Condition
        # if current_fte < max_fte:
        # Check types
        try:
             max_f = float(r_row_str.iloc[0]["resource_max_fte"]) if not r_row_str.empty else 1.0
             curr_f = float(curr_fte)
             print(f"Condition: {curr_f} < {max_f} ? {curr_f < max_f}")
        except Exception as e:
             print(f"Comparison Error: {e}")

if __name__ == "__main__":
    debug_fte()
