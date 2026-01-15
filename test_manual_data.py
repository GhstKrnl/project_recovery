import pandas as pd
import recovery_engine
import root_cause_engine

# Paths
schedule_path = r"C:\Users\Adnan\OneDrive - I2e Consulting\Documents\Apps\project_recovery\csv\project_schedule_3.csv"
resource_path = r"C:\Users\Adnan\OneDrive - I2e Consulting\Documents\Apps\project_recovery\csv\cost.csv"

def run_manual_test():
    print("--- Loading Data ---")
    try:
        df_schedule = pd.read_csv(schedule_path)
        print(f"Schedule Loaded. Rows: {len(df_schedule)}")
        print("Columns:", df_schedule.columns.tolist())
    except Exception as e:
        print(f"Error loading schedule: {e}")
        return

    try:
        df_resource = pd.read_csv(resource_path)
        print(f"Resource Loaded. Rows: {len(df_resource)}")
        print("Columns:", df_resource.columns.tolist())
    except Exception as e:
        print(f"Error loading resource: {e}")
        df_resource = None

    print("\n--- Running Root Cause Analysis ---")
    # Mocking resource stats for now or need to calculate them if logic requires
    # apps.py calculates resource_stats using cost_engine. 
    # For recovery testing, we mainly need the DF and root causes.
    
    # We need to run classification.
    # Root Cause Engine expects df_schedule and resource_stats (dict).
    # We can pass empty resource_stats if we just want to test Schedule/Cost logic, 
    # or we need to build it.
    
    # Let's mock a basic overload if we can't fully run cost engine here easily without app context
    resource_stats = {} 
    
    # We need to ensure numeric columns for RCA
    df_schedule["total_float_days"] = pd.to_numeric(df_schedule.get("total_float_days"), errors='coerce').fillna(0)
    df_schedule["total_schedule_delay"] = pd.to_numeric(df_schedule.get("total_schedule_delay"), errors='coerce').fillna(0)
    # cost columns...
    
    rc_df = root_cause_engine.execute_root_cause_analysis(df_schedule, resource_stats)
    print(f"Root Causes Found: {len(rc_df)}")
    if not rc_df.empty:
        print(rc_df[["Activity", "Root Cause Category", "Impact Days"]].head())
        
    print("\n--- Generating Recovery Actions ---")
    # Init workspace
    rec_ws = recovery_engine.init_recovery_workspace(df_schedule)
    
    # Generate
    actions = recovery_engine.generate_actions(rec_ws, resource_stats, df_resource, rc_df)
    
    print(f"Actions Generated: {len(actions)}")
    for i, a in enumerate(actions):
        print(f"Action {i+1}: {a['type']} - {a['description']}")
        
    print("\n--- Testing Application ---")
    if actions:
        success, msg = recovery_engine.apply_action(rec_ws, actions[0])
        print(f"Applied Action 1: {success} - {msg}")
        
        # Check change log
        row = rec_ws[rec_ws["activity_id"] == actions[0]["activity_id"]].iloc[0]
        print(f"Audit Log: Type={row['last_change_type']}, ID={row['last_change_id']}")

if __name__ == "__main__":
    run_manual_test()
