import pandas as pd
import numpy as np

# Paths
schedule_path = r"C:\Users\Adnan\OneDrive - I2e Consulting\Documents\Apps\project_recovery\csv\project_schedule_3.csv"
cost_path = r"C:\Users\Adnan\OneDrive - I2e Consulting\Documents\Apps\project_recovery\csv\cost.csv"

def run_test():
    print("--- Loading Data ---")
    try:
        df_schedule = pd.read_csv(schedule_path)
        df_resource = pd.read_csv(cost_path)
        print("Loaded Schedule:", len(df_schedule))
        print("Loaded Resource:", len(df_resource))
    except Exception as e:
        print(f"Error loading: {e}")
        return

    # Check Types
    print("\n--- Column Types ---")
    print("Schedule 'activity_id':", df_schedule["activity_id"].dtype)
    print("Schedule 'resource_id':", df_schedule["resource_id"].dtype)
    print("Resource 'resource_id':", df_resource["resource_id"].dtype)

    # Mimic Cost Engine (mock cost_df_results)
    # We assume cost engine output connects activity_id and resource_id
    # for testing, we just merge schedule and resource manually to get assignments
    
    # "assignments likely from cost engine which might use string or int"
    # app.py: for rid, grp in cost_df_results.groupby("resource_id"): ...
    
    # Let's create a mock cost_df_results from df_schedule directly since logic relies on what's in schedule
    cost_df_results = df_schedule[["activity_id", "resource_id"]].copy().dropna()
    
    print("\n--- Testing Logic ---")
    
    # 1. unique assignments
    res_activity_map = {}
    for rid, grp in cost_df_results.groupby("resource_id"):
        res_activity_map[str(rid)] = grp["activity_id"].unique().tolist()
        
    print(f"Map created for {len(res_activity_map)} resources.")
    
    # Iterate resources
    # app.py: for rid, stats in resource_stats.items():
    # we'll just iterate df_resource IDs
    resource_ids = df_resource["resource_id"].unique()
    
    for rid in resource_ids:
        print(f"\nChecking Resource: {rid} (Type: {type(rid)})")
        
        assignments = res_activity_map.get(str(rid), [])
        print(f"  Assignments: {assignments}")
        
        # 2. Lookup Project IDs
        proj_str = "None"
        if assignments:
            # Logic from app.py
            # "activity_id".astype(str).isin([str(x) for x in assignments])
            rel_df = df_schedule[df_schedule["activity_id"].astype(str).isin([str(x) for x in assignments])]
            if "project_id" in rel_df.columns:
                 unique_projs = rel_df["project_id"].dropna().unique()
                 proj_str = ", ".join(map(str, unique_projs))
        print(f"  Project ID Found: '{proj_str}'")
        
        # 3. Lookup Resource Name
        r_name = "Unknown"
        # Logic from app.py
        # df_resource["resource_id"].astype(str) == str(rid)
        r_row = df_resource[df_resource["resource_id"].astype(str) == str(rid)]
        if not r_row.empty:
            r_name = r_row.iloc[0].get("resource_name", "Unknown")
        
        print(f"  Resource Name Found: '{r_name}'")

if __name__ == "__main__":
    run_test()
