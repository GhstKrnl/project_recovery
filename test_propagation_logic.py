import pandas as pd
import numpy as np
import dag_engine
import cpm_engine
import recovery_engine

def test_propagation():
    # 1. Create Baseline Data (FS Chain: 2 -> 3 -> 4)
    data = {
        "activity_id": [2, 3, 4],
        "activity_name": ["Task A", "Task B", "Task C"],
        "planned_start": ["2026-01-01", "2026-01-06", "2026-01-12"],
        "planned_finish": ["2026-01-05", "2026-01-09", "2026-01-16"],
        "planned_duration": [3, 4, 5],
        "remaining_duration_days": [3, 4, 5],
        "predecessor_id": ["", "2FS", "3FS"],
        "resource_id": [1, 2, 3],
        "fte_allocation": [1, 1, 1],
        "on_critical_path": [True, True, True]
    }
    df = pd.DataFrame(data)
    project_start = "2026-01-01"
    
    print("--- BASELINE ---")
    print(df[["activity_id", "planned_start", "planned_finish", "remaining_duration_days"]])

    # 2. Build Graph
    G, _ = dag_engine.build_dag_and_validate(df)
    
    # 3. Apply Compression to Task A (2)
    action = {
        "type": recovery_engine.ACTION_COMPRESS,
        "activity_id": 2,
        "parameters": {"reduce_by_days": 2} # Compress from 3 -> 1
    }
    recovery_engine.apply_action(df, action)
    
    print("\n--- AFTER COMPRESSION (Before CPM) ---")
    print(df[["activity_id", "planned_start", "planned_finish", "remaining_duration_days"]])
    
    # 4. Re-Run CPM on the modified 'remaining_duration_days'
    # NOTE: cpm_engine usually looks at 'planned_duration'. 
    # We must ensure it uses the 'recovery' duration.
    # We'll temporarily point it to 'remaining_duration_days' or update 'planned_duration'.
    
    # In my recovery engine logic, I should probably update 'planned_duration' too for the CPM.
    # OR the CPM engine should be flexible.
    
    # For this test, let's update planned_duration to match remaining_duration_days
    df["planned_duration"] = df["remaining_duration_days"]
    
    cpm_results = cpm_engine.run_cpm(df, G)
    durations = {row["activity_id"]: row["planned_duration"] for _, row in df.iterrows()}
    enriched = cpm_engine.convert_offsets_to_dates(cpm_results, project_start, durations)
    
    # Update DF with results
    for aid, res in enriched.items():
        idx = df[df["activity_id"] == aid].index[0]
        df.at[idx, "planned_start"] = res["ES_date"]
        df.at[idx, "planned_finish"] = res["EF_date"]

    print("\n--- AFTER CPM RE-CALCULATION ---")
    print(df[["activity_id", "planned_start", "planned_finish", "remaining_duration_days", "on_critical_path"]])

if __name__ == "__main__":
    test_propagation()
