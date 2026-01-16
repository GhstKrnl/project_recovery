import pandas as pd
import recovery_engine
from recovery_engine import ACTION_FAST_TRACK
import traceback
import sys

# Mock Data:
cols = ["activity_id", "activity_name", "planned_duration", "remaining_duration_days", 
        "predecessors", "predecessor_id", "actual_start", "actual_finish", 
        "delay_carried_in", "total_float_days", "on_critical_path"]

data = [
    [1, "Task A", 5, 5, "", "", "2024-01-01", None, 0, 0, True],
    [2, "Task B", 5, 5, "1", "1", None, None, 0, 0, True]
]

df = pd.DataFrame(data, columns=cols)

print("--- Initial Data ---")
print(df[["activity_id", "predecessors", "on_critical_path"]])

# Generate Actions
print("\n--- Generating Actions ---")
try:
    # generate_actions(df_schedule, resource_stats, df_resource, root_causes)
    actions = recovery_engine.generate_actions(df, {}, None, pd.DataFrame())
    ft_actions = [a for a in actions if a["type"] == ACTION_FAST_TRACK]

    found = False
    for a in ft_actions:
        if a["activity_id"] == 2:
            found = True
            print(f"Action Found: {a['description']}")
            print(f"Narrative: {a.get('narrative')}")
            
            # Verify Savings
            sav = a['parameters'].get('estimated_savings')
            if sav == 3:
                print("SUCCESS: Estimated Savings is 3 days.")
            else:
                print(f"FAILURE: Estimated Savings is {sav}, expected 3.")
                
            # Apply Logic
            print("Applying action...")
            suc, msg = recovery_engine.apply_action(df, a)
            print(f"Apply Result: {suc} - {msg}")
            
            # Verify Mutation
            new_pred = df.loc[1, "predecessor_id"] 
            print(f"New Predecessor String: '{new_pred}'")
            
            # Verify Metadata
            lct = df.loc[1, "last_change_type"]
            print(f"Metadata Type: {lct}")
            if lct == ACTION_FAST_TRACK:
                print("SUCCESS: Metadata set.")

    if not found:
        print("FAILURE: No Fast-Track action generated for Activity 2.")

except Exception:
    print("CRITICAL ERROR IN SCRIPT:")
    traceback.print_exc()
    sys.exit(1)
