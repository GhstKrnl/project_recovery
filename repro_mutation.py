import pandas as pd
import recovery_engine

def test_mutation():
    print("--- Test Mutation ---")
    # Setup simple DF
    df = pd.DataFrame({
        "activity_id": [101, 102],
        "resource_id": ["RES_A", "RES_B"],
        "resource_rate": [100.0, 100.0],
        "planned_duration": [5, 5],
        "remaining_duration_days": [5, 5],
        "actual_cost": [0,0],
        "planned_cost": [500, 500]
    })
    
    # Init Workspace
    ws = recovery_engine.init_recovery_workspace(df)
    print("Initial Workspace:")
    print(ws[["activity_id", "resource_id", "last_change_type"]])
    
    # Define Action
    action = {
        "type": "Resource Swap",
        "activity_id": 101,
        "description": "Swap test",
        "parameters": {
            "new_res": "RES_NEW" # Simple swap
        }
    }
    
    # Apply
    print(f"\nApplying Action: {action['type']} -> RES_NEW")
    success, msg = recovery_engine.apply_action(ws, action)
    print(f"Result: {success} - {msg}")
    
    # Check Mutation
    row = ws[ws["activity_id"] == 101].iloc[0]
    print("\nPost-Mutation Row:")
    print(row[["activity_id", "resource_id", "last_change_type", "last_change_id"]])
    
    # Verify
    if row["resource_id"] == "RES_NEW" and row["last_change_type"] == "Resource Swap":
        print("\nSUCCESS: Mutation verified.")
    else:
        print("\nFAILURE: Data did not change as expected.")

if __name__ == "__main__":
    test_mutation()
