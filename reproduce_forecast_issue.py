import pandas as pd
import networkx as nx
import forecasting_engine
import numpy as np

# Mock Data
# 1. Completed Late Task (A) -> Successor (B)
# 2. In-Progress Late Task (C) -> Successor (D)

data = {
    "activity_id": [1, 2, 3, 4],
    "planned_duration": [5, 5, 5, 5],
    "baseline_1_start": ["2024-01-01", "2024-01-08", "2024-01-01", "2024-01-08"],
    "baseline_1_finish": ["2024-01-05", "2024-01-12", "2024-01-05", "2024-01-12"],
    
    # CPM Results (Simulated)
    "ES_date": ["2024-01-01", "2024-01-08", "2024-01-01", "2024-01-08"],
    "EF_date": ["2024-01-05", "2024-01-12", "2024-01-05", "2024-01-12"],
    
    # Actuals
    # A: Started late (Jan 3), Finished Late (Jan 9) -> Delay = 2 days (Jan 5 vs Jan 9.. wait. 5th is Fri. 8th Mon. 9th Tue. 2 working days late?)
    # B: Not started
    # C: Started late (Jan 3). Not Finished. Forecast Finish will use CPM EF? 
    #    Wait, my logic in forecasting_engine uses CPM EF if in-progress and no better estimate. 
    #    But CPM EF comes from dates. If CPM wasn't re-run with actuals, CPM EF is still baseline-ish.
    #    The Issue: forecasting_engine doesn't re-calculate Schedule! Only CPM engine does.
    #    If CPM EF is not updated, then Forecast Finish = CPM EF = Baseline Finish. So Delay = 0.
    
    # Key Insight: Does calculate_forecasts rely on CPM being up to date?
    # Yes. "res['forecast_finish_date'] = cpm_ef" for In-Progress tasks.
    
    "actual_start": ["2024-01-03", None, "2024-01-03", None],
    "actual_finish": ["2024-01-09", None, None, None]
}

df = pd.DataFrame(data)

# Build Graph
G = nx.DiGraph()
G.add_edge(1, 2) # A -> B
G.add_edge(3, 4) # C -> D

# We need to simulate the CPM EF being updated IF we want the forecast to show delay for in-progress tasks,
# OR forecasting_engine should calculate a new forecast based on Rem Dur.
# Currently forecasting_engine says: 
# if act_start: ... res["forecast_finish_date"] = cpm_ef
# This implies cpm_ef MUST be 'Forecast Early Finish' from the CPM run.

# Let's run it and see what we get with these inputs.
print("--- TEST DATA ---")
print(df)

results = forecasting_engine.calculate_forecasts(df, G)

print("\n--- RESULTS ---")
for aid, res in results.items():
    print(f"ID {aid}: Delay Carried In = {res['delay_carried_in']}, Forecast Fin = {res['forecast_finish_date']}")

