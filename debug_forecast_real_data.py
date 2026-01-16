import pandas as pd
import dag_engine
import forecasting_engine
import cpm_engine
import numpy as np

# Load Real Data
try:
    df = pd.read_csv("csv/project_schedule.csv")
    print(f"Loaded {len(df)} rows from csv/project_schedule.csv")
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit()

# Validate numeric IDs (dag engine expects ints usually?)
# app.py does: df_schedule["_temp_id"] = pd.to_numeric(df_schedule["activity_id"], errors='coerce')
# Let's verify ID column type
print(f"Activity ID Type: {df['activity_id'].dtype}")

# Build DAG
dag_graph, validation = dag_engine.build_dag_and_validate(df)
print(f"DAG Nodes: {dag_graph.number_of_nodes()}, Edges: {dag_graph.number_of_edges()}")

# Run CPM properly
durations = cpm_engine.calculate_durations(df)
cpm_res = cpm_engine.run_cpm(df, dag_graph)

# Convert dates
p_start = df["planned_start"].min()
cpm_dates = cpm_engine.convert_offsets_to_dates(cpm_res, p_start, durations)
cpm_df = pd.DataFrame.from_dict(cpm_dates, orient='index')

# Map CPM to DF
cols = ["ES", "EF", "ES_date", "EF_date", "planned_duration"]
# Use integer mapping logic
df["_temp_id"] = pd.to_numeric(df["activity_id"], errors='coerce')
for col in cols:
    if col in cpm_df.columns:
        df[col] = df["_temp_id"].map(cpm_df[col])

print("\n--- Rows with Actual Start but No Finish (In Progress) ---")
in_progress = df[df["actual_start"].notna() & df["actual_finish"].isna()]
print(in_progress[["activity_id", "actual_start", "planned_duration", "ES_date", "EF_date"]])

# Run Forecast
print("\n--- Running Forecast Engine ---")
results = forecasting_engine.calculate_forecasts(df, dag_graph)

# Check Results for In-Progress Successors
print("\n--- Detailed Results ---")
for node, res in results.items():
    # Only show if interesting (delay carried in > 0 OR is in progress)
    is_interesting = (res["delay_carried_in"] > 0) or (res["percent_complete"] > 0 and res["percent_complete"] < 100) or (df.loc[df["activity_id"]==node, "actual_start"].notna().any() and df.loc[df["activity_id"]==node, "actual_finish"].isna().any())
    
    # Check if ANY predecessor was in progress
    preds = list(dag_graph.predecessors(node))
    pred_in_progress = False
    for p in preds:
        if p in in_progress["activity_id"].values:
            pred_in_progress = True
            
    if is_interesting or pred_in_progress:
        print(f"Node {node}:")
        print(f"  Status: {'In Progress' if res['forecast_start_date'] == res['forecast_finish_date'] else 'Check'}") 
        print(f"  Forecast Start: {res['forecast_start_date']}, Forecast Finish: {res['forecast_finish_date']}")
        print(f"  Delay Carried In: {res['delay_carried_in']}")
        print(f"  Preds: {preds}")
        if preds:
            for p in preds:
                 p_res = results.get(p)
                 if p_res:
                     print(f"    Pred {p} Forecast Fin: {p_res.get('forecast_finish_date')}")

