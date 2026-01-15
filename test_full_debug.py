import pandas as pd
import cost_engine
import forecasting_engine
import os
import utils

def full_debug():
    base_dir = r"c:/Users/Adnan/OneDrive - I2e Consulting/Documents/Apps/project_recovery/csv"
    sched_path = os.path.join(base_dir, "project_schedule.csv")
    res_path = os.path.join(base_dir, "resource_cost_unit.csv")
    
    print(f"Loading Schedule: {sched_path}")
    df_schedule = pd.read_csv(sched_path)
    
    print(f"Loading Resources: {res_path}")
    df_resource = pd.read_csv(res_path)
    
    # 1. Pipeline Step 1: Initialize ID if needed (Simulate App)
    if "_temp_id" not in df_schedule.columns:
        df_schedule["_temp_id"] = df_schedule.index
        
    # 2. Pipeline Step 2: Forecasting
    # This might shift dates!
    print("\nRunning Forecasting Engine...")
    try:
        df_schedule = forecasting_engine.execute_forecasting(df_schedule)
        print("Forecasting Complete.")
        
        # Check dates for Resource 1 Activities
        print("\nDates AFTER Forecasting (Resource 1):")
        r1_tasks = df_schedule[df_schedule["resource_id"].astype(str).str.strip() == "1"]
        print(r1_tasks[["activity_id", "planned_start", "forecast_start_date", "forecast_finish_date"]])
        
    except Exception as e:
        print(f"Forecasting Error: {e}")
        return

    # 3. Pipeline Step 3: Cost & Availability
    print("\nRunning Cost Engine...")
    enriched_df = cost_engine.calculate_costs(df_schedule, df_resource)
    stats = cost_engine.check_resource_availability(enriched_df)
    
    r1_stats = stats.get("1", stats.get(1, {}))
    print(f"\nResource 1 Stats (Final): {r1_stats}")
    
    # 4. Detailed Day Check
    daily = {}
    print("\nDetailed Daily FTE Map:")
    for _, row in enriched_df.iterrows():
        rid = str(row.get("resource_id", "")).strip()
        if rid == "1":
            start = row.get("forecast_start_date")
            finish = row.get("forecast_finish_date")
            fte = row.get("fte_allocation", 0)
            act_id = row.get("activity_id")
            
            try:
                dates = pd.bdate_range(start=start, end=finish)
                for d in dates:
                    d_str = d.strftime('%Y-%m-%d')
                    daily[d_str] = daily.get(d_str, 0) + fte
                    if daily[d_str] > 1.0:
                         print(f"  [OVERLOAD] {d_str}: Sum={daily[d_str]} (Added {fte} from Task {act_id})")
            except: pass

if __name__ == "__main__":
    full_debug()
