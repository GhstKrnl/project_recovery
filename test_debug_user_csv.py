import pandas as pd
import cost_engine
import os

def debug_user_data():
    base_dir = r"c:/Users/Adnan/OneDrive - I2e Consulting/Documents/Apps/project_recovery/csv"
    sched_path = os.path.join(base_dir, "project_schedule.csv")
    res_path = os.path.join(base_dir, "resource_cost_unit.csv")
    
    print(f"Loading Schedule: {sched_path}")
    df_schedule = pd.read_csv(sched_path)
    print(f"Schedule Rows: {len(df_schedule)}")
    
    dupes = df_schedule[df_schedule.duplicated(subset=['activity_id'], keep=False)]
    print(f"Duplicate Activities Found: {len(dupes)}")
    if not dupes.empty:
        print(dupes[["activity_id", "project_id"]].head())

    print(df_schedule[["activity_id", "resource_id", "planned_start", "planned_finish"]].head())
    
    print(f"\nLoading Resources: {res_path}")
    df_resource = pd.read_csv(res_path)
    print(f"Resource Rows: {len(df_resource)}")
    print(df_resource[["resource_id", "resource_name"]].head())
    
    # 1. Run Calculation
    # We need to simulate 'forecast' dates logic if they don't exist
    # The app calculates forecasting before cost.
    # We will just copy planned to forecast for this test as that's the base state.
    if "forecast_start_date" not in df_schedule.columns:
        df_schedule["forecast_start_date"] = pd.to_datetime(df_schedule["planned_start"])
        df_schedule["forecast_finish_date"] = pd.to_datetime(df_schedule["planned_finish"])
    else:
        df_schedule["forecast_start_date"] = pd.to_datetime(df_schedule["forecast_start_date"])
        df_schedule["forecast_finish_date"] = pd.to_datetime(df_schedule["forecast_finish_date"])
        
    enriched_df = cost_engine.calculate_costs(df_schedule, df_resource)
    
    # 2. Check Overload
    print("\nRunning Availability Check...")
    stats = cost_engine.check_resource_availability(enriched_df)
    
    # Check Resource 1
    # Note: user CSV has "1", "2" (integers?). My cleaning logic handles str conversion.
    # Let's check both int 1 and str "1"
    
    r1_stats = stats.get("1", stats.get(1, {}))
    print(f"\nResource 1 Stats: {r1_stats}")
    
    # We want to see the DAILY usage to spot the overlap
    # I need to modify check_resource_availability or just copy the logic effectively here
    # to print the dates.
    
    # Custom inspection of daily maps
    print("\nDetailed Daily Breakdown for Resource 1:")
    # We replicate the loop logic to print it
    daily_usage = {}
    for _, row in enriched_df.iterrows():
        rid = str(row.get("resource_id", "")).strip()
        if rid == "1" or rid == "1.0":
            start = row.get("forecast_start_date")
            finish = row.get("forecast_finish_date")
            fte = row.get("fte_allocation", 0)
            act_id = row.get("activity_id")
            
            print(f"Task {act_id}: {start} to {finish} | FTE: {fte}")
            
            try:
                dates = pd.bdate_range(start=start, end=finish)
                for d in dates:
                    d_str = d.strftime('%Y-%m-%d')
                    daily_usage[d_str] = daily_usage.get(d_str, 0) + fte
            except Exception as e:
                print(f"Error parsing date: {e}")

    print("\nDaily Sums > 1.0:")
    for d, load in sorted(daily_usage.items()):
        if load > 1.0:
            print(f"Date: {d} | Load: {load}")

if __name__ == "__main__":
    debug_user_data()
