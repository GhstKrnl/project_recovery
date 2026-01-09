import pandas as pd
import numpy as np

def calculate_costs(df_schedule, df_resource):
    """
    Calculates cost and load metrics.
    Merges schedule and resource data on 'resource_id'.
    Returns enriched dataframe.
    """
    # Merge
    # Ensure resource_id is consistent type (str or object)
    if "resource_id" not in df_schedule.columns or "resource_id" not in df_resource.columns:
        return df_schedule, {}

    # Cast to str for join safely. Handle floats (1.0 -> 1) and whitespace.
    def clean_key(val):
        if pd.isna(val) or val == "":
            return "UNKNOWN"
        s = str(val).strip()
        # If looks like float "1.0", convert to int then str
        try:
            val_f = float(s)
            if val_f.is_integer():
                return str(int(val_f))
        except:
            pass
        return s

    df_schedule["_rid"] = df_schedule["resource_id"].map(clean_key)
    df_resource["_rid"] = df_resource["resource_id"].map(clean_key)
    
    merged = pd.merge(df_schedule, df_resource, on="_rid", how="left", suffixes=("", "_res"))
    
    # Calculate Metrics
    # planned_load_hours = planned_duration * resource_working_hours * fte
    # handle NaNs -> 0
    
    def get_col(row, col):
        val = row.get(col)
        if pd.isna(val) or val == "":
            return 0
        return float(val)

    results = []
    
    for _, row in merged.iterrows():
        # Get inputs
        plan_dur = get_col(row, "planned_duration")
        act_dur = get_col(row, "actual_duration")
        rem_dur = get_col(row, "remaining_duration_days")
        
        fte = get_col(row, "fte_allocation")
        # Resource cols might have different names based on merge or source
        # In utils.py: "resource_rate", "resource_max_fte", "resource_start_date"
        # "resource_working_hours" is validated in app.py but not in REQUIRED_COLUMNS_RESOURCE.
        # Assuming it is present in csv.
        
        rate = get_col(row, "resource_rate")
        work_hours = get_col(row, "resource_working_hours") # Need to ensure this exists in CSV/Utils logic
        
        # Logic
        planned_load = plan_dur * work_hours * fte
        planned_cost = planned_load * rate
        
        actual_load = act_dur * work_hours * fte
        actual_cost = actual_load * rate
        
        remaining_load = rem_dur * work_hours * fte
        remaining_cost = remaining_load * rate
        
        eac_cost = actual_cost + remaining_cost
        
        res_dict = {
            "activity_id": row["activity_id"],
            "planned_load_hours": planned_load,
            "planned_cost": planned_cost,
            "actual_load_hours": actual_load,
            "actual_cost": actual_cost,
            "remaining_load_hours": remaining_load,
            "remaining_cost": remaining_cost,
            "eac_cost": eac_cost,
            "resource_id": row["resource_id"], # Keep for aggregation
            # Pass through for overload check
            "forecast_start_date": row.get("forecast_start_date"),
            "forecast_finish_date": row.get("forecast_finish_date"),
            "fte_allocation": fte,
            "resource_max_fte": get_col(row, "resource_max_fte"),
            "resource_start_date": row.get("resource_start_date"), 
            "resource_end_date": row.get("resource_end_date")
        }
        results.append(res_dict)
        
    return pd.DataFrame(results)

def check_resource_availability(cost_df):
    """
    Checks for resource overloads.
    Expands tasks to daily buckets and sums FTE.
    returns dict: {
        resource_id: {
            "overload_days_count": int,
            "peak_fte": float,
            "daily_assigned_fte": dict (date -> fte)
        }
    }
    """
    resource_stats = {}
    
    # Filter only rows with valid dates and forecast dates
    # We iterate cost_df
    
    # Store daily usage: { res_id: { date_str: total_fte } }
    daily_usage = {}
    
    # Store max capacity: { res_id: max_fte }
    res_caps = {}
    
    for _, row in cost_df.iterrows():
        rid = row.get("resource_id")
        if pd.isna(rid) or rid == 0 or rid == "0":
            continue
            
        max_fte = row.get("resource_max_fte", 8.0) # Default? Or 1.0? Usually 1.0 or 8h. 
        # FTE implies Full Time Equivalent. 1.0 = 1 Person.
        # User said "resource_max_fte" in CSV.
        
        if rid not in res_caps:
            res_caps[rid] = max_fte
            
        # Expand dates
        start = row.get("forecast_start_date")
        finish = row.get("forecast_finish_date")
        fte = row.get("fte_allocation", 0)
        
        if pd.isna(start) or pd.isna(finish):
            continue
            
        try:
            # Generate Business Days range
            dates = pd.bdate_range(start=start, end=finish)
            
            if rid not in daily_usage:
                daily_usage[rid] = {}
                
            for d in dates:
                d_str = d.strftime('%Y-%m-%d')
                if d_str not in daily_usage[rid]:
                    daily_usage[rid][d_str] = 0
                daily_usage[rid][d_str] += fte
                
        except:
            continue

    # Analyze Overloads
    for rid, usage_map in daily_usage.items():
        max_cap = res_caps.get(rid, 1.0)
        if max_cap == 0: max_cap = 1.0 # Safety
        
        overloads = 0
        peak = 0
        
        for day, total_fte in usage_map.items():
            if total_fte > max_cap:
                overloads += 1
            if total_fte > peak:
                peak = total_fte
                
        resource_stats[rid] = {
            "overload_days_count": overloads,
            "peak_fte": peak,
            # "daily_assigned_fte": usage_map # Can be heavy, maybe omit?
        }
        
    return resource_stats
