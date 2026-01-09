import pandas as pd
import numpy as np
import networkx as nx

def count_working_days(start, end, inclusive=False):
    """
    Counts working days between start and end (ISO strings or datetimes).
    Mon-Fri working.
    """
    try:
        s = np.datetime64(pd.to_datetime(start), 'D')
        e = np.datetime64(pd.to_datetime(end), 'D')
        
        if np.isnat(s) or np.isnat(e):
            return 0
        
        # busday_count is exclusive of end.
        count = np.busday_count(s, e, weekmask='1111100')
        
        if inclusive:
            # Check if end date itself is a working day?
            # Or just add 1? 
            # If Mon-Mon (Same day). Excl=0. Incl=1.
            # If Mon-Sun. Excl=5. Incl=5 (Sat/Sun ignore).
            # Logic: If inclusive, we treat 'end' as part of the range.
            # busday_count(s, e+1) effectively.
            # But e+1 validity matters.
            # Only add 1 if 'end' is a working day?
            # Standard Duration = busday_count(start, end) + 1 (if end is working)
            # Simplification: busday_count(s, e + 1 day)
            
            e_plus_1 = np.busday_offset(e, 1, roll='forward', weekmask='1111100')
            # Wait, if e is Friday. e+1 is Monday? 
            # busday_count(Fri, Mon) = 1.
            # Correct.
            # But simpler: count = busday_count(s, e). If e is working, +1?
            is_working = np.is_busday(e, weekmask='1111100')
            if is_working:
                count += 1
        
        return int(count)
    except Exception:
        return 0

def calculate_delay_metric_days(target_date, baseline_date):
    """
    Calculates delay in working days: target - baseline.
    Positive = Late. Negative = Early.
    """
    try:
        t = np.datetime64(pd.to_datetime(target_date), 'D')
        b = np.datetime64(pd.to_datetime(baseline_date), 'D')
        
        if np.isnat(t) or np.isnat(b):
            return 0 # Or None? 0 implies no delay.
            
        # We need difference in working days.
        # If t > b: positive (busday_count(b, t))
        # If t < b: negative (-busday_count(t, b))
        
        if t >= b:
             return int(np.busday_count(b, t, weekmask='1111100'))
        else:
             return -int(np.busday_count(t, b, weekmask='1111100'))
             
    except:
        return 0

def calculate_forecasts(df, G):
    """
    Calculates forecasting metrics and returns a DataFrame (or dict) to merge.
    Assumes df has 'ES_date', 'EF_date' (CPM results) and 'baseline_1_start/finish'.
    """
    results = {}
    
    # Pre-process dataframe to dict for speed
    # We need access to predecessors' actuals for "Delay Carried In"
    # Create lookup map
    
    # We need to act row by row, but topological order isn't strictly necessary 
    # if we assume standard 1-depth lookups or if graph edges hold the lags.
    # But "Delay Carried In" looks at Predecessors. 
    # We can iterate through the Graph nodes.
    
    # Convert dates to strings/objects once
    # Ensure columns exist
    cols = ["activity_id", "actual_start", "actual_finish", "baseline_1_start", "baseline_1_finish", 
            "ES_date", "EF_date", "planned_duration"]
    
    # Helper to safe get
    def get_val(row, col):
        if col in row and not pd.isna(row[col]):
            return row[col]
        return None

    # We need a quick lookup for node data
    # Iterating over G.nodes
    
    # Create a look up dict from df
    # Key: activity_id (clean)
    
    df_lookup = {}
    for idx, row in df.iterrows():
        try:
            aid = int(row["activity_id"])
            df_lookup[aid] = row
        except:
            pass
            
    for node in G.nodes:
        # Defaults
        res = {
            "percent_complete": 0,
            "actual_duration": 0,
            "baseline_1_duration": 0,
            "remaining_duration_days": 0,
            "forecast_start_date": None,
            "forecast_finish_date": None,
            "delay_carried_in": 0,
            "total_schedule_delay": 0,
            "task_created_delay": 0,
            "delay_absorbed": 0
        }
        
        row = df_lookup.get(node)
        if row is None:
            results[node] = res
            continue
            
        # 1. Percent Complete & Actual/Remaining Duration
        act_start = get_val(row, "actual_start")
        act_fin = get_val(row, "actual_finish")
        plan_dur = get_val(row, "planned_duration") 
        if plan_dur is None: plan_dur = 0
        
        # CPM Forecast Dates (ES/EF)
        cpm_es = get_val(row, "ES_date")
        cpm_ef = get_val(row, "EF_date")
        
        # Baselines
        bl_start = get_val(row, "baseline_1_start")
        bl_fin = get_val(row, "baseline_1_finish")
        
        # Calculate Baseline Duration (Requested feature)
        if bl_start and bl_fin:
            res["baseline_1_duration"] = count_working_days(bl_start, bl_fin, inclusive=True)
        else:
            res["baseline_1_duration"] = 0

        # Logic
        if act_fin:
            res["percent_complete"] = 100
            # Actual Duration (Inclusive)
            res["actual_duration"] = count_working_days(act_start, act_fin, inclusive=True)
            res["remaining_duration_days"] = 0
            
            # Forecast = Actual
            res["forecast_start_date"] = act_start
            res["forecast_finish_date"] = act_fin
            
            forecast_fin_for_delay = act_fin
            forecast_start_for_delay = act_start
            
        else:
            # 0% or In Progress
            res["percent_complete"] = 0 # As per requirement (empty actual_finish -> 0)
            res["actual_duration"] = 0 # Or partial? Req says 0 logic implied.
            
            # If act_start is present?
            if act_start:
                 # In Progress
                 # Logic for Remaining? Req says "actual_finish not present consider pct=0".
                 # If pct=0, remaining = planned?
                 res["remaining_duration_days"] = plan_dur
                 # Forecast Start = Actual Start
                 res["forecast_start_date"] = act_start
                 # Forecast Finish?
                 # If not finished, we probably shouldn't use CPM EF directly if we started late.
                 # But sticking to MVP scope: "Forecast...".
                 # If we use CPM EF, it ignores Actual Start.
                 # But we don't have a re-calc mechanism here.
                 # Let's use CPM EF as fallback for Finish.
                 res["forecast_finish_date"] = cpm_ef
                 
                 forecast_start_for_delay = act_start
                 forecast_fin_for_delay = cpm_ef
                 
            else:
                 # Not Started
                 res["remaining_duration_days"] = plan_dur
                 res["forecast_start_date"] = cpm_es
                 res["forecast_finish_date"] = cpm_ef
                 
                 forecast_start_for_delay = cpm_es
                 forecast_fin_for_delay = cpm_ef

        # 2. Delay Carried In
        # Max(Predecessor Actual Finish - Predecessor Baseline Finish)
        preds = list(G.predecessors(node))
        carried_delays = [0]
        
        for pred in preds:
            # Check pred stats
            p_row = df_lookup.get(pred)
            if p_row is not None:
                p_act_fin = get_val(p_row, "actual_finish")
                p_base_fin = get_val(p_row, "baseline_1_finish")
                
                # Formula: Pred Actual Fin - Pred Baseline Fin (User omitted lag in text simplification? "Pred.actual_finish + lag - Pred.baseline_finish - lag")
                if p_act_fin and p_base_fin:
                    d = calculate_delay_metric_days(p_act_fin, p_base_fin)
                    carried_delays.append(d)
        
        res["delay_carried_in"] = max(0, max(carried_delays)) # MAX(0, ...)
        
        # 3. Total Schedule Delay
        # MAX( Act_Start - BL_Start, Act_Fin - BL_Fin ) 
        # Using Forecast dates if Actual is missing (for checking variances)
        
        start_var = 0
        fin_var = 0
        
        if bl_start and forecast_start_for_delay:
             start_var = calculate_delay_metric_days(forecast_start_for_delay, bl_start)
             
        if bl_fin and forecast_fin_for_delay:
             fin_var = calculate_delay_metric_days(forecast_fin_for_delay, bl_fin)
             
        res["total_schedule_delay"] = max(start_var, fin_var)
        
        # 4. Task-Created Delay
        # MAX(0, Total - Carried)
        res["task_created_delay"] = max(0, res["total_schedule_delay"] - res["delay_carried_in"])
        
        # 5. Delay Absorbed
        # Carried - Task Created? 
        # Formula: Delay Absorbed = Delay Carried In − Task-Created Delay
        res["delay_absorbed"] = res["delay_carried_in"] - res["task_created_delay"]
        
        results[node] = res
        
    return results
