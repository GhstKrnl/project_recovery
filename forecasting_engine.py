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
    Uses Topological Sort to propagate Delay Carried In correctly.
    """
    results = {}
    
    # Pre-process dataframe to dict for speed
    cols = ["activity_id", "actual_start", "actual_finish", "baseline_1_start", "baseline_1_finish", 
            "ES_date", "EF_date", "planned_duration"]
            
    # Lookup map: activity_id -> row
    df_lookup = {}
    for idx, row in df.iterrows():
        try:
            aid = int(row["activity_id"])
            df_lookup[aid] = row
        except:
            pass

    # Ensure Topological Order for propagation
    if nx.is_directed_acyclic_graph(G):
        ordered_nodes = list(nx.topological_sort(G))
    else:
        # Fallback if cycles exist (should be handled by app.py validation, but safety check)
        ordered_nodes = list(G.nodes)
            
    for node in ordered_nodes:
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
            
        # Helper
        def get_val(r, c): return r[c] if c in r and not pd.isna(r[c]) else None
            
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
        forecast_start_for_delay = None
        forecast_fin_for_delay = None

        if act_fin:
            res["percent_complete"] = 100
            res["actual_duration"] = count_working_days(act_start, act_fin, inclusive=True)
            res["remaining_duration_days"] = 0
            
            # Forecast = Actual
            res["forecast_start_date"] = act_start
            res["forecast_finish_date"] = act_fin
            
            forecast_start_for_delay = act_start
            forecast_fin_for_delay = act_fin
            
        else:
            # 0% or In Progress
            res["percent_complete"] = 0 # MVP Requirement
            res["actual_duration"] = 0
            
            if act_start:
                 # In Progress
                 # Calculate Forecast Finish based on Actual Start + Planned Duration
                 # (Simplification: assuming original duration holds if not updated)
                 res["remaining_duration_days"] = plan_dur
                 res["forecast_start_date"] = act_start
                 
                 # Calc finish date: Start + Duration (Business Days)
                 # We need date math here. 
                 # However, we don't have the heavy numpy date logic imported easily as helper here 
                 # without duplicating cpm_engine logic or importing it.
                 # BUT, we can use calculate_delay_metric_days kind of logic or simple approximation?
                 # Better: Use numpy busday_offset since we imported numpy.
                 
                 try:
                     s = np.datetime64(pd.to_datetime(act_start), 'D')
                     # finish = start + duration - 1 (inclusive)
                     dur_days = int(plan_dur)
                     if dur_days > 0:
                        offset = dur_days - 1
                        f_np = np.busday_offset(s, offset, roll='forward', weekmask='1111100')
                        res["forecast_finish_date"] = str(f_np)
                     else:
                        res["forecast_finish_date"] = act_start
                 except:
                     # Fallback
                     res["forecast_finish_date"] = cpm_ef
                 
                 forecast_start_for_delay = act_start
                 forecast_fin_for_delay = res["forecast_finish_date"]
            else:
                 # Not Started
                 res["remaining_duration_days"] = plan_dur
                 res["forecast_start_date"] = cpm_es
                 res["forecast_finish_date"] = cpm_ef
                 
                 forecast_start_for_delay = cpm_es
                 forecast_fin_for_delay = cpm_ef

        # 2. Delay Carried In (CRITICAL FIX: Use Predecessor Forecasts, not just Actuals)
        # We propagate max delay from predecessors
        # Delay = Pred Forecast Finish - Pred Baseline Finish
        
        preds = list(G.predecessors(node))
        carried_delays = [0]
        
        for pred in preds:
            # Look up predecessor's result we just calculated (since topo sorted)
            pred_res = results.get(pred) 
            p_row = df_lookup.get(pred)
            
            if pred_res and p_row is not None:
                # Use Calculated Forecast Finish from predecessor logic
                p_forecast_fin = pred_res.get("forecast_finish_date")
                p_base_fin = get_val(p_row, "baseline_1_finish")
                
                if p_forecast_fin and p_base_fin:
                    d = calculate_delay_metric_days(p_forecast_fin, p_base_fin)
                    carried_delays.append(d)
        
        res["delay_carried_in"] = max(0, max(carried_delays))
        
        # 3. Total Schedule Delay
        start_var = 0
        fin_var = 0
        if bl_start and forecast_start_for_delay:
             start_var = calculate_delay_metric_days(forecast_start_for_delay, bl_start)
        if bl_fin and forecast_fin_for_delay:
             fin_var = calculate_delay_metric_days(forecast_fin_for_delay, bl_fin)  
        res["total_schedule_delay"] = max(start_var, fin_var)
        
        # 4. Task-Created Delay
        res["task_created_delay"] = max(0, res["total_schedule_delay"] - res["delay_carried_in"])
        
        # 5. Delay Absorbed
        res["delay_absorbed"] = res["delay_carried_in"] - res["task_created_delay"]
        
        results[node] = res
        
    return results
