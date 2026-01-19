import numpy as np
import pandas as pd
import networkx as nx

def calculate_durations(df):
    """
    Calculates duration in business days (Mon-Fri).
    Prefers planned_duration or remaining_duration_days from dataframe if available.
    Falls back to calculating from planned_start and planned_finish dates.
    Returns a dict mapping activity_id -> duration (int).
    """
    durations = {}
    for _, row in df.iterrows():
        try:
            act_id = int(row["activity_id"])
            
            # Priority 1: Use planned_duration if available and valid
            if "planned_duration" in row and pd.notna(row["planned_duration"]):
                try:
                    dur_val = float(row["planned_duration"])
                    if dur_val > 0:
                        durations[act_id] = int(dur_val)
                        continue
                except (ValueError, TypeError):
                    pass
            
            # Priority 2: Use remaining_duration_days if available and valid
            if "remaining_duration_days" in row and pd.notna(row["remaining_duration_days"]):
                try:
                    dur_val = float(row["remaining_duration_days"])
                    if dur_val > 0:
                        durations[act_id] = int(dur_val)
                        continue
                except (ValueError, TypeError):
                    pass
            
            # Priority 3: Fall back to calculating from dates
            start = pd.to_datetime(row["planned_start"])
            finish = pd.to_datetime(row["planned_finish"])
            
            if pd.isna(start) or pd.isna(finish):
                durations[act_id] = 0
                continue
                
            # Convert to numpy datetime64[D]
            start_np = np.datetime64(start, 'D')
            finish_np = np.datetime64(finish, 'D')
            
            # Check for invalid dates
            if np.isnat(start_np) or np.isnat(finish_np):
                durations[act_id] = 0
                continue

            # We assume finish is inclusive.
            # Add 1 day to finish for exclusive upper bound
            finish_exclusive = finish_np + np.timedelta64(1, 'D')
            
            duration = np.busday_count(start_np, finish_exclusive)
            durations[act_id] = int(duration)
            
        except (ValueError, TypeError):
            # Invalid ID or dates
            continue
            
    return durations

def run_cpm(df, G):
    """
    Runs Forward and Backward pass.
    Returns a dict of dicts: {act_id: {'ES':..., 'EF':..., 'LS':..., 'LF':..., 'Float':..., 'Critical':...}}
    """
    durations = calculate_durations(df)
    
    # Check if DAG
    if not nx.is_directed_acyclic_graph(G):
        raise ValueError("Graph contains cycles. CPM cannot be calculated.")

    # Topo sort
    topo_order = list(nx.topological_sort(G))
    
    # --- Forward Pass ---
    # ES, EF are relative integer days from Project Start (Day 0)
    # We treat min_start as Day 0?
    # Actually, standard CPM just calculates relative offsets.
    
    es = {n: 0 for n in G.nodes}
    ef = {n: 0 for n in G.nodes}
    
    for node in topo_order:
        duration = durations.get(node, 0)
        
        # Default start is 0
        node_es = 0
        
        # Check predecessors
        preds = list(G.predecessors(node))
        if preds:
            constraints = []
            for pred in preds:
                edge = G[pred][node]
                dep_type = edge.get("type", "FS")
                lag = edge.get("lag", 0)
                
                pred_ef = ef[pred]
                pred_es = es[pred]
                pred_dur = durations.get(pred, 0)
                
                # Logic:
                # FS: ES_succ >= EF_pred + lag 
                # (Standard CPM: EF is usually finish day index. If A finishes day 5, B starts day 6?
                #  Or if A finishes day 5 (at end of day 5), B starts day 6?
                #  If we use 0-based index: Day 0 is first day.
                #  Duration 1. EF = Start + Dur - 1?
                #  Let's stick to: End = Start + Duration (Exclusive End).
                #  So if Start=0, Dur=5, End=5.
                #  FS: Start >= End + Lag? No, usually Start >= Finish.
                #  If Finish is exclusive (Day 5), then Start >= 5. Correct.
                
                # FS: ES >= pred_EF + lag
                if dep_type == "FS":
                    constraints.append(pred_ef + lag)
                # SS: ES >= pred_ES + lag
                elif dep_type == "SS":
                    constraints.append(pred_es + lag)
                # FF: EF >= pred_EF + lag => ES + dur >= pred_EF + lag => ES >= pred_EF + lag - dur
                elif dep_type == "FF":
                    constraints.append(pred_ef + lag - duration)
                # SF: EF >= pred_ES + lag => ES + dur >= pred_ES + lag => ES >= pred_ES + lag - dur
                elif dep_type == "SF":
                    constraints.append(pred_es + lag - duration)
            
            if constraints:
                node_es = max(constraints)
        
        # Set ES and EF
        # Ensure ES is not negative? CPM allows negative relative to start if lags force it? 
        # Usually project starts at 0. But if a task has negative lag from Start, it might go negative.
        # We will keep it raw.
        es[node] = node_es
        ef[node] = node_es + duration

    # Project Duration
    if not ef:
        return {}
        
    project_finish = max(ef.values())
    
    # --- Backward Pass ---
    ls = {n: project_finish for n in G.nodes}
    lf = {n: project_finish for n in G.nodes}
    
    for node in reversed(topo_order):
        duration = durations.get(node, 0)
        
        # Default LF is project finish, unless constrained by successors
        cols = []
        succs = list(G.successors(node))
        
        if not succs:
            node_lf = project_finish
        else:
            constraints = []
            for succ in succs:
                edge = G[node][succ]
                dep_type = edge.get("type", "FS")
                lag = edge.get("lag", 0)
                
                succ_ls = ls[succ]
                succ_lf = lf[succ]
                succ_dur = durations.get(succ, 0)
                
                # Logic (Reversed):
                # FS: successor ES >= my EF + lag => my EF <= successor ES - lag
                #     my LF <= succ_LS - lag
                if dep_type == "FS":
                    constraints.append(succ_ls - lag)
                # SS: succ ES >= my ES + lag => my ES <= succ ES - lag
                #     my LS <= succ_LS - lag
                elif dep_type == "SS":
                    constraints.append(succ_ls - lag) # Wait, this constrains my LS. LF = LS + Dur
                    # If calculating LF explicitly:
                    # LS <= X => LF - Dur <= X => LF <= X + Dur
                # FF: succ EF >= my EF + lag => my EF <= succ EF - lag
                #     my LF <= succ_LF - lag
                elif dep_type == "FF":
                    constraints.append(succ_lf - lag)
                # SF: succ EF >= my ES + lag => my ES <= succ EF - lag
                #     my LS <= succ_LF - lag
                elif dep_type == "SF":
                    # constrains LS
                    # LF <= succ_LF - lag + dur
                    pass # Handled below
            
            # We need to compute LF.
            # Some constraints affect LS, some LF.
            # Relation: LS = LF - Duration.
            # So everything can be converted to LF constraint.
            
            # Re-eval loop for LF
            lf_constraints = []
            # Initialize with Project Finish if no successors? No, we are iterating.
            # If we are strictly internal, we look at ALL successors.
            # If node is sink, LF = project_finish.
             
            for succ in succs:
                edge = G[node][succ]
                dep_type = edge.get("type", "FS")
                lag = edge.get("lag", 0)
                
                succ_ls = ls[succ]
                succ_lf = lf[succ]
                
                if dep_type == "FS":
                    # my LF <= succ_LS - lag
                    lf_constraints.append(succ_ls - lag)
                elif dep_type == "SS":
                    # my LS <= succ_LS - lag => LF - dur <= succ_LS - lag => LF <= succ_LS - lag + dur
                    lf_constraints.append(succ_ls - lag + duration)
                elif dep_type == "FF":
                    # my LF <= succ_LF - lag
                    lf_constraints.append(succ_lf - lag)
                elif dep_type == "SF":
                    # my LS <= succ_LF - lag => LF <= succ_LF - lag + dur
                    lf_constraints.append(succ_lf - lag + duration)
            
            if lf_constraints:
                node_lf = min(lf_constraints)
            else:
                node_lf = project_finish # Should not happen if succs exist, but logic holds

        lf[node] = node_lf
        ls[node] = node_lf - duration

    # --- Results ---
    results = {}
    for node in G.nodes:
        node_es = es[node]
        node_ef = ef[node]
        node_ls = ls[node]
        node_lf = lf[node]
        
        # Float
        total_float = node_ls - node_es
        # Or node_lf - node_ef. Should be identical.
        
        is_critical = abs(total_float) < 0.0001 # Float around 0
        if total_float < 0:
             is_critical = True # Negative float is also critical (super-critical)
        
        results[node] = {
            "ES": node_es,
            "EF": node_ef,
            "LS": node_ls,
            "LF": node_lf,
            "total_float_days": total_float,
            "on_critical_path": is_critical
        }
        
    return results

def convert_offsets_to_dates(cpm_results, project_start_date, durations):
    """
    Converts CPM integer offsets to calendar dates (ISO string).
    
    Args:
        cpm_results (dict): Output from run_cpm.
        project_start_date (str/datetime): The anchor date (Day 0).
        durations (dict): Activity durations (needed for zero-duration logic).
        
    Returns:
        dict: enriched results with ES_date, EF_date, LS_date, LF_date.
    """
    # 1. Normalize Project Start to next business day if it falls on weekend
    # roll='forward' means if Sat/Sun, move to Mon.
    p_start = np.datetime64(pd.to_datetime(project_start_date), 'D')
    p_start = np.busday_offset(p_start, 0, roll='forward', weekmask='1111100')
    
    enriched_results = cpm_results.copy()
    
    for act_id, data in enriched_results.items():
        # Get offsets
        es_off = int(data["ES"])
        ef_off = int(data["EF"])
        ls_off = int(data["LS"])
        lf_off = int(data["LF"])
        dur = durations.get(act_id, 0)
        
        # Calculate Dates
        # ES Date = Start + ES offset
        es_date_np = np.busday_offset(p_start, es_off, roll='forward', weekmask='1111100')
        
        # EF Date
        # If duration > 0: Finish = Start + EF - 1 (Inclusive)
        # If duration == 0: Finish = Start + EF (same as ES basically)
        # Logic: EF in integer domain is usually exclusive end index.
        # But we want Inlcusive Calendar Date.
        # If Start=0 (Mon). Dur=1. EF=1.
        # Finish should be Mon => Start + 0 offset.
        # So offset is EF - 1.
        
        if dur > 0:
            ef_date_np = np.busday_offset(p_start, ef_off - 1, roll='forward', weekmask='1111100')
        else:
            # Milestone: Finish = Start (ES)
            # Or use EF offset which should act like ES
            ef_date_np = np.busday_offset(p_start, ef_off, roll='forward', weekmask='1111100')

        # LS Date
        ls_date_np = np.busday_offset(p_start, ls_off, roll='forward', weekmask='1111100')
        
        # LF Date
        if dur > 0:
             lf_date_np = np.busday_offset(p_start, lf_off - 1, roll='forward', weekmask='1111100')
        else:
             lf_date_np = np.busday_offset(p_start, lf_off, roll='forward', weekmask='1111100')
             
        # Convert to strings
        data["ES_date"] = str(es_date_np)
        data["EF_date"] = str(ef_date_np)
        data["LS_date"] = str(ls_date_np)
        data["LF_date"] = str(lf_date_np)
        
        # Also return planned_duration (calculated)
        data["planned_duration"] = dur
        
    return enriched_results
