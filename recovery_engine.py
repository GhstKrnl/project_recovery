import pandas as pd
import numpy as np
import uuid

# Action Types
ACTION_RES_SWAP = "Resource Swap"
ACTION_FTE_ADJ = "FTE Adjustment"
ACTION_COMPRESS = "Duration Compression"
ACTION_FAST_TRACK = "Fast-Tracking"
ACTION_DEFERRAL = "Scope Deferral"

def init_recovery_workspace(df_schedule):
    """
    Creates a deep copy of the schedule dataframe for the recovery workspace.
    Adds audit columns.
    """
    if df_schedule is None or df_schedule.empty:
        return pd.DataFrame()
        
    recovery_df = df_schedule.copy(deep=True)
    
    # Initialize audit columns if not present
    if "last_change_type" not in recovery_df.columns:
        recovery_df["last_change_type"] = None
    if "last_change_id" not in recovery_df.columns:
        recovery_df["last_change_id"] = None
    if "is_deferred" not in recovery_df.columns:
        recovery_df["is_deferred"] = False
        
    return recovery_df

def generate_actions(df_schedule, resource_stats, df_resource, root_causes):
    """
    Generates potential recovery actions based on diagnosis (root causes) and rule constraints.
    Returns a list of action dictionaries.
    """
    actions = []
    
    # Pre-process Root Causes for quick lookup
    # Map Activity ID -> Root Cause Category
    rc_map = {}
    if not root_causes.empty:
        # Assuming 'Activity' column exists in root_causes
        for _, row in root_causes.iterrows():
            rc_map[row.get("Activity")] = row.get("Root Cause Category")

    if not df_schedule.empty:
        # Ensure Numeric Rates in Resource DF once
        if df_resource is not None:
             df_resource["resource_rate"] = pd.to_numeric(df_resource["resource_rate"], errors='coerce').fillna(0.0)

        for idx, act_row in df_schedule.iterrows():
            act_id = act_row["activity_id"]
            proj_name = act_row.get("project_name", "Unknown Project")
            
            # --- Global Filter: Skip Completed Tasks ---
            act_fin = str(act_row.get("actual_finish", ""))
            if act_fin and act_fin.lower() != "nan" and act_fin.lower() != "nat":
                 continue

            # Gather Activity Metrics
            current_res = act_row.get("resource_id")
            current_fte = 0.0
            try:
                current_fte = float(act_row.get("fte_allocation", 0))
            except: pass
            
            rem_dur = 0.0
            try:
                rem_dur = float(act_row.get("remaining_duration_days", 0))
            except: pass
            
            on_crit = act_row.get("on_critical_path", False)
            
            # Look up Resource Details (Robustly)
            curr_rate = 0.0
            curr_res_name = "Unknown"
            max_fte = 1.0 # Default
            target_skills = set()
            curr_res_row = pd.DataFrame()

            if df_resource is not None and current_res and str(current_res).lower() != "nan" and current_res != 0:
                try:
                    res_str = str(current_res).strip()
                    mask = df_resource["resource_id"].astype(str).str.strip() == res_str
                    curr_res_row = df_resource[mask]
                    
                    if not curr_res_row.empty:
                        r_row = curr_res_row.iloc[0]
                        curr_rate = float(r_row["resource_rate"])
                        max_fte = float(r_row.get("resource_max_fte", 1.0))
                        curr_res_name = r_row.get("resource_name", str(current_res))
                        
                        ts_str = str(r_row.get("resource_skills", ""))
                        if ts_str and ts_str.lower() != "nan":
                             sep = ";" if ";" in ts_str else ","
                             target_skills = set([s.strip() for s in ts_str.split(sep) if s.strip()])
                except Exception:
                    pass

            # --- Rule A: Resource Optimization (Swap) ---
            # Trigger: Cheaper resource available with matching skills
            if not curr_res_row.empty:
                # Find Candidates
                candidates = df_resource[(df_resource["resource_id"] != current_res) & (df_resource["resource_rate"] < curr_rate)]
                for _, cand in candidates.iterrows():
                     cand_id = cand["resource_id"]
                     cand_name = cand.get("resource_name", cand_id)
                     cand_rate = float(cand["resource_rate"])
                     
                     # Skill Match
                     match_pct = 0
                     cand_skills = set()
                     cs_str = str(cand.get("resource_skills", ""))
                     if cs_str and cs_str.lower() != "nan":
                         sep = ";" if ";" in cs_str else ","
                         cand_skills = set([s.strip() for s in cs_str.split(sep) if s.strip()])
                    
                     if not target_skills:
                         match_pct = 100 
                     else:
                         overlap = target_skills.intersection(cand_skills)
                         match_pct = int((len(overlap) / len(target_skills)) * 100)
                         
                     if match_pct < 60:
                         continue
                         
                     # Availability (Basic Overlap Check)
                     is_busy = False
                     try:
                         act_start = pd.to_datetime(act_row["planned_start"])
                         act_end = pd.to_datetime(act_row["planned_finish"])
                         cand_tasks = df_schedule[df_schedule["resource_id"] == cand_id]
                         for _, task in cand_tasks.iterrows():
                             t_start = pd.to_datetime(task["planned_start"])
                             t_end = pd.to_datetime(task["planned_finish"])
                             if t_start < act_end and t_end > act_start:
                                 is_busy = True
                                 break
                     except: pass
                     
                     if is_busy:
                         continue

                     # Calculate Savings
                     total_dur = float(act_row.get("planned_duration", 0))
                     hours = total_dur * 8 * current_fte
                     savings = (curr_rate - cand_rate) * hours
                     
                     if savings > 0:
                         desc = f"Project: {proj_name} | Task: {act_row['activity_name']}\n"
                         desc += f"Swap **{curr_res_name}** with **{cand_name}**.\n"
                         desc += f"**Save ${savings:,.2f}** | Skill Match: {match_pct}%"
                         
                         story = (f"**Trigger:** Found cheaper resource **{cand_name}** (${cand_rate}/hr) vs **{curr_res_name}** (${curr_rate}/hr). "
                                  f"Skills match: {match_pct}%.")
                         
                         actions.append({
                            "id": str(uuid.uuid4()),
                            "type": ACTION_RES_SWAP,
                            "activity_id": act_id,
                            "description": desc,
                            "narrative": story,
                            "parameters": {
                                "old_res": current_res, "old_name": curr_res_name, "old_rate": curr_rate,
                                "new_res": cand_id, "new_name": cand_name, "new_rate": cand_rate,
                                "savings": savings, "match_pct": match_pct
                            },
                            "project_name": proj_name,
                            "resource_name": curr_res_name
                         })

            # --- Rule B: FTE Adjustment ---
            # Trigger: Critical Path AND Remaining > 0 AND Current < Max
            # Now runs for ALL Critical Path tasks, not just "Root Causes"
            if on_crit and rem_dur > 0 and current_fte < max_fte:
                 new_dur = rem_dur * (current_fte / max_fte)
                 saved_days = rem_dur - new_dur
                 
                 # Simple Cost Impact (assuming total effort hours constant, but shorter duration maybe different rate? No, same resource)
                 # Actually, if we increase FTE, we reduce Duration. Total Hours = Dur * FTE.
                 # Old Hours = RemDur * OldFTE.
                 # New Hours = NewDur * NewFTE.
                 # Since NewDur = RemDur * (OldFTE / NewFTE), NewDur * NewFTE = RemDur * OldFTE.
                 # So Cost is theoretically same (just burned faster).
                 # BUT, normally increasing FTE might imply overtime or premium? Assuming flat rate for MVP.
                 # Let's show Cost Impact roughly 0 or just "Rate * Hours".
                 
                 cost_diff = 0.0 # Neutral cost, just time recovery
                 
                 desc = f"Project: {proj_name} | Task: {act_row['activity_name']}\n"
                 desc += f"Increase **{curr_res_name}** FTE: {current_fte} -> {max_fte}.\n"
                 desc += f"**Save {saved_days:.1f} Days** (Duration: {rem_dur:.1f}->{new_dur:.1f})."
                 
                 story = (f"**Trigger:** Task is on **Critical Path** and **{curr_res_name}** is utilized at only **{current_fte} FTE**. "
                          f"Increasing to max capacity (**{max_fte} FTE**) speeds up completion.")
                          
                 actions.append({
                     "id": str(uuid.uuid4()),
                     "type": ACTION_FTE_ADJ,
                     "activity_id": act_id,
                     "description": desc,
                     "narrative": story,
                     "parameters": {
                         "old_fte": current_fte, "new_fte": max_fte,
                         "old_dur": rem_dur, "new_dur": new_dur,
                         "saved_days": saved_days, "cost_impact": cost_diff
                     },
                     "project_name": proj_name,
                     "resource_name": curr_res_name
                 })

            # --- Rule C: Compression ---
            # Trigger: Critical Path AND Duration >= 2 (Optimization)
            # Or if specifically flagged as Slippage (via rc_map)
            # We'll be aggressive: Suggest for ANY Critical Path task > 2 days
            if on_crit and rem_dur >= 2:
                max_compress = max(1, int(rem_dur * 0.2))
                actions.append({
                    "id": str(uuid.uuid4()),
                    "type": ACTION_COMPRESS,
                    "activity_id": act_id,
                    "description": f"Compress Duration by {max_compress} days (Max 20%)",
                    "parameters": {"reduce_by_days": int(max_compress)}
                })

            # --- Rule D: Fast-Tracking ---
            # Trigger: Critical Path AND FS Predecessors
            if on_crit:
                 preds_str = str(act_row.get("predecessors", ""))
                 if preds_str == "nan" or not preds_str:
                     preds_str = str(act_row.get("predecessor_id", ""))
                 
                 if preds_str and preds_str.lower() != "nan":
                     actions.append({
                         "id": str(uuid.uuid4()),
                         "type": ACTION_FAST_TRACK,
                         "activity_id": act_id,
                         "description": "Convert Predecessors to SS + Lag (Fast-Track)",
                         "parameters": {"target_type": "SS", "lag": 2}
                     })

            # --- Rule E: Scope Deferral ---
            # Trigger: Known Cost Overrun or Risk (from Root Cause Map)
            rc_cat = rc_map.get(act_id)
            if rc_cat in ["Cost Overrun", "Risk / Uncertainty (Proxy)"]:
                actions.append({
                    "id": str(uuid.uuid4()),
                    "type": ACTION_DEFERRAL,
                    "activity_id": act_id,
                    "description": "Defer Scope (Remove from active calculation)",
                    "parameters": {"set_deferred": True}
                })
            
    return actions

def apply_action(df, action):
    """
    Mutates df in place applying the action.
    Returns success (bool), message (str)
    """
    act_id = action.get("activity_id")
    # Find row index
    mask = df["activity_id"] == act_id
    if not mask.any():
        return False, "Activity not found"
        
    idx = df[mask].index[0]
    
    # Log
    df.at[idx, "last_change_type"] = action["type"]
    df.at[idx, "last_change_id"] = action.get("id")
    
    if action["type"] == ACTION_RES_SWAP:
        new_res = action["parameters"]["new_res"]
        df.at[idx, "resource_id"] = new_res
        return True, f"Swapped resource to {new_res}."
        
    elif action["type"] == ACTION_FTE_ADJ:
        new_fte = action["parameters"]["new_fte"]
        df.at[idx, "fte_allocation"] = new_fte
        
        # Adjust remaining duration based on FTE increase
        # Dur_New = Dur_Old * (FTE_Old / FTE_New)
        old_fte = float(action["parameters"]["old_fte"])
        current_dur = float(df.at[idx, "remaining_duration_days"])
        if new_fte > 0:
            new_dur = current_dur * (old_fte / new_fte)
            df.at[idx, "remaining_duration_days"] = new_dur
            
        return True, f"Increased FTE to {new_fte}."
        
    elif action["type"] == ACTION_COMPRESS:
        days = action["parameters"]["reduce_by_days"]
        current = df.at[idx, "remaining_duration_days"]
        df.at[idx, "remaining_duration_days"] = max(0, current - days)
        return True, f"Compressed duration by {days} days."
        
    elif action["type"] == ACTION_FAST_TRACK:
        return True, "Applied Fast-Tracking (Simulated)."
        
    elif action["type"] == ACTION_DEFERRAL:
        df.at[idx, "is_deferred"] = True
        df.at[idx, "remaining_duration_days"] = 0 # effectively removed from sched calcs
        return True, "Deferred scope (Duration set to 0)."
        
    return False, "Unknown Action Type"
