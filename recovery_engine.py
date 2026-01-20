import pandas as pd
import numpy as np
import uuid

# Action Types
ACTION_RES_SWAP = "Resource Swap"
ACTION_FTE_ADJ = "FTE Adjustment"
ACTION_COMPRESS = "Duration Compression"
ACTION_FAST_TRACK = "Fast-Tracking"
ACTION_DEFERRAL = "Scope Deferral"
ACTION_CRASHING = "Task Crashing (Overload)"

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
            if act_fin and act_fin.lower() != "nan" and act_fin.lower() != "nat" and act_fin.lower() != "none":
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

                     # Calculate Savings using effort-based approach
                     # Get task_planned_effort if available, otherwise calculate it
                     task_effort = None
                     total_dur = float(act_row.get("planned_duration", 0))
                     
                     if "task_planned_effort" in act_row.index and pd.notna(act_row.get("task_planned_effort")):
                         task_effort = float(act_row.get("task_planned_effort"))
                     else:
                         # Calculate effort: planned_duration × old_resource_working_hours × fte
                         old_work_hours = float(curr_res_row.iloc[0].get("resource_working_hours", 8.0)) if not curr_res_row.empty else 8.0
                         if pd.isna(old_work_hours) or old_work_hours == 0:
                             old_work_hours = 8.0
                         task_effort = total_dur * old_work_hours * current_fte
                     
                     # Calculate savings: effort × (old_rate - new_rate)
                     savings = task_effort * (curr_rate - cand_rate)
                     
                     # Calculate new duration for display (optional, for info)
                     cand_work_hours = float(cand.get("resource_working_hours", 8.0))
                     if pd.isna(cand_work_hours) or cand_work_hours == 0:
                         cand_work_hours = 8.0
                     new_dur = task_effort / (cand_work_hours * current_fte) if cand_work_hours * current_fte > 0 else total_dur
                     duration_savings = total_dur - new_dur
                     
                     if savings > 0:
                         desc = f"Project: {proj_name} | Task: {act_row['activity_name']}\n"
                         desc += f"Swap **{curr_res_name}** with **{cand_name}**.\n"
                         desc += f"**Save ${savings:,.2f}** | Skill Match: {match_pct}%"
                         
                         story = f"**Trigger:** Found cheaper resource {cand_name} (${cand_rate:.1f}/hr) vs {curr_res_name} (${curr_rate:.1f}/hr). Skills match: {match_pct}%. Availability verified: {cand_name} is not assigned to any overlapping activities or projects during this task period, preventing overallocation."
                         
                         actions.append({
                            "id": str(uuid.uuid4()),
                            "type": ACTION_RES_SWAP,
                            "activity_id": act_id,
                            "description": desc,
                            "narrative": story,
                            "parameters": {
                                "old_res": current_res, "old_name": curr_res_name, "old_rate": curr_rate,
                                "new_res": cand_id, "new_name": cand_name, "new_rate": cand_rate,
                                "savings": savings, "match_pct": match_pct,
                                "task_planned_effort": task_effort,
                                "duration_savings": duration_savings,
                                "new_duration": new_dur
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
                 
                 # Calculate Cost Impact
                 # Cost = Duration * Work Hours * FTE * Rate
                 # Old remaining cost = rem_dur * work_hours * current_fte * rate
                 # New remaining cost = new_dur * work_hours * max_fte * rate
                 # Since new_dur = rem_dur * (current_fte / max_fte):
                 # New cost = (rem_dur * current_fte / max_fte) * work_hours * max_fte * rate
                 #         = rem_dur * current_fte * work_hours * rate
                 # So theoretically cost is the same (total hours unchanged)
                 # However, we show the actual calculated cost for transparency
                 cost_diff = 0.0
                 rate = 0.0
                 work_hours = 8.0
                 old_remaining_cost = 0.0
                 new_remaining_cost = 0.0
                 
                 try:
                     if not curr_res_row.empty:
                         rate = float(curr_res_row.iloc[0].get("resource_rate", 0))
                         work_hours = float(curr_res_row.iloc[0].get("resource_working_hours", 8))
                         
                         old_remaining_cost = rem_dur * work_hours * current_fte * rate
                         new_remaining_cost = new_dur * work_hours * max_fte * rate
                         cost_diff = new_remaining_cost - old_remaining_cost
                 except:
                     pass
                 
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
                         "saved_days": saved_days, "cost_impact": cost_diff,
                         "resource_rate": rate, "work_hours": work_hours,
                         "old_cost": old_remaining_cost, "new_cost": new_remaining_cost
                     },
                     "project_name": proj_name,
                     "resource_name": curr_res_name
                 })

            # --- Rule C: Compression ---
            # Trigger: 
            # 1. Active (Not Finished)
            # 2. Delayed (delay_carried_in > 0)
            # 3. Critical (on_critical_path OR total_float <= 0)
            # 4. Has Remaining Duration >= 2 (Can't compress 1 day task to 0 usually)
            
            act_fin = str(act_row.get("actual_finish", ""))
            is_finished = act_fin and (act_fin.lower() != "nan") and (act_fin.lower() != "nat")
            
            delay_in = float(act_row.get("delay_carried_in", 0))
            total_float = float(act_row.get("total_float_days", 999))
            
            # Criticality check: Explicit flag OR Float <= 0
            is_critical_effective = on_crit or (total_float <= 0)

            if not is_finished and rem_dur >= 2 and delay_in > 0 and is_critical_effective:
                # Max Compression: Min(20% of Duration, Delay Impact)
                # We don't want to compress MORE than the delay, nor more than 20% unrealistic
                
                max_pct_red = int(rem_dur * 0.2)
                
                # Rule: If duration is small (2-4 days), 20% is 0. 
                # But we should allow at least 1 day compression if it helps.
                if max_pct_red < 1:
                    max_pct_red = 1
                
                max_compress = min(max_pct_red, int(delay_in))
                
                # Ensure we don't compress MORE than needed (delay) but also respect physical limits
                # Also ensure we don't compress entire duration to 0 (already checked rem_dur >= 2)
                
                if max_compress >= 1:
                    new_dur_calc = rem_dur - max_compress
                    
                    # Narrative
                    story = (f"Activity {act_id} is on the Critical Path with {delay_in:.1f} days of carried-in delay. "
                             f"Compressing this {'Active' if act_row.get('actual_start') else 'Planned'} task will help catch up.")
                    
                    actions.append({
                        "id": str(uuid.uuid4()),
                        "type": ACTION_COMPRESS,
                        "activity_id": act_id,
                        "description": f"Compress Duration (Max Rec: {max_compress} days)",
                        "narrative": story,
                        "parameters": {
                            "reduce_by_days": int(max_compress), # Default recommendation
                            "old_dur": rem_dur,
                            "new_dur": new_dur_calc,
                            "delay_carried_in": delay_in,
                            "planned_finish": act_row.get("planned_finish", "Unknown")
                        }
                    })

            # --- Rule D: Fast-Tracking ---
            # Trigger: Critical Path AND FS Predecessors
            # Logic: Convert "Finish-to-Start" (FS) to "Start-to-Start" (SS) + Lag
            # Savings = Predecessor Duration - Lag
            
            if on_crit:
                  preds_str = str(act_row.get("predecessors", ""))
                  # Normalize
                  if preds_str == "nan" or not preds_str:
                      preds_str = str(act_row.get("predecessor_id", ""))
                  
                  if preds_str and preds_str.lower() != "nan":
                      # We need to know the Predecessor's Duration to calc savings.
                      # We have 'max_pred_duration' helper? No.
                      # Ideally we parse the predecessor ID string "2FS", "3".
                      # For MVP, assume the primary predecessor is the critical driver.
                      # We can't easily look up predecessor duration here without a lookup map.
                      # But wait, we iterate `act_row`. We assume we have access to the full DF?
                      # Yes, `df` is passed to `generate_actions`.
                      
                      # Quick Lookup for Predecessor Duration
                      # Parse first predecessor for simplicity or max?
                      # "2" or "2FS".
                      import re
                      p_ids = re.findall(r"(\d+)", preds_str)
                      
                      max_saving = 0
                      best_pred = None
                      
                      for pid in p_ids:
                          try:
                              # Robust ID Lookup (Str Comparison)
                              p_row = df_schedule[df_schedule["activity_id"].astype(str) == str(pid)]
                              
                              if not p_row.empty:
                                  p_dur = float(p_row.iloc[0].get("planned_duration", 0))
                                  # Savings = Duration - 2 (Lag).
                                  # If duration is 5, SS+2 means we start 2 days after pred starts.
                                  # VS starting 5 days after (FS).
                                  # Saving = 5 - 2 = 3.
                                  save = p_dur - 2
                                  if save > max_saving:
                                      max_saving = save
                                      best_pred = pid
                          except:
                              pass
                      
                      if max_saving > 0:
                          story = (f"Activity {act_id} waits for Activity {best_pred} to finish. "
                                   f"Fast-tracking allows it to start 2 days after {best_pred} starts, "
                                   f"saving {int(max_saving)} days.")
                                   
                          actions.append({
                              "id": str(uuid.uuid4()),
                              "type": ACTION_FAST_TRACK,
                              "activity_id": act_id,
                              "description": f"Fast-Track via SS+2d (Save ~{int(max_saving)} days)",
                              "narrative": story,
                              "parameters": {
                                  "target_type": "SS", 
                                  "lag": 2,
                                  "estimated_savings": int(max_saving),
                                  "related_pred_id": best_pred
                              }
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

            # --- Rule F: Crashing (Overload) ---
            # Trigger: Critical Path AND Active AND Current FTE > 0
            # Logic: Double the FTE (Overtime/Double Shift) to halve the duration.
            # Allows Overload (New FTE > Max FTE).
            if on_crit and rem_dur > 1 and current_fte > 0:
                 # Proposed: Double the effort
                 new_fte_crash = current_fte * 2.0
                 
                 # New Duration
                 new_dur_crash = rem_dur / 2.0
                 saved_days_crash = rem_dur - new_dur_crash
                 
                 # Check Overload
                 is_overloaded = new_fte_crash > max_fte
                 overload_amt = new_fte_crash - max_fte if is_overloaded else 0
                 
                 story = (f"**Trigger:** Critical Task requires aggressive recovery. "
                          f"**Action:** CRASHING. Double the resource effort ({current_fte} -> {new_fte_crash} FTE). "
                          f"**Result:** Duration cut in half ({rem_dur:.1f} -> {new_dur_crash:.1f} days). "
                          f"{'⚠️ Causes Resource Overload.' if is_overloaded else ''}")
                 
                 actions.append({
                     "id": str(uuid.uuid4()),
                     "type": ACTION_CRASHING,
                     "activity_id": act_id,
                     "description": f"Crash Task (Double FTE{' - Overload' if is_overloaded else ''})",
                     "narrative": story,
                     "parameters": {
                         "old_fte": current_fte, "new_fte": new_fte_crash,
                         "old_dur": rem_dur, "new_dur": new_dur_crash,
                         "saved_days": saved_days_crash,
                         "is_overloaded": is_overloaded
                     }
                 })
            
    return actions

def apply_action(df, action, df_resource=None):
    """
    Mutates df in place applying the action.
    Returns success (bool), message (str)
    
    Args:
        df: DataFrame to modify
        action: Action dictionary with type and parameters
        df_resource: Optional resource DataFrame for looking up resource_working_hours
    """
    act_id = action.get("activity_id")
    # Find row index (Robust String Comparison)
    mask = df["activity_id"].astype(str) == str(act_id)
    if not mask.any():
        return False, f"Activity {act_id} not found"
        
    idx = df[mask].index[0]
    
    # Log Metadata (Always set this first)
    df.at[idx, "last_change_type"] = action["type"]
    df.at[idx, "last_change_id"] = action.get("id")
    
    if action["type"] == ACTION_RES_SWAP:
        new_res = action["parameters"]["new_res"]
        df.at[idx, "resource_id"] = new_res
        
        # Recalculate duration based on task_planned_effort (effort-based approach)
        # Duration = task_planned_effort / (resource_working_hours × fte_allocation)
        if "task_planned_effort" in df.columns:
            task_effort = float(df.at[idx, "task_planned_effort"]) if pd.notna(df.at[idx, "task_planned_effort"]) else None
            
            if task_effort is not None and task_effort > 0:
                # Get FTE allocation
                fte = float(df.at[idx, "fte_allocation"]) if pd.notna(df.at[idx, "fte_allocation"]) else 1.0
                if fte == 0:
                    fte = 1.0  # Safety default
                
                # Look up new resource's working hours
                new_work_hours = 8.0  # Default
                if df_resource is not None:
                    res_row = df_resource[df_resource["resource_id"].astype(str) == str(new_res)]
                    if not res_row.empty:
                        new_work_hours = float(res_row.iloc[0].get("resource_working_hours", 8.0))
                        if pd.isna(new_work_hours) or new_work_hours == 0:
                            new_work_hours = 8.0
                
                # Recalculate duration: effort / (work_hours × fte)
                new_duration = task_effort / (new_work_hours * fte)
                
                # Update duration columns
                df.at[idx, "remaining_duration_days"] = new_duration
                df.at[idx, "planned_duration"] = new_duration  # Sync for CPM consistency
                
                return True, f"Swapped resource to {new_res}. Duration recalculated: {new_duration:.1f} days (from {task_effort:.1f} hrs effort)."
        
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
            # Sync planned_duration so CPM engine sees it (for consistency with remaining)
            # CPM engine prioritizes planned_duration, so we must update it for dates to propagate
            df.at[idx, "planned_duration"] = new_dur
            
        return True, f"Increased FTE to {new_fte}."
        
    elif action["type"] == ACTION_COMPRESS:
        # idx is already found above
        params = action["parameters"]
        
        # Check for user override 'new_dur' first (from UI input), else calculate from 'reduce_by_days'
        if "new_dur_input" in params:
             new_dur = float(params["new_dur_input"])
        else:
             reduce_by = params.get("reduce_by_days", 0)
             curr_dur = float(df.at[idx, "remaining_duration_days"])
             new_dur = max(1, curr_dur - reduce_by)
             
        old_dur = float(df.at[idx, "remaining_duration_days"]) if "remaining_duration_days" in df.columns else 0.0
        
        # Update Duration
        df.at[idx, "remaining_duration_days"] = new_dur
        # Sync planned_duration so CPM engine sees it (for consistency with remaining)
        df.at[idx, "planned_duration"] = new_dur
        
        # Cost Update (Proportional)
        # New Rem Cost = Old Rem Cost * (New Dur / Old Dur)
        if old_dur > 0:
            old_rem_cost = float(df.at[idx, "remaining_cost"]) if "remaining_cost" in df.columns else 0.0
            ratio = new_dur / old_dur
            new_rem_cost = old_rem_cost * ratio
            df.at[idx, "remaining_cost"] = new_rem_cost
            
            # Also update eac_cost roughly if present
                
            # Also update eac_cost roughly if present
            if "eac_cost" in df.columns:
                 # EAC = AC + Remaining. Assume AC doesn't change for compression of future work.
                 ac = float(df.at[idx, "actual_cost"]) if "actual_cost" in df.columns else 0.0
                 df.at[idx, "eac_cost"] = ac + new_rem_cost

        # Set Metadata for Highlighting
        df.at[idx, "last_change_type"] = ACTION_COMPRESS
        df.at[idx, "last_change_id"] = action.get("id")
        
        return True, f"Compressed duration to {new_dur:.1f} days. Cost updated proportionally."
        
    elif action["type"] == ACTION_FAST_TRACK:
        # We need to Mutate the Predecessor String
        # Current: "2" or "2FS" -> New: "2SS+2d"
        # We find the pred specified in parameters or all?
        # params: related_pred_id
        target_pred = action["parameters"].get("related_pred_id")
        
        current_preds = str(df.at[idx, "predecessor_id"]) 
        # (Assuming predecessor_id is the main col used by engine, logic elsewhere checks both)
        
        if target_pred:
            # Simple Replace: "2" -> "2SS+2d". "2FS" -> "2SS+2d"
            # Regex to match target_pred followed by optional FS/SS etc
            import re
            # Match strict ID
            pattern = re.compile(rf"\b{target_pred}(?:FS|SS|FF|SF)?(?:\+\d+d|-\d+d)?\b")
            
            new_preds = pattern.sub(f"{target_pred}SS+2d", current_preds)
            df.at[idx, "predecessor_id"] = new_preds
            # Also update 'predecessors' col if exists and differs
            if "predecessors" in df.columns:
                 curs = str(df.at[idx, "predecessors"])
                 new_p = pattern.sub(f"{target_pred}SS+2d", curs)
                 df.at[idx, "predecessors"] = new_p

        # Metadata
        df.at[idx, "last_change_type"] = ACTION_FAST_TRACK
        df.at[idx, "last_change_id"] = action.get("id")
        
        return True, f"Fast-tracked dependency on Activity {target_pred} (Converted to SS+2d)."
        
    elif action["type"] == ACTION_DEFERRAL:
        df.at[idx, "is_deferred"] = True
        return True, "Activity deferred (removed from calculation)."
        
    elif action["type"] == ACTION_CRASHING:
        # Same mechanics as FTE Adjustment, but sets highlighting differently
        new_fte = float(action["parameters"]["new_fte"])
        old_fte = float(action["parameters"]["old_fte"])
        
        df.at[idx, "fte_allocation"] = new_fte
        
        # Recalc duration
        current_dur = float(df.at[idx, "remaining_duration_days"])
        if new_fte > 0:
            new_dur = current_dur * (old_fte / new_fte)
            df.at[idx, "remaining_duration_days"] = new_dur
            # Sync planned_duration so CPM engine sees it (for consistency with remaining)
            # CPM engine prioritizes planned_duration, so we must update it for dates to propagate
            df.at[idx, "planned_duration"] = new_dur
            
        return True, f"Crashed task! FTE doubled to {new_fte}. Duration halved."
        
    return False, "Unknown Action Type"
