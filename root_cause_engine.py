import pandas as pd
import numpy as np

# Rule Constants
CAT_CRITICAL_SLIP = "Critical Path Slippage"
CAT_NEG_FLOAT = "Negative Float / Logic Constraint"
CAT_RES_OVERLOAD = "Resource Overallocation"
CAT_COST_OVERRUN = "Cost Overrun"
CAT_RISK = "Risk / Uncertainty (Proxy)"

CERTAINTY_DIRECT = "Direct"
CERTAINTY_INDIRECT = "Indirect"
CERTAINTY_INFERRED = "Inferred (Risk)"

def execute_root_cause_analysis(df_schedule, resource_stats):
    """
    Applies strict rule-based classification to every activity.
    Returns a DataFrame with columns: 
    [Activity, Root Cause Category, Impact Days, Impact Cost, Cause Certainty, Explanation]
    Ranked by Impact.
    """
    results = []
    
    if df_schedule.empty:
        return pd.DataFrame()

    def get_val(row, col, default=0):
        val = row.get(col)
        if pd.isna(val) or val == "":
            return default
        return val

    for idx, row in df_schedule.iterrows():
        # Extracted metrics
        on_crit = get_val(row, "on_critical_path", False)
        task_created_delay = float(get_val(row, "task_created_delay", 0))
        total_float = float(get_val(row, "total_float_days", 0))
        total_sched_delay = float(get_val(row, "total_schedule_delay", 0))
        
        resource_id = get_val(row, "resource_id", None)
        
        planned_cost = float(get_val(row, "planned_cost", 0))
        actual_cost = float(get_val(row, "actual_cost", 0))
        
        remaining_duration = float(get_val(row, "remaining_duration_days", 0))
        
        # Determine Resource Overload status
        # resource_stats is { rid: { 'overload_days_count': N, ... } }
        res_overload_days = 0
        if resource_id and str(resource_id) in resource_stats:
            res_overload_days = resource_stats[str(resource_id)].get("overload_days_count", 0)
            
        # Defaults
        category = None
        certainty = None
        explanation = ""
        impact_days = 0
        
        # Calculate Cost Impact globally (if valid numbers)
        # Impact Cost = Actual - Planned (if > 0, i.e., Overrun)
        # User might want to see Underrun? "Impact Cost" usually implies negative impact.
        # Let's stick to Overrun > 0.
        cost_variance = actual_cost - planned_cost
        impact_cost = max(0.0, cost_variance)
        
        # --- Rule Hierarchy ---
        
        # 1. Critical Path Slippage
        if on_crit and task_created_delay > 0:
            category = CAT_CRITICAL_SLIP
            certainty = CERTAINTY_DIRECT
            impact_days = task_created_delay
            explanation = f"Critical task created {task_created_delay} days of delay."
            
        # 2. Negative Float / Logic Constraint
        elif total_float < 0: # Strict check
             # And delay driven by logic? Implicit if float is negative.
             category = CAT_NEG_FLOAT
             certainty = CERTAINTY_INDIRECT
             impact_days = abs(total_float) # Negative float magnitude is the impact/slip needed to recover
             explanation = f"Negative float ({total_float} days) indicates infeasible logic."
             
        # 3. Resource Overallocation
        elif res_overload_days > 0:
             # AND delay attributable? 
             # If Task has Created Delay OR Total Delay?
             if total_sched_delay > 0:
                 category = CAT_RES_OVERLOAD
                 certainty = CERTAINTY_INDIRECT
                 impact_days = total_sched_delay # Attributing total delay to resource contention
                 explanation = f"Resource '{resource_id}' is overloaded by {res_overload_days} days."
             else:
                 # No delay yet -> Risk?
                 pass 
                 
        # 4. Cost Overrun
        if category is None: # Only if not yet classified
            if impact_cost > 0:
                category = CAT_COST_OVERRUN
                certainty = CERTAINTY_DIRECT
                # impact_cost already set
                explanation = f"Actual cost exceeds planned by ${impact_cost:,.2f}."
                
        # 5. Risk / Uncertainty
        if category is None:
            # Check criteria: High remaining, High cost exp, Low float.
            # "AND ALL are true"
            # Define thresholds
            HIGH_REM_DUR = 10 # heuristic
            HIGH_COST_EXP = 1000 # heuristic
            LOW_FLOAT = 5 # heuristic
            
            # Cost Exposure = Remaining Cost? Or Planned? "High cost exposure".
            remaining_cost = float(get_val(row, "remaining_cost", 0))
            
            if (remaining_duration > HIGH_REM_DUR and 
                remaining_cost > HIGH_COST_EXP and 
                total_float < LOW_FLOAT):
                
                category = CAT_RISK
                certainty = CERTAINTY_INFERRED
                explanation = "High risk profile: Long duration, high cost, low float."
        
        # If still None? (e.g. On Time, On Budget, No Risk).
        # We only output "Why is this activity contributing...".
        # If it's NOT contributing, maybe we exclude it?
        # But "Each activity must be assigned exactly ONE category." 
        #Implies valid classification for problem tasks. 
        # Tasks that are fine -> "No Issue"?
        # "First match wins".
        # If none match, return "On Track" or exclude from "Root Cause" list?
        # "Root Cause Summary Table (ranked)". Usually implies showing the PROBLEMS.
        # But for completeness let's label them "On Track" but filter out later if needed.
        
        if category:
            results.append({
                "Activity": row["activity_id"],
                "Root Cause Category": category,
                "Impact Days": impact_days,
                "Impact Cost": impact_cost,
                "Cause Certainty": certainty,
                "Explanation": explanation,
                # For sorting
                "_sort_certainty": 3 if certainty == CERTAINTY_DIRECT else (2 if certainty == CERTAINTY_INDIRECT else 1)
            })

    # Ranking Logic
    # 1. impact_days (desc)
    # 2. impact_cost (desc)
    # 3. Cause Certainty priority (Direct > Indirect > Inferred)
    
    df_res = pd.DataFrame(results)
    if not df_res.empty:
        df_res = df_res.sort_values(
            by=["Impact Days", "Impact Cost", "_sort_certainty"], 
            ascending=[False, False, False]
        )
        # Drop helper
        df_res = df_res.drop(columns=["_sort_certainty"])
        
    return df_res
