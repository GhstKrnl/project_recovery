import pandas as pd
import numpy as np
import forecasting_engine

def calculate_evm_metrics(df_schedule, status_date=None, eac_method_index=0):
    """
    Calculates project-level EVM metrics.
    
    eac_method_index corresponding to user selection:
    0: EAC = AC + Remaining Cost (Bottom-up / MVP 5 logic)
    1: EAC = BAC / CPI
    2: EAC = AC + (BAC - EV)
    3: EAC = AC + Bottom-up ETC (Assuming MVP 5 Remaining Cost is the bottom-up ETC)
    4: EAC = AC + [(BAC - EV) / (CPI * SPI)]
    
    Returns a dictionary of metrics.
    """
    
    # Defaults
    if status_date is None:
        status_date = pd.Timestamp.now().date()
    else:
        status_date = pd.to_datetime(status_date).date()
        
    metrics = {
        "PV": 0.0,
        "EV": 0.0,
        "AC": 0.0,
        "BAC": 0.0,
        "CV": 0.0,
        "SV": 0.0,
        "VAC": 0.0,
        "CPI": 1.0, # Default to 1 to avoid div/0 issues initially? Or 0? 1 is "on plan".
        "SPI": 1.0,
        "EAC": 0.0,
        "ETC": 0.0,
        "TCPI_BAC": 0.0,
        "TCPI_EAC": 0.0,
    }
    
    if df_schedule.empty:
        return metrics
        
    # Helpers
    def get_val(row, col):
        return row.get(col, 0) if pd.notna(row.get(col)) else 0

    # 1. Aggregate Basic Metrics (BAC, AC, EV, Remaining Cost for Bottom-Up)
    total_bac = 0.0
    total_ac = 0.0
    total_ev = 0.0
    total_pv = 0.0
    total_remaining_cost = 0.0 # For Bottom-Up
    
    for _, row in df_schedule.iterrows():
        planned_cost = get_val(row, "planned_cost")
        actual_cost = get_val(row, "actual_cost")
        remaining_cost = get_val(row, "remaining_cost") # Computed in MVP 5
        
        pct_comp = get_val(row, "percent_complete") / 100.0
        
        # BAC
        total_bac += planned_cost
        
        # AC
        total_ac += actual_cost
        
        # EV = BAC * % Complete
        total_ev += (planned_cost * pct_comp)
        
        # Remaining (Bottom-Up ETC input)
        total_remaining_cost += remaining_cost

        # PV Calculation
        # Linear burn based on Status Date
        # If today < Start, PV=0. If today > Finish, PV=BAC.
        # Else fraction.
        
        p_start = row.get("planned_start")
        p_finish = row.get("planned_finish")
        
        if pd.notna(p_start) and pd.notna(p_finish):
            # Using working days for valid fraction
            # PV = BAC * (WorkingDays(Start, Status) / WorkingDays(Start, Finish))
            
            # Convert to ISO strings for existing utils
            try:
                # Total Duration of task
                total_dur = forecasting_engine.count_working_days(p_start, p_finish, inclusive=True)
                
                # Elapsed Duration
                # If status < start -> 0
                # If status > finish -> total_dur
                
                # Check bounds
                # We need simple comparison. String comparison works for ISO YYYY-MM-DD
                s_date_str = status_date.isoformat()
                
                if s_date_str >= str(p_finish):
                    elapsed = total_dur
                elif s_date_str < str(p_start):
                    elapsed = 0
                else:
                    elapsed = forecasting_engine.count_working_days(p_start, s_date_str, inclusive=True)
                
                fraction = 0
                if total_dur > 0:
                    fraction = elapsed / total_dur
                elif total_dur == 0:
                    # milestone. If passed, 1.0. 
                    fraction = 1.0 if elapsed > 0 else 0.0
                    
                total_pv += (planned_cost * fraction)
                
            except:
                pass
        else:
            # If no dates, assume no PV unless complete?
            pass

    metrics["BAC"] = total_bac
    metrics["AC"] = total_ac
    metrics["EV"] = total_ev
    metrics["PV"] = total_pv
    
    # 2. Indices & Variances
    metrics["CV"] = total_ev - total_ac
    metrics["SV"] = total_ev - total_pv
    
    # Divisions (Handle 0)
    if total_ac != 0:
        metrics["CPI"] = total_ev / total_ac
    else:
        metrics["CPI"] = 1.0 if total_ev == 0 else 0.0 # If Cost is 0 but EV exists -> Infinite efficiency? Represent as 0 or null? Code says "No rounding tricks". "Division by zero -> show -". Logic layer returns float?
        # Let's keep it float, UI handles display.
        if total_ev > 0: metrics["CPI"] = float('inf') 
        
    if total_pv != 0:
        metrics["SPI"] = total_ev / total_pv
    else:
        # If PV 0 and EV > 0?
        if total_ev > 0: metrics["SPI"] = float('inf')
        else: metrics["SPI"] = 1.0
        
    # 3. EAC Calculation
    # 10. EAC = AC + remaining_cost (Bottom Up / MVP 5)
    eac_10 = total_ac + total_remaining_cost
    
    # 11. EAC = BAC / CPI
    if metrics["CPI"] != 0 and metrics["CPI"] != float('inf'):
        eac_11 = total_bac / metrics["CPI"]
    else:
        eac_11 = total_bac # Fallback? Or Infinite?
        
    # 12. EAC = AC + (BAC - EV)
    eac_12 = total_ac + (total_bac - total_ev)
    
    # 13. EAC = AC + Bottom-up ETC
    # Same as #10 if we treat MVP 5 Remaining Cost as the bottom-up ETC.
    eac_13 = eac_10 
    
    # 14. EAC = AC + [(BAC - EV) / (CPI * SPI)]
    denom = metrics["CPI"] * metrics["SPI"]
    if denom != 0 and denom != float('inf'):
        eac_14 = total_ac + ((total_bac - total_ev) / denom)
    else:
        eac_14 = eac_10 # Fallback
        
    # Selection
    eac_options = [eac_10, eac_11, eac_12, eac_13, eac_14]
    
    # Ensure index is valid
    if eac_method_index < 0 or eac_method_index >= len(eac_options):
        eac_final = eac_10
    else:
        eac_final = eac_options[eac_method_index]
        
    metrics["EAC"] = eac_final
    
    # 4. Final Derived
    metrics["VAC"] = total_bac - metrics["EAC"]
    metrics["ETC"] = metrics["EAC"] - total_ac
    
    # TCPI
    # (BAC - EV) / (BAC - AC)
    remaining_work = total_bac - total_ev
    remaining_budget_bac = total_bac - total_ac
    if remaining_budget_bac != 0:
        metrics["TCPI_BAC"] = remaining_work / remaining_budget_bac
    else:
        metrics["TCPI_BAC"] = float('inf') if remaining_work > 0 else 0
        
    # (BAC - EV) / (EAC - AC)
    remaining_budget_eac = metrics["EAC"] - total_ac
    if remaining_budget_eac != 0:
        metrics["TCPI_EAC"] = remaining_work / remaining_budget_eac
    else:
        metrics["TCPI_EAC"] = float('inf') if remaining_work > 0 else 0
        
    return metrics
