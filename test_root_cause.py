import unittest
import pandas as pd
import root_cause_engine

class TestRootCause(unittest.TestCase):

    def test_scenarios(self):
        # Setup comprehensive test data
        data = [
            # 1. Critical Slippage with Cost Overrun
            {
                "activity_id": "CRIT_COST", 
                "on_critical_path": True, 
                "task_created_delay": 5, 
                "planned_cost": 100, "actual_cost": 200 # +100 Overrun
            },
            # 2. Critical Slippage (No Cost Overrun)
            {
                "activity_id": "CRIT_ONLY", 
                "on_critical_path": True, 
                "task_created_delay": 5, 
                "planned_cost": 200, "actual_cost": 150 # Underrun
            },
            # 3. Resource Overload (with Cost Overrun)
            {
                "activity_id": "RES_COST",
                "total_schedule_delay": 3,
                "resource_id": "R_OVER",
                "planned_cost": 100, "actual_cost": 150 # +50
            },
            # 4. Cost Overrun Only
            {
                "activity_id": "COST_ONLY",
                "planned_cost": 100, "actual_cost": 500 # +400
            },
            # 5. Risk (Inferred)
            {
                "activity_id": "RISK_TASK",
                "remaining_duration_days": 20, # High
                "remaining_cost": 2000, # High
                "total_float_days": 0, # Low
                "planned_cost": 0, "actual_cost": 0 # No deviation
            }
        ]
        
        df = pd.DataFrame(data)
        
        # Resource stats
        r_stats = {"R_OVER": {"overload_days_count": 5}}
        
        results = root_cause_engine.execute_root_cause_analysis(df, r_stats)
        results = results.set_index("Activity")
        
        # Assertions
        
        # 1. CRIT_COST
        # Category: Critical Path Slippage (Rule 1)
        # Impact Days: 5
        # Impact Cost: 100 (Should be visible!)
        row = results.loc["CRIT_COST"]
        self.assertEqual(row["Root Cause Category"], root_cause_engine.CAT_CRITICAL_SLIP)
        self.assertEqual(row["Impact Days"], 5)
        self.assertEqual(row["Impact Cost"], 100) # This confirms user's fix
        
        # 2. CRIT_ONLY
        # Impact Cost: 0 (Underrun ignored or 0)
        row = results.loc["CRIT_ONLY"]
        self.assertEqual(row["Root Cause Category"], root_cause_engine.CAT_CRITICAL_SLIP)
        self.assertEqual(row["Impact Cost"], 0)
        
        # 3. RES_COST
        # Category: Resource (Rule 3 > Rule 4)
        # Impact Cost: 50
        row = results.loc["RES_COST"]
        self.assertEqual(row["Root Cause Category"], root_cause_engine.CAT_RES_OVERLOAD)
        self.assertEqual(row["Impact Cost"], 50)
        
        # 4. COST_ONLY
        # Category: Cost Overrun
        row = results.loc["COST_ONLY"]
        self.assertEqual(row["Root Cause Category"], root_cause_engine.CAT_COST_OVERRUN)
        self.assertEqual(row["Impact Cost"], 400)
        
        # 5. RISK_TASK
        row = results.loc["RISK_TASK"]
        self.assertEqual(row["Root Cause Category"], root_cause_engine.CAT_RISK)

if __name__ == '__main__':
    unittest.main()
