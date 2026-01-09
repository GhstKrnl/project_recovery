import unittest
import pandas as pd
import cost_engine

class TestCostEngine(unittest.TestCase):

    def test_cost_calculation(self):
        # 2 activities, 1 resource
        # Act 1: 5 days, 8h, 1.0 FTE. Rate 100.
        # Cost = 5 * 8 * 1 * 100 = 4000.
        
        df_sched = pd.DataFrame([{
            "activity_id": 1,
            "resource_id": "R1",
            "planned_duration": 5,
            "actual_duration": 2, # Partial
            "remaining_duration_days": 3,
            "fte_allocation": 1.0,
            "forecast_start_date": "2023-01-02", # Mon
            "forecast_finish_date": "2023-01-06" # Fri
        }])
        
        df_res = pd.DataFrame([{
            "resource_id": "R1",
            "resource_rate": 100,
            "resource_working_hours": 8,
            "resource_max_fte": 1.0
        }])
        
        res_df = cost_engine.calculate_costs(df_sched, df_res)
        row = res_df.iloc[0]
        
        self.assertEqual(row["planned_cost"], 4000)
        self.assertEqual(row["actual_cost"], 1600) # 2*8*1*100
        self.assertEqual(row["eac_cost"], 4000) # (2+3)*...

    def test_resource_overload(self):
        # R1 Max FTE 1.0
        # Act 1: Mon-Fri. 1.0 FTE.
        # Act 2: Mon-Mon. 0.5 FTE.
        # Overlap on Mon. Total 1.5 > 1.0. Overload = 1 day.
        
        # We need `check_resource_availability` using cost_df structure
        cost_df = pd.DataFrame([
            {
                "resource_id": "R1",
                "resource_max_fte": 1.0,
                "forecast_start_date": "2023-01-02", # Mon
                "forecast_finish_date": "2023-01-06", # Fri
                "fte_allocation": 1.0
            },
            {
                "resource_id": "R1",
                "resource_max_fte": 1.0,
                "forecast_start_date": "2023-01-02", # Mon
                "forecast_finish_date": "2023-01-02", # Mon
                "fte_allocation": 0.5
            }
        ])
        
        stats = cost_engine.check_resource_availability(cost_df)
        r1_stats = stats["R1"]
        
        self.assertEqual(r1_stats["overload_days_count"], 1) # Only Monday is 1.5
        self.assertEqual(r1_stats["peak_fte"], 1.5)

if __name__ == '__main__':
    unittest.main()
