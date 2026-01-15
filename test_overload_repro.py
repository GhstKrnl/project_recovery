import unittest
import pandas as pd
import numpy as np
import cost_engine

class TestOverloadRepro(unittest.TestCase):

    def test_duplicate_resource_causes_overload(self):
        """
        Reproduces the scenario where a duplicate resource entry causes
        artificial overload on non-overlapping tasks.
        """
        # 1. Schedule: Two tasks, NON-overlapping
        df_schedule = pd.DataFrame([
            {
                "activity_id": "T1",
                "resource_id": "R1",
                "planned_duration": 5,
                "actual_duration": 0,
                "remaining_duration_days": 5,
                "fte_allocation": 1.0,
                "forecast_start_date": pd.Timestamp("2026-01-01"),
                "forecast_finish_date": pd.Timestamp("2026-01-05")
            },
            {
                "activity_id": "T2",
                "resource_id": "R1", 
                "planned_duration": 4,
                "actual_duration": 0,
                "remaining_duration_days": 4,
                "fte_allocation": 1.0,
                "forecast_start_date": pd.Timestamp("2026-01-27"),
                "forecast_finish_date": pd.Timestamp("2026-01-30")
            }
        ])
        
        # 2. Resource DB: DUPLICATE Entry for R1
        # This simulates a bad CSV join or duplicate rows in source file
        df_resource = pd.DataFrame([
            {
                "resource_id": "R1",
                "resource_rate": 100,
                "resource_max_fte": 1.0, 
                "resource_working_hours": 8
            },
            { # DUPLICATE
                "resource_id": "R1",
                "resource_rate": 100,
                "resource_max_fte": 1.0,
                "resource_working_hours": 8
            }
        ])
        
        # 3. Run Calculation
        enriched_df = cost_engine.calculate_costs(df_schedule, df_resource)
        
        # Check if row count exploded (Should be 2 if clean, 4 if duped)
        print(f"\nEnriched DF Length: {len(enriched_df)}")
        
        # 4. Check Overload
        stats = cost_engine.check_resource_availability(enriched_df)
        r1_stats = stats.get("R1", {})
        
        print(f"R1 Stats: {r1_stats}")
        
        # If bug exists, overload > 0 because every day has 2.0 FTE (1.0 * 2 rows)
        # If fixed, overload == 0
        self.assertGreater(r1_stats.get('overload_days_count', 0), 0, "Should duplicate overload if bug exists")

if __name__ == '__main__':
    unittest.main()
