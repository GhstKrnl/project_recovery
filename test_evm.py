import unittest
import pandas as pd
import evm_engine

class TestEVM(unittest.TestCase):

    def test_basic_evm(self):
        # 1 Task. Planned Cost 1000.
        # Planned: 10 days.
        # Status: Day 5 (50% elapsed). PV = 500.
        # % Comp: 20%. EV = 200.
        # AC: 300.
        
        df = pd.DataFrame([{
            "planned_cost": 1000,
            "actual_cost": 300,
            "remaining_cost": 800, # implied
            "percent_complete": 20,
            "planned_start": "2023-01-02", # Mon
            "planned_finish": "2023-01-13", # Fri (10 days working)
            "status_date": "2023-01-09" # Start (2nd) -> 9th. 2nd,3,4,5,6 (Fri), 9 (Mon).
            # Working days: 2,3,4,5,6 = 5 days. 9th is inclusive? 
            # Logic: count_working_days(start, status, inclusive=True).
        }])
        
        # Mock status date in call
        # 2nd to 9th inclusive: Mon(2), Tue(3), Wed(4), Thu(5), Fri(6), Mon(9). That's 6 days?
        # Let's check logic:
        # 2-6 = 5 days. 9th is 6th day.
        # Total duration: 2-13. 2-6 (5), 9-13 (5) = 10 days.
        # PV Fraction = 6/10 = 0.6. PV = 600.
        
        metrics = evm_engine.calculate_evm_metrics(df, status_date="2023-01-09")
        
        self.assertEqual(metrics["BAC"], 1000)
        self.assertEqual(metrics["AC"], 300)
        self.assertEqual(metrics["EV"], 200) # 20% of 1000
        self.assertEqual(metrics["PV"], 600) # 60% of 1000
        
        # CPI = EV / AC = 200 / 300 = 0.66
        self.assertAlmostEqual(metrics["CPI"], 0.666, places=2)
        
        # SPI = EV / PV = 200 / 600 = 0.33
        self.assertAlmostEqual(metrics["SPI"], 0.333, places=2)

    def test_eac_formulas(self):
        # BAC=100. AC=50. EV=50. CPI=1. SPI=1. Rem=50.
        df = pd.DataFrame([{
            "planned_cost": 100,
            "actual_cost": 50,
            "remaining_cost": 50,
            "percent_complete": 50,
            "planned_start": "2023-01-01",
            "planned_finish": "2023-01-10"
        }])
        
        # Method 0: AC + Rem = 50 + 50 = 100
        m0 = evm_engine.calculate_evm_metrics(df, eac_method_index=0)
        self.assertEqual(m0["EAC"], 100)
        
        # Method 1: BAC / CPI. CPI = 50/50=1. 100/1 = 100.
        m1 = evm_engine.calculate_evm_metrics(df, eac_method_index=1)
        self.assertEqual(m1["EAC"], 100)
        
    def test_div_zero(self):
        df = pd.DataFrame([{"planned_cost": 100, "actual_cost": 0, "percent_complete": 0}])
        m = evm_engine.calculate_evm_metrics(df)
        self.assertEqual(m["CPI"], 1.0) # EV=0, AC=0 -> logic set to 1.0 or handled?
        # Logic: if AC=0: if EV=0 -> 1.0? 
        # Code: if AC!=0... else: if EV==0: 1.0. Correct.

if __name__ == '__main__':
    unittest.main()
