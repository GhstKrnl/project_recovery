import unittest
import pandas as pd
import networkx as nx
from dag_engine import parse_dependency_string, build_dag_and_validate
import cpm_engine

class TestCPMEngine(unittest.TestCase):

    def setUp(self):
        # Helper to create df
        pass

    def test_duration_calc(self):
        # Mon to Fri = 5 days
        df = pd.DataFrame({
            "activity_id": [1],
            "planned_start": ["2023-01-02"], # Mon
            "planned_finish": ["2023-01-06"] # Fri
        })
        durs = cpm_engine.calculate_durations(df)
        self.assertEqual(durs[1], 5)

        # Mon to Mon = 1 day
        df2 = pd.DataFrame({
            "activity_id": [2],
            "planned_start": ["2023-01-02"], 
            "planned_finish": ["2023-01-02"] 
        })
        durs2 = cpm_engine.calculate_durations(df2)
        self.assertEqual(durs2[2], 1)

    def test_simple_fs_chain(self):
        # 1 (5d) -> 2 (5d)
        data = {
            "activity_id": [1, 2],
            "activity_name": ["A", "B"],
            "planned_start": ["2023-01-02", "2023-01-02"], 
            "planned_finish": ["2023-01-06", "2023-01-06"], # Both 5d
            "predecessor_id": ["", "1FS"]
        }
        df = pd.DataFrame(data)
        G, _ = build_dag_and_validate(df)
        res = cpm_engine.run_cpm(df, G)
        
        # A: ES=0, EF=5. 
        self.assertEqual(res[1]["ES"], 0)
        self.assertEqual(res[1]["EF"], 5)
        # B: ES=5, EF=10. (Because FS means Start >= Finish(A)=5)
        self.assertEqual(res[2]["ES"], 5)
        self.assertEqual(res[2]["EF"], 10)
        
        # Critical Path: Both should be critical
        self.assertTrue(res[1]["on_critical_path"])
        self.assertTrue(res[2]["on_critical_path"])

    def test_ss_lag(self):
        # 1 (5d) 
        # 2 (5d) SS+2d from 1
        data = {
            "activity_id": [1, 2],
            "planned_start": ["2023-01-02", "2023-01-02"], 
            "planned_finish": ["2023-01-06", "2023-01-06"],
            "predecessor_id": ["", "1SS+2d"]
        }
        df = pd.DataFrame(data)
        G, _ = build_dag_and_validate(df)
        res = cpm_engine.run_cpm(df, G)
        
        # 1: ES=0, EF=5
        # 2: ES >= ES(1) + 2 = 2. EF=7.
        self.assertEqual(res[2]["ES"], 2)
        self.assertEqual(res[2]["EF"], 7)

    def test_float_and_parallel(self):
        # 1 (5d) -> 2 (5d) -> 4 (5d)
        # 1 (5d) -> 3 (2d) -> 4 (5d)
        # Path A: 5+5+5 = 15
        # Path B: 5+2+5 = 12
        # Float on 3 should be 3 days.
        
        data = {
            "activity_id": [1, 2, 3, 4],
            "planned_start": ["2023-01-02"]*4, 
            "planned_finish": ["2023-01-06", "2023-01-06", "2023-01-03", "2023-01-06"], # Durs: 5, 5, 2, 5
            "predecessor_id": ["", "1FS", "1FS", "2FS;3FS"]
        }
        df = pd.DataFrame(data)
        G, _ = build_dag_and_validate(df)
        res = cpm_engine.run_cpm(df, G)
        
        # Check Node 3 Float
        self.assertAlmostEqual(res[3]["total_float_days"], 3)
        self.assertFalse(res[3]["on_critical_path"])
        
        # Check Critical
        self.assertTrue(res[2]["on_critical_path"])

class TestCalendarLogic(unittest.TestCase):
    
    def test_weekend_start_adjustment(self):
        # Start on Saturday (2023-01-07). Should roll to Monday (2023-01-09).
        cpm_res = {1: {"ES": 0, "EF": 5, "LS": 0, "LF": 5}} # 5 day duration
        durs = {1: 5}
        p_start = "2023-01-07" # Sat
        
        res = cpm_engine.convert_offsets_to_dates(cpm_res, p_start, durs)
        
        # ES Date should be Monday 2023-01-09 (Start offset 0 from adjusted anchor)
        self.assertEqual(res[1]["ES_date"], "2023-01-09")
        
        # EF Date: 5 days. Mon(1), Tue(2), Wed(3), Thu(4), Fri(5). Finish Fri Jan 13.
        # Offset 5-1 = 4. 9+4=13.
        self.assertEqual(res[1]["EF_date"], "2023-01-13")

    def test_crossing_weekend(self):
        # Start Friday (2023-01-06). Duration 2.
        # Fri (1), Sat(X), Sun(X), Mon (2). Finish Mon Jan 09.
        cpm_res = {1: {"ES": 0, "EF": 2, "LS": 0, "LF": 2}}
        durs = {1: 2}
        p_start = "2023-01-06" # Fri
        
        res = cpm_engine.convert_offsets_to_dates(cpm_res, p_start, durs)
        
        self.assertEqual(res[1]["ES_date"], "2023-01-06")
        self.assertEqual(res[1]["EF_date"], "2023-01-09") # Finish Mon

    def test_milestone(self):
        # Start Mon Jan 02. Duration 0.
        cpm_res = {1: {"ES": 0, "EF": 0, "LS": 0, "LF": 0}}
        durs = {1: 0}
        p_start = "2023-01-02"
        
        res = cpm_engine.convert_offsets_to_dates(cpm_res, p_start, durs)
        
        self.assertEqual(res[1]["ES_date"], "2023-01-02")
        self.assertEqual(res[1]["EF_date"], "2023-01-02")

if __name__ == '__main__':
    unittest.main()
