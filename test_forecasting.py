import unittest
import pandas as pd
import networkx as nx
import forecasting_engine

class TestForecasting(unittest.TestCase):

    def test_working_days(self):
        # Mon to Tue (inclusive) = 2
        d = forecasting_engine.count_working_days("2023-01-02", "2023-01-03", inclusive=True)
        self.assertEqual(d, 2)
        
        # Fri to Mon (inclusive) = 2 (Fri, Mon)
        d2 = forecasting_engine.count_working_days("2023-01-06", "2023-01-09", inclusive=True)
        self.assertEqual(d2, 2)

    def test_delay_metrics(self):
        # Baseline: Mon Jan 02
        # Target: Wed Jan 04
        # Delay: 2 days
        d = forecasting_engine.calculate_delay_metric_days("2023-01-04", "2023-01-02")
        self.assertEqual(d, 2)

    def test_forecast_logic_complete_task(self):
        # Task 1: Complete.
        # Baseline: 10th. Actual: 12th.
        # Delay = 2.
        
        df = pd.DataFrame([{
            "activity_id": 1,
            "actual_start": "2023-01-01",
            "actual_finish": "2023-01-12",
            "baseline_1_start": "2023-01-01",
            "baseline_1_finish": "2023-01-10",
            "ES_date": "2023-01-01",
            "EF_date": "2023-01-10",
            "planned_duration": 10
        }])
        G = nx.DiGraph()
        G.add_node(1)
        
        res = forecasting_engine.calculate_forecasts(df, G)
        r1 = res[1]
        
        self.assertEqual(r1["percent_complete"], 100)
        self.assertEqual(r1["forecast_finish_date"], "2023-01-12")
        self.assertEqual(r1["total_schedule_delay"], 2) # 12 - 10 = 2
        self.assertEqual(r1["task_created_delay"], 2) # No preds, so all created
        self.assertEqual(r1["delay_carried_in"], 0)

    def test_delay_inheritance(self):
        # Task 1 (Pred) -> Late by 2 days.
        # Task 2 (Succ) -> Baseline Start follows Task 1.
        # Task 2 Actuals: None (Not started).
        # Task 2 Forecast: Should allow checking 'Carried In'.
        
        # Pred: BL Fin=10th. Act Fin=12th. (Variance 2)
        # Succ: CPM ES=13th (Assuming driven by Pred Act?). 
        # Wait, CPM uses Planned. If Planned inputs are updated, CPM is updated.
        # But here we are testing isolate logic.
        
        df = pd.DataFrame([
            {
                "activity_id": 1,
                "actual_finish": "2023-01-12",
                "baseline_1_finish": "2023-01-10"
            },
            {
                "activity_id": 2,
                "actual_finish": None,
                "baseline_1_finish": "2023-01-15",
                # Let's say CPM shows it starting late?
                "ES_date": "2023-01-13", 
                "EF_date": "2023-01-17"
            }
        ])
        G = nx.DiGraph()
        G.add_edge(1, 2)
        
        res = forecasting_engine.calculate_forecasts(df, G)
        r2 = res[2]
        
        # Carried In should be 2 (from Pred)
        self.assertEqual(r2["delay_carried_in"], 2)
        
        # Total Delay?
        # Forecast Fin (CPM EF) = 17th. Baseline Fin = 15th.
        # Variance = 2.
        self.assertEqual(r2["total_schedule_delay"], 2)
        
        # Task Created = Max(0, Total - Carried) = Max(0, 2 - 2) = 0.
        self.assertEqual(r2["task_created_delay"], 0)
        
        # Absorbed = Carried - Created = 2 - 0 = 2.
        self.assertEqual(r2["delay_absorbed"], 2)

if __name__ == '__main__':
    unittest.main()
