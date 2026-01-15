import unittest
import pandas as pd
import numpy as np
import recovery_engine

class TestRecovery(unittest.TestCase):

    def setUp(self):
        # Base Schedule
        self.df = pd.DataFrame([
            {
                "activity_id": "A",
                "activity_name": "Task A",
                "on_critical_path": True,
                "remaining_duration_days": 10,
                "planned_duration": 10,
                "resource_id": "R1",
                "fte_allocation": 1.0,
                "predecessors": "B",
                "planned_start": "2024-01-01",
                "planned_finish": "2024-01-10",
                "actual_finish": np.nan,
                "project_name": "Project Alpha"
            }
        ])
        
        # Candidate Resources
        self.df_resource = pd.DataFrame([
            { # Current Resource
                "resource_id": "R1",
                "resource_name": "Alice",
                "resource_rate": 100.0,
                "resource_skills": "Python; SQL"
            },
            { # Perfect Candidate
                "resource_id": "R2", # Cheaper, Matches Skills, Free
                "resource_name": "Bob",
                "resource_rate": 50.0,
                "resource_skills": "Python, SQL" # Comma sep
            },
            { # Busy Candidate
                "resource_id": "R3",
                "resource_name": "Charlie",
                "resource_rate": 50.0,
                "resource_skills": "Python",
            },
            { # Expensive Candidate
                "resource_id": "R4",
                "resource_name": "Dave",
                "resource_rate": 150.0,
                "resource_skills": "Python",
            }
        ])
        
        self.root_causes = pd.DataFrame([
            {
                "Activity": "A",
                "Root Cause Category": "Critical Path Slippage",
                "Impact Days": 5
            }
        ])
        
    def test_init(self):
        ws = recovery_engine.init_recovery_workspace(self.df)
        self.assertIn("last_change_type", ws.columns)
        self.assertIsNot(ws, self.df) # Deep copy

    def test_res_swap_logic(self):
        # A is critical, R1 costs 100
        # R2 costs 50, matches skills. Should appear.
        
        # Logic requires df_schedule for availability check of candidates
        # Create a schedule where R3 is busy
        schedule_with_busy_r3 = self.df.copy()
        # Add a task for R3 that overlaps
        busy_task = {
            "activity_id": "B",
            "resource_id": "R3",
            "planned_start": "2024-01-05", # Overlaps 01-10
            "planned_finish": "2024-01-06"
        }
        schedule_with_busy_r3 = pd.concat([schedule_with_busy_r3, pd.DataFrame([busy_task])], ignore_index=True)

        actions = recovery_engine.generate_actions(schedule_with_busy_r3, {}, self.df_resource, self.root_causes)
        
        # Filter for swaps on A
        swaps = [a for a in actions if a['type'] == recovery_engine.ACTION_RES_SWAP and a['activity_id'] == "A"]
        
        # Expect R2 to be suggested
        r2_swap = next((s for s in swaps if s['parameters']['new_res'] == "R2"), None)
        self.assertIsNotNone(r2_swap, "R2 should be suggested")
        
        # Verify Metadata Quality (No Nones or Zeros)
        params = r2_swap['parameters']
        self.assertIsNotNone(params.get('old_name'), "Old Name should not be None")
        self.assertNotEqual(params.get('old_rate'), 0, "Old Rate should not be 0")
        self.assertIsNotNone(params.get('new_name'), "New Name should not be None")
        self.assertGreater(params.get('savings', 0), 0, "Savings should be positive")
        self.assertIsNotNone(params.get('match_pct'), "Match % should not be None")
        self.assertIsNotNone(r2_swap.get('narrative'), "Action should have a story/narrative")

    def test_fte_logic(self):
        # Setup: Task A is critical, FTE 0.5 (in Setup it is 1.0, let's override)
        self.df.loc[0, "fte_allocation"] = 0.5
        # Resource R1 Max FTE is default 1.0? 
        # We need to add 'resource_max_fte' to df_resource mock
        self.df_resource["resource_max_fte"] = 1.0 # Add column
        
        actions = recovery_engine.generate_actions(self.df, {}, self.df_resource, self.root_causes)
        
        fte_action = next((a for a in actions if a['type'] == recovery_engine.ACTION_FTE_ADJ), None)
        self.assertIsNotNone(fte_action)
        
        # Check calcs
        # Old FTE 0.5 -> New 1.0. Duration 10 -> 5. Saved 5.
        self.assertEqual(fte_action['parameters']['new_fte'], 1.0)
        self.assertIn("Save 5.0 Days", fte_action['description'])
        self.assertIn("Project: Project Alpha", fte_action['description'])
        self.assertIsNotNone(fte_action.get('id'))
        self.assertEqual(fte_action.get('resource_name'), "Alice")

    def test_compression_logic(self):
        # Trigger: Critical Path Slippage
        # Activity A is Critical, Duration 10.
        # Max compress 20% = 2 days.
        
        # Ensure Trigger Category matches
        # We need to simulate the 'category' logic loop in generate_actions
        # generate_actions iterates ALL tasks? No, it looks at Root Causes.
        
        # Root Cause is "Critical Path Slippage" for Activity A.
        actions = recovery_engine.generate_actions(self.df, {}, self.df_resource, self.root_causes)
        
        comp_action = next((a for a in actions if a['type'] == recovery_engine.ACTION_COMPRESS), None)
        self.assertIsNotNone(comp_action)
        self.assertEqual(comp_action['parameters']['reduce_by_days'], 2)
        self.assertIn("Compress Duration by 2 days", comp_action['description'])

    def test_fast_track_logic(self):
        # Trigger: FS Dependency on Critical Path
        # Task A has predecessor B.
        # We need A to be Critical (True in setUp).
        # We need A to have predecessors.
        
        actions = recovery_engine.generate_actions(self.df, {}, self.df_resource, self.root_causes)
        
        ft_action = next((a for a in actions if a['type'] == recovery_engine.ACTION_FAST_TRACK), None)
        self.assertIsNotNone(ft_action)
        self.assertIn("Convert Predecessors to SS + Lag", ft_action['description'])
        
    def test_no_results_scenario(self):
        # Scenario where NO actions should be generated
        # 1. Non-critical task (No FTE/Compress/FastTrack)
        # 2. No Skill Match (No Swap)
        
        safe_df = self.df.copy()
        safe_df['on_critical_path'] = False # Disables FTE, Compress, FastTrack
        
        # Mismatch skills for Swap
        # Current R1 has Python. Candidate R2 has Java.
        mismatch_resources = self.df_resource.copy()
        mismatch_resources.loc[1, 'resource_skills'] = "Java" # Bob now Java only
        
        # Safe Root Causes (No Slip)
        safe_rc = pd.DataFrame()
        
        actions = recovery_engine.generate_actions(safe_df, {}, mismatch_resources, safe_rc)
        self.assertEqual(len(actions), 0, "Expect no actions for safe/mismatched scenario")

if __name__ == '__main__':
    unittest.main()
