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
        
        # Test narrative formatting - should be clean single-line string
        narrative = r2_swap.get('narrative')
        self.assertIsInstance(narrative, str, "Narrative should be a string")
        
        # Check that narrative is a single line (no newlines or line breaks)
        self.assertEqual(narrative.count('\n'), 0, "Narrative should be a single line")
        
        # Check that markdown formatting is present (will be rendered by st.markdown)
        self.assertIn("**Trigger:**", narrative, "Narrative should start with Trigger")
        self.assertIn("**", narrative, "Narrative should contain markdown bold markers")
        
        # Check that dollar amounts are formatted correctly (with .1f precision)
        import re
        # Should match pattern like $50.0/hr or $100.0/hr
        dollar_pattern = r'\$\d+\.\d+/hr'
        self.assertRegex(narrative, dollar_pattern, "Narrative should contain properly formatted dollar amounts")
        
        # Check that resource names appear (without bold formatting)
        self.assertIn(params.get('old_name'), narrative, "Old resource name should appear in narrative")
        self.assertIn(params.get('new_name'), narrative, "New resource name should appear in narrative")
        # Resource names should NOT be bolded
        self.assertNotIn(f"**{params.get('old_name')}**", narrative, "Old resource name should not be bolded")
        self.assertNotIn(f"**{params.get('new_name')}**", narrative, "New resource name should not be bolded")
        
        # Check that skills match percentage is included
        self.assertIn(f"Skills match: {params.get('match_pct')}%", narrative, "Narrative should include skills match percentage")
        
        # Check that availability verification is mentioned
        self.assertIn("Availability verified", narrative, "Narrative should mention availability verification")
        self.assertIn("preventing overallocation", narrative, "Narrative should mention preventing overallocation")

    def test_fte_logic(self):
        # Setup: Task A is critical, FTE 0.5 (in Setup it is 1.0, let's override)
        self.df.loc[0, "fte_allocation"] = 0.5
        # Resource R1 Max FTE is default 1.0? 
        # We need to add 'resource_max_fte' to df_resource mock
        self.df_resource["resource_max_fte"] = 1.0 # Add column
        # Add work_hours for cost calculation
        self.df_resource["resource_working_hours"] = 8.0
        
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
    
    def test_fte_cost_calculation(self):
        """
        Test that FTE adjustment cost calculation is correct.
        When FTE increases and duration decreases proportionally, 
        total hours (and cost) should remain the same.
        
        Example:
        - Old: 10 days * 0.5 FTE = 5 person-days = 40 hours (at 8h/day)
        - New: 5 days * 1.0 FTE = 5 person-days = 40 hours (at 8h/day)
        - Cost should be the same: 40 hours * rate
        """
        import cost_engine
        
        # Setup: Task with specific FTE and duration
        self.df.loc[0, "fte_allocation"] = 0.5
        self.df.loc[0, "remaining_duration_days"] = 10.0
        self.df_resource["resource_max_fte"] = 1.0
        self.df_resource["resource_working_hours"] = 8.0
        self.df_resource["resource_rate"] = 100.0  # $100/hour
        
        # Generate FTE adjustment action
        actions = recovery_engine.generate_actions(self.df, {}, self.df_resource, self.root_causes)
        fte_action = next((a for a in actions if a['type'] == recovery_engine.ACTION_FTE_ADJ), None)
        
        self.assertIsNotNone(fte_action, "Should have FTE adjustment action")
        
        params = fte_action['parameters']
        old_fte = params['old_fte']
        new_fte = params['new_fte']
        old_dur = params['old_dur']
        new_dur = params['new_dur']
        cost_impact = params.get('cost_impact', 0)
        
        # Verify FTE and duration changes
        self.assertEqual(old_fte, 0.5, "Old FTE should be 0.5")
        self.assertEqual(new_fte, 1.0, "New FTE should be 1.0")
        self.assertEqual(old_dur, 10.0, "Old duration should be 10 days")
        self.assertEqual(new_dur, 5.0, "New duration should be 5 days (10 * 0.5/1.0)")
        
        # Calculate expected costs manually
        rate = 100.0
        work_hours = 8.0
        
        # Old remaining cost = duration * work_hours * fte * rate
        old_cost = old_dur * work_hours * old_fte * rate
        # New remaining cost = duration * work_hours * fte * rate
        new_cost = new_dur * work_hours * new_fte * rate
        
        expected_old_cost = 10.0 * 8.0 * 0.5 * 100.0  # = 4000
        expected_new_cost = 5.0 * 8.0 * 1.0 * 100.0   # = 4000
        
        self.assertEqual(old_cost, expected_old_cost, f"Old cost should be {expected_old_cost}")
        self.assertEqual(new_cost, expected_new_cost, f"New cost should be {expected_new_cost}")
        self.assertEqual(old_cost, new_cost, "Cost should remain the same (total hours unchanged)")
        
        # Verify cost_impact in action parameters
        expected_cost_diff = new_cost - old_cost
        self.assertAlmostEqual(cost_impact, expected_cost_diff, places=2, 
                              msg=f"Cost impact should be {expected_cost_diff} (should be 0 or very close)")
        
        # Print calculation details for verification
        print(f"\nFTE Cost Calculation Test:")
        print(f"  Old: {old_dur} days * {work_hours} hrs/day * {old_fte} FTE * ${rate}/hr = ${old_cost:,.2f}")
        print(f"  New: {new_dur} days * {work_hours} hrs/day * {new_fte} FTE * ${rate}/hr = ${new_cost:,.2f}")
        print(f"  Total Hours (Old): {old_dur * old_fte * work_hours} hours")
        print(f"  Total Hours (New): {new_dur * new_fte * work_hours} hours")
        print(f"  Cost Difference: ${cost_impact:,.2f}")
        print(f"  Conclusion: Cost should be same because total hours are unchanged")

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
        
    def test_resource_swap_narrative_formatting(self):
        """
        Test that Resource Swap narratives are properly formatted:
        - Single-line string (no line breaks)
        - Proper markdown formatting
        - Dollar amounts formatted with .1f precision
        - No literal markdown characters that would display incorrectly
        """
        # Setup: Create a scenario with a resource swap
        schedule_with_busy_r3 = self.df.copy()
        busy_task = {
            "activity_id": "B",
            "resource_id": "R3",
            "planned_start": "2024-01-05",
            "planned_finish": "2024-01-06"
        }
        schedule_with_busy_r3 = pd.concat([schedule_with_busy_r3, pd.DataFrame([busy_task])], ignore_index=True)

        actions = recovery_engine.generate_actions(schedule_with_busy_r3, {}, self.df_resource, self.root_causes)
        
        # Get a resource swap action
        swaps = [a for a in actions if a['type'] == recovery_engine.ACTION_RES_SWAP and a['activity_id'] == "A"]
        self.assertGreater(len(swaps), 0, "Should have at least one swap action")
        
        swap_action = swaps[0]
        narrative = swap_action.get('narrative')
        
        # Test 1: Narrative should be a single-line string
        self.assertIsInstance(narrative, str, "Narrative should be a string")
        self.assertEqual(narrative.count('\n'), 0, "Narrative should be a single line (no newlines)")
        
        # Test 2: Should not contain problematic patterns that would display incorrectly
        # Should not have spaces before closing **
        self.assertNotRegex(narrative, r'\*\* \w+ \*\*', "Narrative should not have spaces inside markdown bold markers")
        
        # Test 3: Dollar amounts should be properly formatted (e.g., $50.0/hr not $50/hr or $50.00/hr)
        import re
        dollar_pattern = r'\$\d+\.\d+/hr'
        matches = re.findall(dollar_pattern, narrative)
        self.assertGreater(len(matches), 0, "Narrative should contain properly formatted dollar amounts")
        
        # Test 4: Should contain proper markdown structure
        self.assertIn("**Trigger:**", narrative, "Narrative should start with 'Trigger:' in bold")
        
        # Test 5: Resource names should appear without bold formatting (just plain text)
        params = swap_action['parameters']
        old_name = params.get('old_name')
        new_name = params.get('new_name')
        if old_name:
            # Check that name appears without bold formatting
            self.assertIn(old_name, narrative, f"Old resource name '{old_name}' should appear in narrative")
            # Should NOT have bold markers around the name
            self.assertNotIn(f"**{old_name}**", narrative, f"Old resource name '{old_name}' should not be bolded")
        if new_name:
            self.assertIn(new_name, narrative, f"New resource name '{new_name}' should appear in narrative")
            # Should NOT have bold markers around the name
            self.assertNotIn(f"**{new_name}**", narrative, f"New resource name '{new_name}' should not be bolded")
        
        # Test 6: Skills match percentage should be included
        match_pct = params.get('match_pct')
        if match_pct is not None:
            self.assertIn(f"Skills match: {match_pct}%", narrative, "Narrative should include skills match percentage")
        
        # Test 7: Should not have literal markdown artifacts that would display incorrectly
        # Check that we don't have patterns like "**Name **" (space before closing **)
        problematic_patterns = [
            r'\*\* \w+ \*\*',  # Space before closing **
            r'\(\s*\*\*',      # Space before ** in parentheses
            r'\*\*\s+\)',      # Space after ** before closing paren
        ]
        for pattern in problematic_patterns:
            self.assertNotRegex(narrative, pattern, f"Narrative should not contain problematic pattern: {pattern}")
        
        # Test 8: Verify the narrative format matches expected structure
        # Should be: **Trigger:** Found cheaper resource Name1 ($X.X/hr) vs Name2 ($X.X/hr). Skills match: X%. Availability verified: ...
        expected_structure = r'\*\*Trigger:\*\* Found cheaper resource .+? \(\$.*?/hr\) vs .+? \(\$.*?/hr\)\. Skills match: \d+%\. Availability verified:'
        self.assertRegex(narrative, expected_structure, "Narrative should match expected format structure")
        
        # Test 9: Should mention availability verification
        self.assertIn("Availability verified", narrative, "Narrative should mention availability verification")
        self.assertIn("not assigned to any overlapping activities", narrative, "Narrative should mention no overlapping activities")
        self.assertIn("preventing overallocation", narrative, "Narrative should mention preventing overallocation")
        
        # Print success message (without special Unicode characters for Windows compatibility)
        print(f"\nNarrative format test passed!")
        print(f"  Narrative: {narrative}")

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
