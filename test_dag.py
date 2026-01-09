import unittest
import pandas as pd
import networkx as nx
from dag_engine import parse_dependency_string, build_dag_and_validate

class TestDagEngine(unittest.TestCase):

    def test_parse_simple(self):
        # "3FS"
        res = parse_dependency_string("3FS")
        self.assertEqual(res, [{"predecessor_id": 3, "type": "FS", "lag": 0}])

    def test_parse_complex(self):
        # "3FS;2SS+1d;5FF-2d"
        res = parse_dependency_string("3FS;2SS+1d;5FF-2d")
        expected = [
            {"predecessor_id": 3, "type": "FS", "lag": 0},
            {"predecessor_id": 2, "type": "SS", "lag": 1},
            {"predecessor_id": 5, "type": "FF", "lag": -2},
        ]
        # Sort by id to compare regardless of order if list implementation changes
        # But parse preserves order, so direct compare is fine
        self.assertEqual(res, expected)

    def test_invalid_syntax(self):
        with self.assertRaises(ValueError):
            parse_dependency_string("3XX") # Invalid type
        with self.assertRaises(ValueError):
            parse_dependency_string("3FS+kd") # Invalid lag

    def test_dag_cycles_and_errors(self):
        data = {
            "activity_id": [1, 2, 3],
            "activity_name": ["A", "B", "C"],
            "predecessor_id": [
                "",          # 1: No preds
                "1FS",       # 2: Depends on 1
                "2FS;1FS"    # 3: Depends on 2 and 1
            ]
        }
        df = pd.DataFrame(data)
        G, val = build_dag_and_validate(df)
        self.assertEqual(val[1], "OK")
        self.assertEqual(val[2], "OK")
        self.assertEqual(val[3], "OK")
        self.assertTrue(nx.is_directed_acyclic_graph(G))

    def test_cycle_detection(self):
        # 1->2, 2->1
        data = {
            "activity_id": [1, 2],
            "predecessor_id": ["2FS", "1FS"]
        }
        df = pd.DataFrame(data)
        G, val = build_dag_and_validate(df)
        
        self.assertIn("ERROR: Cycle detected", val[1])
        self.assertIn("ERROR: Cycle detected", val[2])

    def test_self_dependency(self):
        data = {
            "activity_id": [1],
            "predecessor_id": ["1FS"]
        }
        df = pd.DataFrame(data)
        G, val = build_dag_and_validate(df)
        self.assertIn("ERROR: Self-dependency", val[1])

    def test_missing_ref(self):
        data = {
            "activity_id": [1],
            "predecessor_id": ["99FS"]
        }
        df = pd.DataFrame(data)
        G, val = build_dag_and_validate(df)
        self.assertIn("ERROR: Missing predecessor ID 99", val[1])

if __name__ == '__main__':
    unittest.main()
