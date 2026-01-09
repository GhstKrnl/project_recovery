import re
import networkx as nx
import pandas as pd

# Regex for parsing: <ID><TYPE><OPTIONAL LAG>
# Group 1: ID (digits)
# Group 2: Type (FS, SS, FF, SF)
# Group 3: Lag (optional, signed integer + 'd')
DEPENDENCY_REGEX = re.compile(r"^(?P<id>\d+)(?P<type>FS|SS|FF|SF)(?P<lag>[+-]?\d+d)?$")

def parse_dependency_string(dep_str):
    """
    Parses a single dependency string like '3FS', '5SS+2d', '7FF-1d'.
    Returns a dict or raises ValueError.
    """
    if not isinstance(dep_str, str) or not dep_str.strip():
        return []

    deps = []
    # Split by semicolon
    parts = [p.strip() for p in dep_str.split(";") if p.strip()]

    for part in parts:
        match = DEPENDENCY_REGEX.match(part)
        if not match:
            # Malformed dependency string
            raise ValueError(f"Malformed dependency: '{part}'")
        
        data = match.groupdict()
        pred_id = int(data["id"])
        dep_type = data["type"]
        
        # Parse lag
        lag_str = data["lag"]
        lag_days = 0
        if lag_str:
            # Remove 'd' and parse int
            try:
                lag_days = int(lag_str.replace('d', ''))
            except ValueError:
                 raise ValueError(f"Invalid lag format: '{lag_str}'")

        deps.append({
            "predecessor_id": pred_id,
            "type": dep_type,
            "lag": lag_days
        })
    return deps

def build_dag_and_validate(df):
    """
    Builds a NetworkX DiGraph from the dataframe.
    Validates:
    - Syntax
    - Self-loops
    - Missing references
    - Cycles
    
    Returns:
    - G: The NetworkX graph
    - validation_results: Dict mapping activity_id -> status string ("OK" or "ERROR: ...")
    """
    G = nx.DiGraph()
    validation_results = {}
    
    # 1. Add all nodes first to ensure we know what exists
    # Activity IDs can be int or string in CSV, force int for consistency if numeric
    # The requirement said activity_id are 1, 2, etc. so assume int.
    
    # helper map: id -> exists
    valid_ids = set()
    
    # First pass: Add nodes
    for _, row in df.iterrows():
        try:
            act_id = int(row["activity_id"])
            valid_ids.add(act_id)
            G.add_node(act_id, label=row.get("activity_name", str(act_id)))
        except (ValueError, TypeError):
             # If ID is invalid, we can't really do much with it in the graph
             continue

    # Second pass: Add edges and validate
    for _, row in df.iterrows():
        try:
            act_id = int(row["activity_id"])
        except ValueError:
            validation_results["UNKNOWN"] = "ERROR: Invalid Activity ID"
            continue
            
        status = "OK"
        preds_str = row.get("predecessor_id")
        
        # Handling NaN/None
        if pd.isna(preds_str) or str(preds_str).strip() == "":
            validation_results[act_id] = "OK"
            continue
            
        try:
            parsed_deps = parse_dependency_string(str(preds_str))
            
            for dep in parsed_deps:
                pred_id = dep["predecessor_id"]
                
                # Check 1: Self-dependency
                if pred_id == act_id:
                    status = f"ERROR: Self-dependency on {pred_id}"
                    break
                
                # Check 2: Missing reference
                if pred_id not in valid_ids:
                    status = f"ERROR: Missing predecessor ID {pred_id}"
                    break
                
                # Add edge
                G.add_edge(pred_id, act_id, type=dep["type"], lag=dep["lag"])
                
        except ValueError as e:
            status = f"ERROR: {str(e)}"
        
        validation_results[act_id] = status

    # Check 3: Cycles
    # NetworkX simple_cycles is computationally expensive for large graphs, 
    # but efficient enough for typical project schedules (< 10k nodes).
    # However, valid DAGs have no cycles. finding_cycle raises error if found.
    try:
        cycles = list(nx.simple_cycles(G))
        if cycles:
            # Mark nodes involved in cycles
            for cycle in cycles:
                cycle_str = "->".join(map(str, cycle))
                for node in cycle:
                    # Append error if not already erred (or overwrite to show critical cycle error)
                    current_status = validation_results.get(node, "OK")
                    if current_status == "OK":
                        validation_results[node] = f"ERROR: Cycle detected ({cycle_str})"
                    else:
                        validation_results[node] += f"; Cycle detected"
    except Exception as e:
        # Fallback if graph is huge or something fails
        pass

    return G, validation_results
