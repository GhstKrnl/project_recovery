import pandas as pd
import dateutil.parser

# --- Constants ---

REQUIRED_COLUMNS_SCHEDULE = [
    "portfolio_name",
    "project_id",
    "project_name",
    "project_description",
    "activity_id",
    "activity_name",
    "activity_type",
    "planned_start",
    "planned_finish",
    "planned_duration",
    "percent_complete",
    "predecessor_id",
    "resource_id",
    "fte_allocation",
]

REQUIRED_COLUMNS_RESOURCE = [
    "resource_id",
    "resource_rate",
    "resource_max_fte",
    "resource_start_date",
    "resource_working_hours",
]

# --- Validation Functions ---

def validate_columns(df, required_columns, filename):
    """
    Checks if all required columns are present in the dataframe.
    Returns a list of error strings.
    """
    errors = []
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        errors.append(f"{filename}: Missing required columns: {', '.join(missing)}")
    return errors

def validate_iso_dates(df, date_cols, filename):
    """
    Checks if specified columns contain valid ISO dates.
    Returns a list of error strings.
    """
    errors = []
    for col in date_cols:
        if col in df.columns:
            # Drop nulls for validation
            non_null_values = df[col].dropna()
            for idx, val in non_null_values.items():
                try:
                    # Attempt to parse as ISO
                    # We treat specific formats as strictly ISO for this MVP or use generic parser
                    # User asked for "Valid ISO format"
                    pd.to_datetime(val, format='ISO8601')
                except (ValueError, TypeError):
                     # Fallback to dateutil for more robust check if pandas fails strict ISO
                     try:
                         dateutil.parser.isoparse(str(val))
                     except Exception:
                        errors.append(f"{filename} (Row {idx+2}): Invalid ISO date in '{col}': '{val}'")
    
    # Cap errors to avoid flooding UI
    if len(errors) > 10:
        errors = errors[:10] + [f"... and {len(errors)-10} more date errors."]
    return errors

def validate_numeric(df, num_cols, filename):
    """
    Checks if specified columns are numeric.
    Returns a list of errors.
    """
    errors = []
    for col in num_cols:
        if col in df.columns:
            # Coerce to numeric, find NaNs that weren't NaNs before (meaning parse failed)
            # But wait, input is CSV, so everything is object/string initially or int/float.
            # We want to ensure they ARE numbers.
            
            non_null_values = df[col].dropna()
            # If pandas read_csv already inferred it as float/int, we are good.
            # If it's object, we try to convert.
            
            if not pd.api.types.is_numeric_dtype(non_null_values):
                 # It's an object/string column, try to coerce
                 for idx, val in non_null_values.items():
                     try:
                         float(val)
                     except ValueError:
                         errors.append(f"{filename} (Row {idx+2}): Non-numeric value in '{col}': '{val}'")
    
    if len(errors) > 10:
        errors = errors[:10] + [f"... and {len(errors)-10} more numeric errors."]
    return errors
