import pandas as pd
import numpy as np

# Case 1: Int Keys, Float Map
data_dict = {
    1: {"val": "A"},
    2: {"val": "B"}
}
df_res = pd.DataFrame.from_dict(data_dict, orient='index')
print("--- Result DF Index ---")
print(df_res.index) 
# Should be Int64Index

df_main = pd.DataFrame({"id": [1, 2, 3]})
df_main["float_id"] = df_main["id"].astype(float)
print("\n--- Main DF ---")
print(df_main)

# Try Map
df_main["mapped"] = df_main["float_id"].map(df_res["val"])
print("\n--- Mapped (Float -> Int Index) ---")
print(df_main["mapped"])

# Case 2: what if we explicitly cast to match?
df_main["mapped_fixed"] = df_main["float_id"].astype("Int64").map(df_res["val"])
print("\n--- Mapped (Int64 -> Int Index) ---")
print(df_main["mapped_fixed"])
