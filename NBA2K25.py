import pandas as pd

# player data frames
# format: (name/nickname)_df
ant_df = pd.read_csv("AntMan2k25.csv")




ant_df["season"] = "2025-26"
fgp_idx = ant_df.columns.get_loc("Field Goals Attempted") + 1


ant_df.insert(
    fgp_idx,
    "fg%",
    ant_df["Field Goals Made"] / ant_df["Field Goals Attempted"]
)

# Inspect the data
with pd.option_context(
    "display.max_rows", None,
    "display.max_columns", None
):
    print(ant_df)
