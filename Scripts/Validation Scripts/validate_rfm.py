import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f)

print(f"Total rows: {len(df)}")
print(f"Distinct user_id: {df['user_id'].nunique()}  (should equal total rows above)")
print()

print("Score range check (all should show min=1, max=5, no nulls):")
for col in ["recency_score", "frequency_score", "monetary_score"]:
    print(f"  {col}: min={df[col].min()}, max={df[col].max()}, nulls={df[col].isna().sum()}")
print()

print(f"recency_days: min={df['recency_days'].min()} (should be >= 0), max={df['recency_days'].max()}")
print()

print("Segment distribution:")
print(df["segment"].value_counts())
print()

new_check = df[df["segment"] == "New"]
bad_new = new_check[new_check["frequency"] != 1]
print(f"'New' segment rows where frequency != 1 (should be 0): {len(bad_new)}")
