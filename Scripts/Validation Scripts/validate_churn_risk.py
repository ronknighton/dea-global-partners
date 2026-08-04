import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f)

print(f"Total rows: {len(df)}")
print(f"Distinct user_id: {df['user_id'].nunique()}  (should equal total rows above)")
print(f"Should also equal customer_segments_rfm's row count (20174) since both are built off the same customer base.")
print()

print("risk_tier distribution:")
print(df["risk_tier"].value_counts())
print()

print("is_overdue distribution:")
print(df["is_overdue"].value_counts())
print()

# Sanity: risk_tier should align directionally with recency_days
print("Median recency_days per risk_tier (should increase Active -> At Risk -> Churned):")
print(df.groupby("risk_tier")["recency_days"].median().reindex(["Active", "At Risk", "Churned"]))
print()

# avg_days_between_orders should be null only for customers with frequency == 1
one_timers = df[df["frequency"] == 1]
bad_one_timers = one_timers[one_timers["avg_days_between_orders"].notna()]
print(f"frequency==1 customers with a non-null avg_days_between_orders (should be 0): {len(bad_one_timers)}")

repeat_customers = df[df["frequency"] > 1]
bad_repeat = repeat_customers[repeat_customers["avg_days_between_orders"].isna()]
print(f"frequency>1 customers with a null avg_days_between_orders (should be 0): {len(bad_repeat)}")
print()

# is_overdue should never be True when avg_days_between_orders is null
bad_overdue = df[(df["avg_days_between_orders"].isna()) & (df["is_overdue"] == True)]
print(f"is_overdue=True rows with a null avg_days_between_orders (should be 0): {len(bad_overdue)}")
print()

# is_overdue should logically only be True when recency_days > 2x avg gap
df_check = df[df["avg_days_between_orders"].notna()].copy()
df_check["expected_overdue"] = df_check["recency_days"] > (2 * df_check["avg_days_between_orders"])
mismatch = df_check[df_check["expected_overdue"] != df_check["is_overdue"]]
print(f"Rows where is_overdue doesn't match recomputed expectation (should be 0): {len(mismatch)}")
