import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f)

print(f"Total rows (distinct dates): {len(df)}")
print(f"order_date range: {df['order_date'].min()} to {df['order_date'].max()}")
print()

# The big one: does date_dim actually cover the order data's date range?
null_calendar_attrs = df[df["year"].isna()]
print(f"Rows with NULL year (i.e. order_date not found in date_dim): {len(null_calendar_attrs)} of {len(df)}")
if len(null_calendar_attrs) > 0:
    print("  date_dim does NOT cover this range -- sample missing dates:")
    print(f"  {sorted(null_calendar_attrs['order_date'].unique())[:10]}")
print()

print(f"total_revenue: min={df['total_revenue'].min()}, negative values={len(df[df['total_revenue'] < 0])} (should be 0)")
print(f"order_count: min={df['order_count'].min()}, zero/negative values={len(df[df['order_count'] <= 0])} (should be 0)")
print()

# Recompute avg_order_value independently and compare
df["recomputed_avg"] = df["total_revenue"] / df["order_count"]
mismatch = df[(df["avg_order_value"] - df["recomputed_avg"]).abs() > 0.01]
print(f"Rows where avg_order_value doesn't match total_revenue/order_count (should be 0): {len(mismatch)}")
print()

print(f"Grand total revenue across all days: {df['total_revenue'].sum():,.2f}")
print(f"Grand total orders across all days: {df['order_count'].sum():,}")
print("(cross-check these two figures against your own records/expectations if you have them)")
