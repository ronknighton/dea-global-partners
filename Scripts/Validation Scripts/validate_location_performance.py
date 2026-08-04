import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f)

print(f"Total distinct restaurant_id rows: {len(df)}")
print()

total_revenue = df["total_revenue"].sum()
total_orders = df["order_count"].sum()
print(f"Sum of total_revenue across all locations: {total_revenue:,.2f}")
print(f"  (should match sales_trends grand total: 10,018,999.82)")
print(f"Sum of order_count across all locations: {total_orders:,}")
print(f"  (should match sales_trends grand total: 131,328)")

revenue_match = abs(total_revenue - 10018999.82) < 1.0
orders_match = total_orders == 131328
print(f"Revenue reconciliation: {'PASS' if revenue_match else 'FAIL -- investigate'}")
print(f"Order count reconciliation: {'PASS' if orders_match else 'FAIL -- investigate'}")
print()

df["recomputed_avg"] = df["total_revenue"] / df["order_count"]
mismatch = df[(df["avg_order_value"] - df["recomputed_avg"]).abs() > 0.01]
print(f"Rows where avg_order_value doesn't match recomputation (should be 0): {len(mismatch)}")
print()

print(f"total_revenue: min={df['total_revenue'].min():.2f}, negative={len(df[df['total_revenue'] < 0])} (should be 0)")
print(f"order_count: min={df['order_count'].min()}, zero/negative={len(df[df['order_count'] <= 0])} (should be 0)")
print()

# The specific known accounts: 609d681462e498b356e72a6d (2 restaurant_ids)
# and 6282b576c000c6237277bbbd (1 restaurant_id) should produce visible
# flagged_revenue_share on whichever locations they're tied to.
print("Top 10 locations by flagged_revenue_share (should be non-trivial for at least a couple):")
print(df.sort_values("flagged_revenue_share", ascending=False)
      [["restaurant_id", "total_revenue", "flagged_account_revenue", "flagged_revenue_share", "flagged_account_order_count"]]
      .head(10).to_string(index=False))
