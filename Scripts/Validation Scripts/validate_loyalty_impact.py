import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f)

print(f"Total rows (should be <= 4): {len(df)}")
print()
print(df.to_string(index=False))
print()

# Cross-check against sales_trends grand totals -- if these don't match,
# something got dropped or double-counted in the grouping.
total_revenue = df["total_revenue"].sum()
total_orders = df["order_count"].sum()
print(f"Sum of total_revenue across all groups: {total_revenue:,.2f}")
print(f"  (should match sales_trends grand total: 10,018,999.82)")
print(f"Sum of order_count across all groups: {total_orders:,}")
print(f"  (should match sales_trends grand total: 131,328)")
print()

revenue_match = abs(total_revenue - 10018999.82) < 1.0
orders_match = total_orders == 131328
print(f"Revenue reconciliation: {'PASS' if revenue_match else 'FAIL -- investigate'}")
print(f"Order count reconciliation: {'PASS' if orders_match else 'FAIL -- investigate'}")
print()

# avg_order_value sanity
df["recomputed_avg"] = df["total_revenue"] / df["order_count"]
mismatch = df[(df["avg_order_value"] - df["recomputed_avg"]).abs() > 0.01]
print(f"Rows where avg_order_value doesn't match recomputation (should be 0): {len(mismatch)}")
print()

# distinct_customers sanity for the flagged group -- should be small (<=19)
flagged_rows = df[df["is_likely_non_individual"] == True]
if len(flagged_rows) > 0:
    print(f"distinct_customers among is_likely_non_individual=True rows (should be <=19 total): "
          f"{flagged_rows['distinct_customers'].sum()}")
