import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f, columns=["user_id", "is_loyalty"])

print(f"Total rows: {len(df)}")

with_user = df[df["user_id"].notna()]
print(f"Rows with a non-null user_id: {len(with_user)}")
print(f"Distinct customers: {with_user['user_id'].nunique()}")
print()

# For each customer, how many distinct is_loyalty values appear across their orders?
flag_counts = with_user.groupby("user_id")["is_loyalty"].nunique()

consistent = (flag_counts == 1).sum()
inconsistent = (flag_counts > 1).sum()

print(f"Customers with a consistent is_loyalty flag across all their orders: {consistent}")
print(f"Customers with an INCONSISTENT is_loyalty flag (both True and False seen): {inconsistent}")
print(f"  ({inconsistent / len(flag_counts) * 100:.2f}% of customers)")
print()

if inconsistent > 0:
    inconsistent_ids = flag_counts[flag_counts > 1].index

    # Compact summary: for each inconsistent customer, just the count of
    # True vs False orders, not every individual row.
    summary = (
        with_user[with_user["user_id"].isin(inconsistent_ids)]
        .groupby(["user_id", "is_loyalty"])
        .size()
        .unstack(fill_value=0)
        .rename(columns={True: "loyalty_true_count", False: "loyalty_false_count"})
    )

    print("Sample of inconsistent customers (order counts per flag value, first 10):")
    print(summary.head(10).to_string())
    print()

    # Overall skew: are inconsistent customers mostly-loyalty, mostly-not,
    # or genuinely mixed? Helps decide how to treat them.
    summary["total"] = summary.get("loyalty_true_count", 0) + summary.get("loyalty_false_count", 0)
    summary["pct_loyalty"] = summary.get("loyalty_true_count", 0) / summary["total"]
    print("Distribution of % of orders flagged loyalty=True, among inconsistent customers:")
    print(summary["pct_loyalty"].describe())

print()
print("=" * 60)
print("Overall order volume concentration check (ALL customers, not just inconsistent ones)")
print("=" * 60)
overall_counts = with_user.groupby("user_id").size().sort_values(ascending=False)
print(f"Total distinct customers: {len(overall_counts)}")
print(f"Median line items per customer: {overall_counts.median()}")
print(f"Mean line items per customer: {overall_counts.mean():.1f}")
print()
print("Top 15 customers by line item volume:")
print(overall_counts.head(15).to_string())
print()
top_10_share = overall_counts.head(10).sum() / overall_counts.sum() * 100
print(f"Share of ALL line items held by just the top 10 customers: {top_10_share:.2f}%")