import pandas as pd
import sys

# Usage: python validate_flag_rollout.py clv.parquet rfm.parquet churn.parquet

known_flagged = {
    "5f1b00e5535ee93e0cb768e7", "5ece77fe902ad501337b23fd", "609d681462e498b356e72a6d",
    "5eac88d6902ad598127b240b", "6282b576c000c6237277bbbd", "5f3c1860505ee9de2a7b23f0",
    "5fcc79d04f5ee9f812be49c2", "5ecf54cf4f5ee92f387b23f3", "5ecf9eda505ee9682b445912",
    "5eab1a0c4f5ee9d77b7b23f3",
}

labels = ["clv_daily", "customer_segments_rfm", "churn_risk"]

for label, path in zip(labels, sys.argv[1:4]):
    df = pd.read_parquet(path)
    print(f"=== {label} ===")
    print(f"Total rows: {len(df)}")
    print(f"'is_likely_non_individual' present: {'is_likely_non_individual' in df.columns}")

    if "is_likely_non_individual" in df.columns:
        distinct_customers = df.drop_duplicates("user_id")
        flagged = distinct_customers[distinct_customers["is_likely_non_individual"] == True]
        print(f"Distinct customers: {len(distinct_customers)}")
        print(f"Flagged as is_likely_non_individual: {len(flagged)} (expected: 19)")

        known_present = distinct_customers[distinct_customers["user_id"].isin(known_flagged)]
        all_flagged_correctly = (known_present["is_likely_non_individual"] == True).all()
        print(f"All 10 previously-investigated accounts correctly flagged True: {all_flagged_correctly}")
    print()
