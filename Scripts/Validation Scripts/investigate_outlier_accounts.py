import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f)

top_ids = [
    "5f1b00e5535ee93e0cb768e7", "5ece77fe902ad501337b23fd", "609d681462e498b356e72a6d",
    "5eac88d6902ad598127b240b", "6282b576c000c6237277bbbd", "5f3c1860505ee9de2a7b23f0",
    "5fcc79d04f5ee9f812be49c2", "5ecf54cf4f5ee92f387b23f3", "5ecf9eda505ee9682b445912",
    "5eab1a0c4f5ee9d77b7b23f3",
]

subset = df[df["user_id"].isin(top_ids)]

print("Distinct restaurant_id per top account (1 = always same restaurant, many = spread across locations):")
print(subset.groupby("user_id")["restaurant_id"].nunique())
print()

print("Distinct app_name per top account:")
print(subset.groupby("user_id")["app_name"].nunique())
print()

print("Distinct printed_card_number per top account (many distinct cards under one user_id would be suspicious):")
print(subset.groupby("user_id")["printed_card_number"].nunique())
print()

print("Date range (first/last order) per top account:")
subset_dates = subset.copy()
subset_dates["order_date"] = pd.to_datetime(subset_dates["creation_time_utc"]).dt.date
print(subset_dates.groupby("user_id")["order_date"].agg(["min", "max", "nunique"]))
