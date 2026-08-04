import pandas as pd
df = pd.read_parquet(r"C:\Users\ronkn\Dropbox\Data Engineering Academy\Projects\End-to-End Projects\Global Partners E2E\Parquet Files\Marts\clv_daily\part-00000-aebbe1d3-c223-4450-b170-3368f09083ea-c000.snappy.parquet")
df_sorted = df.sort_values(["user_id", "order_date"])
bad = df_sorted.groupby("user_id")["cumulative_clv"].apply(lambda s: (s.diff().dropna() < 0).any())
print(f"Customers with a decreasing cumulative_clv (should be 0): {bad.sum()}")