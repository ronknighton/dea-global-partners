import pandas as pd
import sys

f = sys.argv[1]
df = pd.read_parquet(f, columns=["user_id"])

with_user = df[df["user_id"].notna()]
counts = with_user.groupby("user_id").size()

print(f"Total customers: {len(counts)}")
print()
print("Percentile breakdown of line items per customer:")
for pct in [50, 75, 90, 95, 97, 98, 99, 99.5, 99.9, 99.99]:
    print(f"  p{pct}: {counts.quantile(pct/100):.1f}")
print()
print(f"  max: {counts.max()}")
print()

# How many customers, and how many line items, fall above a few candidate thresholds
for threshold in [100, 200, 300, 500]:
    above = counts[counts > threshold]
    print(f"Threshold > {threshold} line items: {len(above)} customers "
          f"({len(above)/len(counts)*100:.3f}% of customers), "
          f"holding {above.sum()} line items ({above.sum()/counts.sum()*100:.2f}% of all line items)")
