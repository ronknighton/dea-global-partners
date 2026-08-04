import pandas as pd
import sys

order_items_path = sys.argv[1]
order_item_options_path = sys.argv[2]

oi = pd.read_parquet(order_items_path, columns=["item_price", "item_quantity"])
oio = pd.read_parquet(order_item_options_path, columns=["option_price", "option_quantity"])

print("=== order_items.item_price ===")
print(f"min={oi['item_price'].min()}, max={oi['item_price'].max()}")
print(f"negative values: {len(oi[oi['item_price'] < 0])}")
print(f"zero values: {len(oi[oi['item_price'] == 0])}")
print()

print("=== order_item_options.option_price ===")
print(f"min={oio['option_price'].min()}, max={oio['option_price'].max()}")
print(f"negative values: {len(oio[oio['option_price'] < 0])}")
print(f"zero values: {len(oio[oio['option_price'] == 0])}")
