import pandas as pd
import glob
import re
import sys

# Point this at wherever you've downloaded the raw/order_items parquet file(s) locally
files = glob.glob(sys.argv[1] if len(sys.argv) > 1 else "*.parquet")

pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$')

for f in files:
    # Read the whole file first so we can find the column regardless of casing
    full = pd.read_parquet(f)
    col_name = None
    for candidate in ["CREATION_TIME_UTC", "creation_time_utc"]:
        if candidate in full.columns:
            col_name = candidate
            break
    if col_name is None:
        print(f"{f}: no creation_time_utc column found. Columns present: {list(full.columns)}")
        continue

    if not pd.api.types.is_string_dtype(full[col_name]) and not pd.api.types.is_object_dtype(full[col_name]):
        print(f"{f}: WARNING -- '{col_name}' is already a non-string type "
              f"({full[col_name].dtype}). This looks like the PROCESSED file, "
              f"not raw -- failed rows will show as NULL here with no original "
              f"value to inspect. Re-run against the raw/order_items/ file instead.")
        continue

    df = full[[col_name]]

    no_decimal = df[~df[col_name].astype(str).str.contains(r'\.', regex=True, na=False)]
    print(f"  {len(no_decimal)} rows have NO fractional-second decimal point at all "
          f"(e.g. '...T11:03:32Z' with no '.xxx') -- these would fail Spark's "
          f"'.SSS' pattern, which requires a literal decimal point, even though "
          f"they're perfectly valid ISO 8601 timestamps.")
    if len(no_decimal) > 0:
        print("  Sample values with no decimal point:")
        print(no_decimal[col_name].head(10).to_string(index=False))
        print()

    # Use pandas' own datetime parser (lenient about fractional-second
    # precision, same as Spark's SSS pattern turned out to be) rather than
    # a strict regex -- this finds genuine parse failures, not just
    # variable millisecond-digit-count rows that actually parse fine.
    parsed = pd.to_datetime(df[col_name], format="ISO8601", errors="coerce", utc=True)
    non_matching = df[parsed.isna() & df[col_name].notna()]
    also_check_blank_or_null = df[df[col_name].isna() | (df[col_name].astype(str).str.strip() == "")]

    print(f"{f}: {len(df)} total rows")
    print(f"  {len(non_matching)} rows have a non-null value that failed to parse as a timestamp")
    print(f"  {len(also_check_blank_or_null)} rows have a genuinely null/blank source value")
    if len(non_matching) > 0:
        print("  Sample values that failed to parse:")
        print(non_matching[col_name].head(20).to_string(index=False))
        print()