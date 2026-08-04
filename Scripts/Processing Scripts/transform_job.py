"""
dea-global-partners: Transform job
Reads all raw/ partitions for order_items, order_item_options, and
date_dim, standardizes column names/types, and writes cleaned tables to
processed/. Full rebuild of processed/ from all of raw/ on every run
(overwrite, not append) -- justified at this data volume; see solution
design doc Section 8/11 for the tradeoff vs. partition-level incremental
merge.

Deliberately does NOT join order_items to order_item_options here.
order_item_options is a child table (multiple options per line item);
joining now would fan out order_items' grain silently. The join is
deferred to the aggregate/Athena CTAS layer, same pattern used on the
Healthcare Metrics project.
"""

import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Job setup
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = "dea-global-partners-rhk"


def read_raw(table_name: str):
    """Reads all dt= partitions under raw/<table_name>/ in one shot."""
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"s3://{S3_BUCKET}/raw/{table_name}/"],
            "recurse": True,
        },
        format="parquet",
    )
    return dyf.toDF()


def to_snake_case(df):
    """Lowercases all column names. Source columns are plain uppercase
    (ORDER_ID, CREATION_TIME_UTC, etc.), not camelCase, so a straight
    .lower() is sufficient -- no underscore-insertion logic needed."""
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df


def check_parse_failures(df, column: str, label: str):
    """Prints a warning if parsing a date/timestamp column produced any
    nulls that weren't already null in the source -- catches a format
    assumption that doesn't hold for every row, rather than letting it
    silently disappear."""
    null_count = df.filter(F.col(column).isNull()).count()
    if null_count > 0:
        print(
            f"WARNING: {label} -- {null_count} rows have a null "
            f"'{column}' after parsing. Investigate before trusting "
            f"this output downstream."
        )


# ---------------------------------------------------------------------------
# Transform: order_items
# ---------------------------------------------------------------------------

order_items_df = read_raw("order_items")
order_items_df = to_snake_case(order_items_df)

# creation_time_utc is ISO 8601, but not uniformly formatted: most rows
# include milliseconds (e.g. 2023-03-08T11:03:32.223Z), but 187 rows are
# whole-second timestamps with no fractional component at all
# (e.g. 2023-05-04T10:31:19Z) -- confirmed by directly inspecting the
# rows that failed a single strict '.SSS' pattern. Try both formats
# explicitly and coalesce, rather than assuming every row has millis.
order_items_df = order_items_df.withColumn(
    "creation_time_utc",
    F.coalesce(
        F.to_timestamp("creation_time_utc", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp("creation_time_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ),
)
check_parse_failures(order_items_df, "creation_time_utc", "order_items")

order_items_df = order_items_df.coalesce(1)

order_items_df.write.mode("overwrite").parquet(
    f"s3://{S3_BUCKET}/processed/order_items/"
)
print(f"order_items: wrote {order_items_df.count()} rows to processed/")

# ---------------------------------------------------------------------------
# Transform: order_item_options
# ---------------------------------------------------------------------------

order_item_options_df = read_raw("order_item_options")
order_item_options_df = to_snake_case(order_item_options_df)
order_item_options_df = order_item_options_df.coalesce(1)

order_item_options_df.write.mode("overwrite").parquet(
    f"s3://{S3_BUCKET}/processed/order_item_options/"
)
print(f"order_item_options: wrote {order_item_options_df.count()} rows to processed/")

# ---------------------------------------------------------------------------
# Transform: date_dim
# ---------------------------------------------------------------------------
# date_key is stored as DD-MM-YYYY text (e.g. "16-10-2023"), confirmed
# via direct inspection of extracted output -- not ISO, and not safe to
# sort/compare as a plain string. Parsed explicitly into a real date type;
# the original string is dropped rather than kept alongside, since nothing
# downstream needs the raw text once a proper date column exists.

date_dim_df = read_raw("date_dim")
date_dim_df = to_snake_case(date_dim_df)

date_dim_df = date_dim_df.withColumn("date_key", F.to_date("date_key", "dd-MM-yyyy"))
check_parse_failures(date_dim_df, "date_key", "date_dim")

date_dim_df = date_dim_df.coalesce(1)

date_dim_df.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/processed/date_dim/")
print(f"date_dim: wrote {date_dim_df.count()} rows to processed/")

job.commit()
