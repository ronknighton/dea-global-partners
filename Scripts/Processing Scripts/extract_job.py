"""
dea-global-partners: Extract job
Pulls order_items, order_item_options, and date_dim from RDS SQL Server
(GlobalPartners) into S3 raw/, using an incremental watermark on
creation_time_utc for the two transactional tables. date_dim is loaded
in full each run (small, slowly-changing dimension table).

Checkpoint (last extracted creation_time_utc) is read from and written to
SSM Parameter Store, so this job behaves correctly whether run once
against static historical data or on a recurring schedule against live data.
"""

import sys
import boto3
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

# ---------------------------------------------------------------------------
# Job setup
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

REGION = "us-east-1"
CONNECTION_NAME = "dea-global-partners-rds-connection"
S3_BUCKET = "dea-global-partners-rhk"
CHECKPOINT_PARAM = "/dea-global-partners/checkpoint/order_items_last_extracted"

# NOTE: source columns are uppercase (confirmed against live schema via
# sys.columns query on 2026-07-30). order_items has no single-column
# surrogate key -- ORDER_ID + LINEITEM_ID together form the natural key
# that order_item_options joins against.

ssm = boto3.client("ssm", region_name=REGION)

# ---------------------------------------------------------------------------
# Checkpoint read
# ---------------------------------------------------------------------------
# On the very first run, the parameter won't exist yet -- fall back to a
# date far enough in the past to capture the entire historical dataset.


def get_checkpoint():
    try:
        response = ssm.get_parameter(Name=CHECKPOINT_PARAM)
        return response["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        return "1900-01-01T00:00:00.000Z"


def set_checkpoint(new_value: str):
    ssm.put_parameter(
        Name=CHECKPOINT_PARAM,
        Value=new_value,
        Type="String",
        Overwrite=True,
    )


checkpoint = get_checkpoint()
print(f"Using checkpoint: {checkpoint}")

# Ingestion date partition, used for all three tables' raw/ output paths
ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Extract: order_items (incremental, watermark on CREATION_TIME_UTC)
# ---------------------------------------------------------------------------
# NOTE: pulling the full table via dbtable and filtering in Spark, rather
# than pushing the WHERE clause down via the "query" connection option.
# The "query" option combined with connectionName is a documented AWS Glue
# bug (raises "key not found: location"/"url" inconsistently across
# versions -- see AWS re:Post). At this data volume (~200K rows), a full
# pull + in-memory filter costs nothing meaningful and avoids the bug
# entirely rather than fighting it.

order_items_dyf_full = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": CONNECTION_NAME,
        "dbtable": "dbo.order_items",
    },
)

order_items_df_full = order_items_dyf_full.toDF()
order_items_df = order_items_df_full.filter(
    order_items_df_full["CREATION_TIME_UTC"] > checkpoint
)

order_items_count = order_items_df.count()
print(f"order_items: {order_items_count} new rows since checkpoint")


if order_items_count > 0:
    order_items_df = order_items_df.coalesce(1)
    order_items_dyf = DynamicFrame.fromDF(
        order_items_df, glueContext, "order_items_dyf"
    )
    glueContext.write_dynamic_frame.from_options(
        frame=order_items_dyf,
        connection_type="s3",
        connection_options={
            "path": f"s3://{S3_BUCKET}/raw/order_items/dt={ingestion_date}/"
        },
        format="parquet",
    )

# ---------------------------------------------------------------------------
# Extract: order_item_options (incremental, same watermark via join)
# ---------------------------------------------------------------------------
# Same dbtable + Spark-filter approach as order_items above. The join to
# order_items happens in Spark rather than as a SQL Server-side JOIN, since
# we're already avoiding server-side query pushdown for the reason noted
# above. At this volume this is a trivial join for Spark.

order_item_options_dyf_full = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": CONNECTION_NAME,
        "dbtable": "dbo.order_item_options",
    },
)

order_item_options_df_full = order_item_options_dyf_full.toDF()

order_item_options_df = order_item_options_df_full.join(
    order_items_df.select("ORDER_ID", "LINEITEM_ID"),
    on=["ORDER_ID", "LINEITEM_ID"],
    how="inner",
)

order_item_options_count = order_item_options_df.count()
print(f"order_item_options: {order_item_options_count} new rows since checkpoint")

if order_item_options_count > 0:
    order_item_options_df = order_item_options_df.coalesce(1)
    order_item_options_dyf = DynamicFrame.fromDF(
        order_item_options_df, glueContext, "order_item_options_dyf"
    )
    glueContext.write_dynamic_frame.from_options(
        frame=order_item_options_dyf,
        connection_type="s3",
        connection_options={
            "path": f"s3://{S3_BUCKET}/raw/order_item_options/dt={ingestion_date}/"
        },
        format="parquet",
    )

# ---------------------------------------------------------------------------
# Extract: date_dim (full pull every run -- small, slowly-changing)
# ---------------------------------------------------------------------------

date_dim_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": CONNECTION_NAME,
        "dbtable": "dbo.date_dim",
    },
)

date_dim_df = date_dim_dyf.toDF().coalesce(1)
date_dim_dyf = DynamicFrame.fromDF(date_dim_df, glueContext, "date_dim_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=date_dim_dyf,
    connection_type="s3",
    connection_options={"path": f"s3://{S3_BUCKET}/raw/date_dim/dt={ingestion_date}/"},
    format="parquet",
)

# ---------------------------------------------------------------------------
# Checkpoint write -- only advance it if the run actually succeeded above.
# Uses current UTC time rather than MAX(creation_time_utc) from the source,
# so a run with zero new rows still correctly avoids re-scanning old data
# next time, without needing a second query back to RDS.
# ---------------------------------------------------------------------------

new_checkpoint = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
set_checkpoint(new_checkpoint)
print(f"Checkpoint advanced to: {new_checkpoint}")

job.commit()
