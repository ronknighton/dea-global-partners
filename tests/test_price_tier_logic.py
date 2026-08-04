"""
Example unit test demonstrating the testable pattern for this project's
Glue ETL scripts.

IMPORTANT CAVEAT, stated honestly rather than glossed over: the actual
aggregate_job.py (and extract_job.py, transform_job.py) are written as
flat top-level scripts -- the Glue-specific boilerplate (SparkContext(),
GlueContext(sc), job.init(...)) executes immediately at import time.
That boilerplate requires the real awsglue library, which is only
available inside AWS Glue's actual runtime (or the official Glue Docker
image), not a standard GitHub Actions runner with a plain `pip install
pyspark`.

Because of that, these scripts cannot be directly imported and unit
tested as-is in CI. This test instead demonstrates the pattern with a
standalone reimplementation of one piece of pure transformation logic
(the discount_effectiveness price-tier bucketing) using only plain
PySpark, which IS installable and testable in a normal CI runner.

For a more mature version of this pipeline, the recommended next step
would be refactoring the actual job scripts to separate pure
transformation functions (importable, testable with plain PySpark) from
the thin Glue-specific entry point (SparkContext/GlueContext/job.init),
so tests like this one exercise the real logic directly rather than a
parallel reimplementation. Noted here as a known gap, not silently
glossed over -- the manual validation process used throughout this
project's development (documented in the solution design doc) is what
currently covers correctness for the full scripts.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from chispa import assert_df_equality


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-price-tier")
        .getOrCreate()
    )


def bucket_price_tier(df, price_col="item_price"):
    """Standalone reimplementation of the price-tier logic from
    aggregate_job.py's discount_effectiveness section, for testing
    without needing the Glue runtime. Keep this in sync with the real
    script if the tiering logic changes."""
    return df.withColumn(
        "price_tier",
        F.when(F.col(price_col) == 0, "Free")
        .when(F.col(price_col) < 5.0, "$0.01-$5")
        .when(F.col(price_col) < 10.0, "$5-$10")
        .when(F.col(price_col) < 20.0, "$10-$20")
        .otherwise("$20+"),
    )


def test_price_tier_boundaries(spark):
    input_df = spark.createDataFrame(
        [(0.0,), (0.01,), (4.99,), (5.0,), (9.99,), (10.0,), (19.99,), (20.0,), (50.0,)],
        ["item_price"],
    )

    result = bucket_price_tier(input_df)

    expected = spark.createDataFrame(
        [
            (0.0, "Free"),
            (0.01, "$0.01-$5"),
            (4.99, "$0.01-$5"),
            (5.0, "$5-$10"),
            (9.99, "$5-$10"),
            (10.0, "$10-$20"),
            (19.99, "$10-$20"),
            (20.0, "$20+"),
            (50.0, "$20+"),
        ],
        ["item_price", "price_tier"],
    )

    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


def test_price_tier_no_nulls_produced(spark):
    """Every row should get a tier label -- none should fall through
    to null, which would indicate a gap in the boundary conditions."""
    input_df = spark.createDataFrame([(0.0,), (100.0,), (0.005,)], ["item_price"])
    result = bucket_price_tier(input_df)
    null_count = result.filter(F.col("price_tier").isNull()).count()
    assert null_count == 0
