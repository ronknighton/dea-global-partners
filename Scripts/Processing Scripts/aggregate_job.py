"""
dea-global-partners: Aggregate job
Reads processed/order_items, processed/order_item_options, and
processed/date_dim, computes the seven target marts, writes to marts/.

All aggregation logic runs in PySpark, per the project's explicit
"all logics should be done using PYSPARK" requirement -- unlike the
Healthcare Metrics project, this does NOT use Athena CTAS for
aggregation. Athena's role here is read-only querying of the finished
marts/ tables for QuickSight, not computing them.

This is the first place order_items and order_item_options are actually
joined (deferred from the transform job -- see transform_job.py header).

Each of the seven metrics is wrapped in try/except so a failure in one
does not block the others from running -- important because this job
computes all seven in one run, sharing a Spark session. Without this,
one bad metric would halt everything after it, leaving marts/ in an
inconsistent state (some tables freshly correct, others stale) while
still reporting total job failure. With this, every metric that CAN
succeed does, failures are logged individually, and the job still
correctly reports overall failure at the end if anything went wrong --
so Glue Workflow's retry logic still sees an accurate signal.
"""

import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = "dea-global-partners-rhk"

metric_results = {}


def read_processed(table_name: str):
    return spark.read.parquet(f"s3://{S3_BUCKET}/processed/{table_name}/")


order_items = read_processed("order_items")
order_item_options = read_processed("order_item_options")

# ---------------------------------------------------------------------------
# Data quality fix: item price anomalies
# ---------------------------------------------------------------------------
# Investigation (triggered by an unrealistic single-day revenue spike in
# sales_trends -- one day showing ~$2.5M from just 20 orders) found line
# items priced at extreme multiples of that same item's own median price
# (e.g. Korean Kimchi at $5,000 vs. its normal $30 -- confirmed against
# the same item name appearing at a normal price elsewhere in the data).
# Distinguished from legitimate high-quantity catering orders (which show
# NORMAL per-unit pricing at large quantities, price_ratio ~1x) by
# comparing each row's price against that item's own median, not a flat
# dollar/quantity threshold -- a flat threshold would have incorrectly
# excluded real catering orders alongside the genuine errors.
#
# PRICE_ANOMALY_RATIO_THRESHOLD (20x) chosen from the actual ratio
# distribution (SQL Server-side investigation, not guessed): p99=3x,
# p99.9=9x -- the natural ceiling of legitimate price variation (promo/
# seasonal pricing) tops out under 10x. 20x sits comfortably above that
# ceiling with margin to spare, while still catching the confirmed
# genuine errors (which ranged 140x-500x) with room to spare on the
# other side. Excludes 37 of 203,517 rows (0.018%).
#
# Notable finding: 3 of these 37 anomalous rows belong to accounts
# already flagged as is_likely_non_individual (see below) -- a
# compounding, not competing, data quality issue for those specific
# accounts. This filter runs BEFORE customer_volume is computed, so the
# is_likely_non_individual flag itself is based on cleaned line-item
# counts (though removing 37 rows total was never close to shifting any
# customer across the 500-line-item threshold either way).

PRICE_ANOMALY_RATIO_THRESHOLD = 20

item_median_price = (
    order_items
    .filter(F.col("item_name").isNotNull())
    .groupBy("item_name")
    .agg(F.expr("percentile_approx(item_price, 0.5)").alias("median_item_price"))
)

order_items_with_ratio = order_items.join(item_median_price, on="item_name", how="left")
order_items_with_ratio = order_items_with_ratio.withColumn(
    "price_ratio",
    F.when(
        F.col("median_item_price") > 0,
        F.col("item_price") / F.col("median_item_price"),
    ),  # null if median is 0 or item_name was null -- treated as not anomalous below
)

anomaly_count = order_items_with_ratio.filter(
    F.col("price_ratio") > PRICE_ANOMALY_RATIO_THRESHOLD
).count()
print(f"Price anomaly filter: excluding {anomaly_count} line items priced "
      f">{PRICE_ANOMALY_RATIO_THRESHOLD}x their item's own median price")

order_items = (
    order_items_with_ratio
    .filter(
        (F.col("price_ratio") <= PRICE_ANOMALY_RATIO_THRESHOLD)
        | F.col("price_ratio").isNull()
    )
    .drop("median_item_price", "price_ratio")
)
date_dim = read_processed("date_dim")


def with_calendar_attrs(df, date_col: str):
    """Derives year/month/week/day_of_week/is_weekend directly from a
    date column via Spark's built-in date functions, then left-joins the
    source date_dim ONLY for is_holiday/holiday_name.

    This matters because date_dim only covers 365 rows (roughly one
    year), while order data spans ~3.8 years (2020-04-21 to 2024-02-21,
    confirmed via direct validation). year/month/week/day_of_week/
    is_weekend don't actually require date_dim at all -- they're
    mechanically derivable from any date, for any year. Only holiday
    knowledge genuinely requires the source date_dim, since that can't
    be computed from a date alone. Deriving the rest directly avoids
    silently nulling out calendar attributes for ~73% of dates (the
    portion outside date_dim's actual coverage), at the cost of
    is_holiday/holiday_name remaining genuinely null outside whatever
    single year date_dim covers -- a real, narrower, documented gap
    rather than losing every calendar dimension.
    """
    df = (
        df
        .withColumn("year", F.year(date_col))
        .withColumn("month", F.month(date_col))
        .withColumn("week", F.weekofyear(date_col))
        .withColumn("day_of_week", F.date_format(date_col, "EEEE"))
        .withColumn("is_weekend", F.dayofweek(date_col).isin(1, 7))  # Spark: 1=Sun, 7=Sat
    )
    df = df.join(
        date_dim.select(
            date_dim["date_key"],
            date_dim["is_holiday"],
            date_dim["holiday_name"],
        ),
        df[date_col] == date_dim["date_key"],
        how="left",
    ).drop("date_key")
    return df


# ---------------------------------------------------------------------------
# Shared prep: per-line-item total revenue (base item + attached options)
# ---------------------------------------------------------------------------
# This is the first place order_items and order_item_options are joined --
# deliberately deferred from the transform job (see transform_job.py) to
# avoid fanning out order_items' grain until it's actually needed here.
#
# NOT wrapped in try/except -- every metric below depends on this. If it
# fails, nothing downstream can run anyway, so let it fail loudly and
# immediately rather than having all seven metrics individually catch
# the same root cause.

options_revenue = (
    order_item_options
    .withColumn("option_revenue", F.col("option_price") * F.col("option_quantity"))
    .groupBy("order_id", "lineitem_id")
    .agg(F.sum("option_revenue").alias("options_revenue"))
)

line_items_with_revenue = (
    order_items
    .withColumn("item_revenue", F.col("item_price") * F.col("item_quantity"))
    .join(options_revenue, on=["order_id", "lineitem_id"], how="left")
    .withColumn("options_revenue", F.coalesce(F.col("options_revenue"), F.lit(0.0)))
    .withColumn("total_revenue", F.col("item_revenue") + F.col("options_revenue"))
)

# ---------------------------------------------------------------------------
# Data quality flag: is_likely_non_individual
# ---------------------------------------------------------------------------
# Investigation finding (independently confirmed against source RDS data,
# not a pipeline artifact): a small number of user_ids show line-item
# volume far beyond plausible individual behavior -- e.g. 700-2,500+
# line items across the dataset's ~4-year span, several spanning 6-19
# distinct restaurant_ids, mostly with no printed_card_number on file,
# with one account ordering on 81% of all days in its active window.
# This pattern (near-daily cadence, multi-location, no card) is far more
# consistent with a corporate/default/no-login account than an
# individual customer.
#
# NON_INDIVIDUAL_THRESHOLD (500 line items) is chosen from the actual
# distribution shape, not a round number: the customer base shows a
# smooth, continuous long tail up through the 99.9th percentile (372
# line items -- plausible for a genuinely frequent weekly customer over
# 4 years), then a sharp discontinuity to the 99.99th percentile (1,131)
# -- a different population, not a continuation of the same trend. 500
# sits in the gap between the smooth tail and the confirmed-anomalous
# cluster (all directly-investigated accounts were 724+), conservative
# enough not to mislabel genuinely loyal individual customers.
#
# This is a FLAG, not a filter -- flagged customers are NOT excluded
# from any mart. Whether these represent known corporate/catering
# accounts or a genuine data quality issue is a business question for
# the SME, not something to resolve unilaterally in the pipeline.
# Downstream dashboards/analysts can filter on this column as needed.
#
# Also not wrapped -- every customer-level metric below depends on this.

NON_INDIVIDUAL_THRESHOLD = 500

customer_volume = (
    line_items_with_revenue
    .filter(F.col("user_id").isNotNull())
    .groupBy("user_id")
    .agg(F.count("*").alias("total_line_items"))
    .withColumn(
        "is_likely_non_individual",
        F.col("total_line_items") > NON_INDIVIDUAL_THRESHOLD,
    )
    .select("user_id", "is_likely_non_individual")
)

flagged_count = customer_volume.filter(F.col("is_likely_non_individual")).count()
print(f"is_likely_non_individual: flagged {flagged_count} of "
      f"{customer_volume.count()} customers (threshold: >{NON_INDIVIDUAL_THRESHOLD} line items)")

# ---------------------------------------------------------------------------
# Mart: clv_daily
# ---------------------------------------------------------------------------
# Grain: one row per user_id per order date the customer actually ordered
# on (not a full calendar spine of every day for every customer -- on
# days with no order, cumulative CLV is unchanged from the prior value
# by construction, so a forward-filled row adds no information a
# dashboard can't already infer from a step chart. Documented as a
# deliberate scale-driven simplification; a full calendar spine would be
# the production-scale answer if a dashboard specifically needed
# "no purchase yet today" rows to render correctly).
#
# user_id is nullable in the source; rows with a null user_id are
# excluded here since there is no customer to attribute that spend to.

customer_rfm_base = None  # populated in the RFM block below; churn_risk depends on it

try:
    null_user_count = line_items_with_revenue.filter(F.col("user_id").isNull()).count()
    print(f"clv_daily: excluding {null_user_count} line items with a null user_id")

    clv_base = (
        line_items_with_revenue
        .filter(F.col("user_id").isNotNull())
        .withColumn("order_date", F.to_date("creation_time_utc"))
        .groupBy("user_id", "order_date")
        .agg(F.sum("total_revenue").alias("daily_revenue"))
    )

    customer_date_window = (
        Window.partitionBy("user_id")
        .orderBy("order_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    clv_daily = clv_base.withColumn(
        "cumulative_clv", F.sum("daily_revenue").over(customer_date_window)
    )
    clv_daily = with_calendar_attrs(clv_daily, "order_date").select(
        "user_id",
        "order_date",
        "daily_revenue",
        "cumulative_clv",
        "year",
        "month",
        "week",
        "day_of_week",
        "is_weekend",
        "is_holiday",
    ).join(customer_volume, on="user_id", how="left").coalesce(1)

    clv_daily.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/clv_daily/")
    print(f"clv_daily: wrote {clv_daily.count()} rows to marts/")
    metric_results["clv_daily"] = "success"
except Exception as e:
    print(f"ERROR: clv_daily failed -- {e}")
    metric_results["clv_daily"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Mart: customer_segments_rfm
# ---------------------------------------------------------------------------
# Grain: one row per user_id (snapshot, not daily like clv_daily).
# Same null-user_id exclusion as clv_daily.
#
# Reference date for recency: since this is static historical data (not a
# live feed), "today" isn't a meaningful anchor -- recency is computed
# against the most recent order date present in the dataset. Documented
# as a known adaptation for static data; a production/live version would
# anchor recency to the actual current date instead.

try:
    reference_date = order_items.select(F.max(F.to_date("creation_time_utc"))).first()[0]
    print(f"customer_segments_rfm: using reference date {reference_date}")

    customer_rfm_base = (
        line_items_with_revenue
        .filter(F.col("user_id").isNotNull())
        .withColumn("order_date", F.to_date("creation_time_utc"))
        .groupBy("user_id")
        .agg(
            F.datediff(F.lit(reference_date), F.max("order_date")).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("total_revenue").alias("monetary"),
        )
    )

    # Quintile scoring (1-5, 5 = best) via ntile. For frequency/monetary,
    # ascending order means the top bucket (5) naturally lands on the
    # highest values. For recency, ascending order means bucket 1 gets
    # the *lowest* recency_days (most recent = best) -- so the score is
    # inverted (6 - bucket) to keep "5 = best" consistent across all
    # three dimensions.

    recency_window = Window.orderBy(F.col("recency_days").asc())
    frequency_window = Window.orderBy(F.col("frequency").asc())
    monetary_window = Window.orderBy(F.col("monetary").asc())

    customer_rfm = (
        customer_rfm_base
        .withColumn("recency_score", 6 - F.ntile(5).over(recency_window))
        .withColumn("frequency_score", F.ntile(5).over(frequency_window))
        .withColumn("monetary_score", F.ntile(5).over(monetary_window))
    )

    # Segment labels, matching the requirements doc's dashboard language
    # (VIP / New / Churn Risk). "New" is based on true lifetime order
    # count (exactly one order ever), not just a low frequency score, so
    # it correctly identifies genuinely first-time customers rather than
    # anyone in the bottom frequency quintile.

    customer_segments_rfm = (
        customer_rfm
        .withColumn(
            "segment",
            F.when(
                (F.col("recency_score") >= 4) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4),
                "VIP",
            )
            .when(
                (F.col("frequency") == 1) & (F.col("recency_score") >= 4),
                "New",
            )
            .when(
                (F.col("recency_score") <= 2) & ((F.col("frequency_score") >= 3) | (F.col("monetary_score") >= 3)),
                "Churn Risk",
            )
            .otherwise("Regular"),
        )
        .join(customer_volume, on="user_id", how="left")
        .coalesce(1)
    )

    customer_segments_rfm.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/customer_segments_rfm/")
    print(f"customer_segments_rfm: wrote {customer_segments_rfm.count()} rows to marts/")
    metric_results["customer_segments_rfm"] = "success"
except Exception as e:
    print(f"ERROR: customer_segments_rfm failed -- {e}")
    metric_results["customer_segments_rfm"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Mart: churn_risk
# ---------------------------------------------------------------------------
# Grain: one row per user_id. Reuses the same reference_date and
# recency_days basis as customer_segments_rfm, rather than recomputing.
#
# Two complementary signals, not just one:
#   risk_tier      -- absolute day-threshold buckets (Active/At Risk/
#                      Churned) against the same fixed reference date.
#                      Thresholds (30/90 days) are a business assumption
#                      for a frequent-order food category, not a derived
#                      fact -- worth tuning with real stakeholder input
#                      rather than treating as ground truth.
#   is_overdue     -- a more behavioral signal: flags customers who have
#                      gone quiet relative to THEIR OWN typical ordering
#                      cadence, not everyone's the same fixed thresholds.
#                      avg_days_between_orders is approximated as each
#                      customer's (last order date - first order date)
#                      divided by (distinct order days - 1) -- i.e. an
#                      even-spacing approximation, not an exact average
#                      of individual gaps (which would need a per-
#                      customer sorted-date-list computation). Simpler
#                      and sufficient to capture overall cadence;
#                      documented as an approximation. Null/not-flagged
#                      for one-time customers, since a single order has
#                      no cadence to compare against.
#
# Depends on customer_rfm_base from the RFM block above -- if RFM
# failed, this metric cannot run either, and will correctly fail too.

try:
    if customer_rfm_base is None:
        raise RuntimeError("customer_rfm_base unavailable -- customer_segments_rfm must succeed first")

    ACTIVE_THRESHOLD_DAYS = 30
    CHURNED_THRESHOLD_DAYS = 90

    customer_order_pattern = (
        line_items_with_revenue
        .filter(F.col("user_id").isNotNull())
        .withColumn("order_date", F.to_date("creation_time_utc"))
        .groupBy("user_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_date").alias("distinct_order_days"),
        )
        .withColumn(
            "avg_days_between_orders",
            F.when(
                F.col("distinct_order_days") > 1,
                F.datediff("last_order_date", "first_order_date") / (F.col("distinct_order_days") - 1),
            ),  # null otherwise -- one-time customers have no cadence
        )
    )

    churn_risk = (
        customer_rfm_base
        .join(customer_order_pattern, on="user_id", how="left")
        .withColumn(
            "risk_tier",
            F.when(F.col("recency_days") <= ACTIVE_THRESHOLD_DAYS, "Active")
            .when(F.col("recency_days") <= CHURNED_THRESHOLD_DAYS, "At Risk")
            .otherwise("Churned"),
        )
        .withColumn(
            "is_overdue",
            F.when(
                F.col("avg_days_between_orders").isNotNull(),
                F.col("recency_days") > (2 * F.col("avg_days_between_orders")),
            ).otherwise(F.lit(False)),
        )
        .select(
            "user_id",
            "recency_days",
            "frequency",
            "monetary",
            "avg_days_between_orders",
            "risk_tier",
            "is_overdue",
        )
        .join(customer_volume, on="user_id", how="left")
        .coalesce(1)
    )

    churn_risk.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/churn_risk/")
    print(f"churn_risk: wrote {churn_risk.count()} rows to marts/")
    metric_results["churn_risk"] = "success"
except Exception as e:
    print(f"ERROR: churn_risk failed -- {e}")
    metric_results["churn_risk"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Mart: sales_trends
# ---------------------------------------------------------------------------
# Grain: one row per calendar date (business-level, not per-customer).
#
# Deliberately does NOT exclude null-user_id line items, unlike every
# customer-level metric above. CLV/RFM/churn are customer metrics, so a
# null user_id genuinely means "no customer to attribute this to."
# Sales trends is a business-level metric -- an order with no attached
# user_id (e.g. guest checkout) is still real revenue that happened on
# that day, and excluding it would understate total daily revenue.
#
# Only daily grain is materialized here; weekly/monthly/yearly rollups
# are left to QuickSight's native date-hierarchy drill (Year > Month >
# Day) rather than building separate pre-aggregated tables for each
# level, which would just be redundant data QuickSight doesn't need.

try:
    sales_trends_base = (
        line_items_with_revenue
        .withColumn("order_date", F.to_date("creation_time_utc"))
        .groupBy("order_date")
        .agg(
            F.sum("total_revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))
    )

    sales_trends = with_calendar_attrs(sales_trends_base, "order_date").select(
        "order_date",
        "total_revenue",
        "order_count",
        "avg_order_value",
        "year",
        "month",
        "week",
        "day_of_week",
        "is_weekend",
        "is_holiday",
    ).coalesce(1)

    sales_trends.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/sales_trends/")
    print(f"sales_trends: wrote {sales_trends.count()} rows to marts/")
    metric_results["sales_trends"] = "success"
except Exception as e:
    print(f"ERROR: sales_trends failed -- {e}")
    metric_results["sales_trends"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Mart: loyalty_impact
# ---------------------------------------------------------------------------
# Grain: (is_loyalty, is_likely_non_individual) -- order-level, not
# customer-level. is_loyalty is stored per order_items row, and a
# loyalty-consistency check found it is NOT a stable per-customer trait:
# 2,783 customers showed BOTH True and False across different orders,
# with no clean skew (mean 42% loyalty-flagged, std 0.25). Given that,
# average CLV per loyalty group was considered and deliberately
# excluded here -- CLV is a customer-level cumulative metric, and
# forcing it onto an order-level, per-customer-inconsistent flag would
# require an arbitrary rule (e.g. ">=50% of orders flagged loyalty ->
# loyalty customer") with no clear justification. Revenue/order-count/
# avg-order-value all work cleanly at the order-level grain the flag
# actually lives at, without that problem.
#
# is_likely_non_individual included as a second grouping dimension
# (not a filter) -- consistent with how it's handled everywhere else
# in this job -- so a dashboard can filter those 19 accounts in or out
# as needed rather than the pipeline deciding for them.

try:
    loyalty_impact_base = (
        line_items_with_revenue
        .join(customer_volume, on="user_id", how="left")
        .withColumn(
            # null user_id -> not flaggable, treat as False
            "is_likely_non_individual",
            F.coalesce(F.col("is_likely_non_individual"), F.lit(False)),
        )
        .groupBy("is_loyalty", "is_likely_non_individual")
        .agg(
            F.sum("total_revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("distinct_customers"),
        )
        .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))
        .coalesce(1)
    )

    loyalty_impact_base.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/loyalty_impact/")
    print(f"loyalty_impact: wrote {loyalty_impact_base.count()} rows to marts/")
    metric_results["loyalty_impact"] = "success"
except Exception as e:
    print(f"ERROR: loyalty_impact failed -- {e}")
    metric_results["loyalty_impact"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Mart: location_performance
# ---------------------------------------------------------------------------
# Grain: one row per restaurant_id. Business-level like sales_trends --
# includes all orders regardless of user_id nullability, since a
# location's revenue is real regardless of whether the customer was
# identifiable.
#
# is_likely_non_individual is surfaced as informational columns
# (flagged-account order count / revenue share) rather than a second
# grouping dimension like loyalty_impact -- the total number of
# restaurant_ids isn't known to be small, so splitting every location
# into 2 rows risked a lot of sparse/empty combinations. This still
# makes the flag's impact visible per location (two of the 19 flagged
# accounts are tied to just 1-2 restaurant_ids each, per the earlier
# investigation, which could visibly skew those specific locations'
# totals) without forcing a row split everywhere.

try:
    location_base = line_items_with_revenue.join(customer_volume, on="user_id", how="left")

    location_performance = (
        location_base
        .groupBy("restaurant_id")
        .agg(
            F.sum("total_revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("distinct_customers"),
            F.sum(
                F.when(F.col("is_likely_non_individual"), F.col("total_revenue")).otherwise(0.0)
            ).alias("flagged_account_revenue"),
            F.countDistinct(
                F.when(F.col("is_likely_non_individual"), F.col("order_id"))
            ).alias("flagged_account_order_count"),
        )
        .withColumn("avg_order_value", F.col("total_revenue") / F.col("order_count"))
        .withColumn(
            "flagged_revenue_share",
            F.col("flagged_account_revenue") / F.col("total_revenue"),
        )
        .coalesce(1)
    )

    location_performance.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/location_performance/")
    print(f"location_performance: wrote {location_performance.count()} rows to marts/")
    metric_results["location_performance"] = "success"
except Exception as e:
    print(f"ERROR: location_performance failed -- {e}")
    metric_results["location_performance"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Mart: discount_effectiveness
# ---------------------------------------------------------------------------
# IMPORTANT SCOPE NOTE: the source data has no discount, promo, or price-
# adjustment field of any kind, and no negative-price pattern that would
# indicate a discount was applied (confirmed via direct inspection --
# item_price and option_price are both entirely non-negative across the
# full dataset). option_price = 0 accounts for 66% of all option rows,
# but this is the normal pattern for free customization options (e.g.
# "No Cheese"), not discounts -- already correctly reflected as $0
# revenue contribution elsewhere in this job. item_price = 0 occurs on
# only 0.08% of order items (156 rows), too sparse to represent a real
# discount/promo program.
#
# Given no true discount signal exists, this mart is repurposed as a
# PRICE-TIER analysis -- which price points drive the most volume and
# revenue -- the closest legitimate question this data can answer, and
# explicitly NOT presented as literal discount effectiveness. This scope
# substitution should be raised with the SME; documented here as a
# known, deliberate limitation rather than a silently invented metric.

try:
    PRICE_TIER_BOUNDARIES = [0.01, 5.0, 10.0, 20.0]

    discount_effectiveness = (
        line_items_with_revenue
        .withColumn(
            "price_tier",
            F.when(F.col("item_price") == 0, "Free")
            .when(F.col("item_price") < PRICE_TIER_BOUNDARIES[0], "Free")
            .when(F.col("item_price") < PRICE_TIER_BOUNDARIES[1], "$0.01-$5")
            .when(F.col("item_price") < PRICE_TIER_BOUNDARIES[2], "$5-$10")
            .when(F.col("item_price") < PRICE_TIER_BOUNDARIES[3], "$10-$20")
            .otherwise("$20+"),
        )
        .groupBy("price_tier")
        .agg(
            F.sum("total_revenue").alias("total_revenue"),
            F.count("*").alias("line_item_count"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("distinct_customers"),
        )
        .coalesce(1)
    )

    discount_effectiveness.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/marts/discount_effectiveness/")
    print(f"discount_effectiveness: wrote {discount_effectiveness.count()} rows to marts/")

    # Secondary finding (printed, not a separate mart): does an order
    # containing at least one free item associate with higher or lower
    # total order value? Small sample (156 free-item line items across
    # the whole dataset) -- directional signal only, not a robust
    # statistical claim.

    orders_with_free_item = (
        line_items_with_revenue
        .filter(F.col("item_price") == 0)
        .select("order_id")
        .distinct()
    )

    order_totals = (
        line_items_with_revenue
        .groupBy("order_id")
        .agg(F.sum("total_revenue").alias("order_total"))
    )

    avg_with_free = (
        order_totals.join(orders_with_free_item, on="order_id", how="inner")
        .agg(F.avg("order_total")).first()[0]
    )
    avg_without_free = (
        order_totals.join(orders_with_free_item, on="order_id", how="left_anti")
        .agg(F.avg("order_total")).first()[0]
    )
    free_item_order_count = orders_with_free_item.count()

    print(f"discount_effectiveness (secondary finding): "
          f"avg order value WITH a free item ({free_item_order_count} orders) = {avg_with_free:.2f}, "
          f"avg order value WITHOUT = {avg_without_free:.2f}")
    metric_results["discount_effectiveness"] = "success"
except Exception as e:
    print(f"ERROR: discount_effectiveness failed -- {e}")
    metric_results["discount_effectiveness"] = f"failed: {e}"

# ---------------------------------------------------------------------------
# Final status check -- fail the job if any metric failed, so Glue
# Workflow's retry/failure-reload logic sees an accurate signal, even
# though the metrics that succeeded already have their output written.
# ---------------------------------------------------------------------------

print()
print("=" * 60)
print("Aggregate job summary:")
for metric, result in metric_results.items():
    print(f"  {metric}: {result}")
print("=" * 60)

failures = {k: v for k, v in metric_results.items() if v != "success"}
if failures:
    raise Exception(f"Aggregate job completed with failures: {failures}")

job.commit()
