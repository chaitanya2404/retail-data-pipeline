"""Bronze → silver → gold transformations.

The layers are not decoration. Each answers a different question, and collapsing them is what
produces a pipeline where "why is this number wrong" has no answer:

* **Bronze** is the source, faithfully. Nothing is dropped, nothing is corrected. It exists so a
  bad transform can be replayed without going back to the upstream system — which, for a nightly
  extract that overwrites itself, may no longer have yesterday's data at all.
* **Silver** is conformed and deduplicated. Business rules that everyone agrees on live here:
  cancellations removed, types enforced, one row per real event.
* **Gold** is shaped for consumption. Aggregates that a dashboard reads directly, so a BI tool
  never scans the full fact table to draw one line chart.

The functions take and return DataFrames rather than reading and writing internally, so each layer
is testable against a handful of rows without a filesystem.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark.session import BRONZE, GOLD, SILVER, lake_path

#: Cancellations are marked by a leading C on the invoice number, not by a flag column.
CANCELLED_INVOICE_PREFIX = "C"

#: Ledger adjustments use an A prefix ("Adjust bad debt"). They are real accounting entries but
#: not sales, and leaving them in makes every revenue figure wrong by the adjustment amount.
ADJUSTMENT_INVOICE_PREFIX = "A"


def to_bronze(raw: DataFrame) -> DataFrame:
    """Land the source with provenance attached, and nothing else changed.

    Partitioned by invoice year and month rather than by ingestion date. Analytical queries filter
    on when a sale *happened*, so that is what must prune; partitioning by load date means every
    "revenue last March" query reads every file ever written.
    """
    return (
        raw.withColumn("ingested_at", F.current_timestamp())
        .withColumn("invoice_year", F.year("InvoiceDate"))
        .withColumn("invoice_month", F.month("InvoiceDate"))
    )


def to_silver(bronze: DataFrame) -> DataFrame:
    """Conform, deduplicate, and drop what is not a sale."""
    typed = (
        bronze.select(
            F.col("InvoiceNo").alias("invoice_no"),
            F.col("StockCode").alias("product_key"),
            F.trim(F.col("Description")).alias("product_description"),
            F.col("Quantity").alias("quantity"),
            F.col("InvoiceDate").alias("invoiced_at"),
            F.col("UnitPrice").alias("unit_price"),
            # Unattributed rows are bucketed, never dropped: about a quarter of the source has no
            # customer, and discarding them would understate revenue by roughly £1.75M.
            F.coalesce(F.col("CustomerID"), F.lit("UNKNOWN")).alias("customer_key"),
            F.col("Country").alias("country"),
            F.to_date("InvoiceDate").alias("date_key"),
            F.col("invoice_year"),
            F.col("invoice_month"),
        )
        .filter(~F.col("invoice_no").startswith(CANCELLED_INVOICE_PREFIX))
        .filter(~F.col("invoice_no").startswith(ADJUSTMENT_INVOICE_PREFIX))
        # Zero-quantity and zero-price lines are catalogue noise: they inflate line counts while
        # contributing nothing to revenue.
        .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    )

    # Revenue is computed once, here, so the fact and every aggregate agree by construction.
    # Decimal arithmetic throughout — a double would drift by fractions of a penny per row and
    # the totals would stop reconciling.
    with_revenue = typed.withColumn(
        "line_revenue", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,4)")
    )

    # Exact duplicates are a re-delivery artefact, not two sales. dropDuplicates over the full
    # natural key rather than the whole row: a differing ingestion timestamp must not make the
    # same line look distinct.
    return with_revenue.dropDuplicates(
        ["invoice_no", "product_key", "invoiced_at", "quantity", "unit_price"]
    )


def gold_daily_sales(silver: DataFrame) -> DataFrame:
    """Revenue per day and country — the shape a time-series dashboard reads directly."""
    return (
        silver.groupBy("date_key", "country")
        .agg(
            F.countDistinct("invoice_no").alias("invoice_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("line_revenue").alias("revenue"),
            F.countDistinct("customer_key").alias("active_customers"),
        )
        .withColumn("invoice_year", F.year("date_key"))
        .withColumn("invoice_month", F.month("date_key"))
    )


def gold_customer_rfm(silver: DataFrame) -> DataFrame:
    """RFM scores per customer, computed against the last observed invoice.

    Anchored to the data, not to ``current_date``: this extract ends in December 2011, so
    wall-clock recency would classify every customer as churned on every run.
    """
    as_of = silver.agg(F.max("date_key").alias("as_of")).collect()[0]["as_of"]

    per_customer = silver.groupBy("customer_key").agg(
        F.min("date_key").alias("first_purchase_date"),
        F.max("date_key").alias("last_purchase_date"),
        F.countDistinct("invoice_no").alias("invoice_count"),
        F.sum("quantity").alias("total_units"),
        F.sum("line_revenue").alias("lifetime_revenue"),
    )

    scored = per_customer.withColumn(
        "recency_days", F.datediff(F.lit(as_of), F.col("last_purchase_date"))
    )

    # ntile over the whole customer base rather than fixed thresholds, which rot as the business
    # grows. The window is unpartitioned by necessity — quintiles are global — so this is the one
    # genuine full shuffle in the job.
    recency_window = Window.orderBy(F.col("recency_days").desc())
    frequency_window = Window.orderBy(F.col("invoice_count").asc())
    monetary_window = Window.orderBy(F.col("lifetime_revenue").asc())

    scored = (
        scored.withColumn("recency_score", F.ntile(5).over(recency_window))
        .withColumn("frequency_score", F.ntile(5).over(frequency_window))
        .withColumn("monetary_score", F.ntile(5).over(monetary_window))
    )

    return scored.withColumn(
        "rfm_segment",
        F.when(F.col("customer_key") == "UNKNOWN", F.lit("Unattributed"))
        .when(
            (F.col("recency_score") >= 4)
            & (F.col("frequency_score") >= 4)
            & (F.col("monetary_score") >= 4),
            F.lit("Champion"),
        )
        .when((F.col("recency_score") >= 4) & (F.col("frequency_score") >= 3), F.lit("Loyal"))
        .when(F.col("recency_score") >= 4, F.lit("Recent"))
        .when((F.col("recency_score") <= 2) & (F.col("monetary_score") >= 4), F.lit("At Risk"))
        .when(F.col("recency_score") <= 2, F.lit("Churned"))
        .otherwise(F.lit("Regular")),
    )


def gold_product_performance(silver: DataFrame) -> DataFrame:
    """Per-product totals with a deterministic description.

    The source has no product master, so the same stock code appears under several spellings. The
    most frequently used one wins, ties broken by most recent — otherwise a product renames itself
    every time the job runs.
    """
    description_window = Window.partitionBy("product_key").orderBy(
        F.col("times_used").desc(), F.col("last_used_at").desc()
    )

    resolved = (
        silver.filter(F.col("product_description").isNotNull())
        .groupBy("product_key", "product_description")
        .agg(F.count("*").alias("times_used"), F.max("invoiced_at").alias("last_used_at"))
        .withColumn("rank", F.row_number().over(description_window))
        .filter(F.col("rank") == 1)
        .select("product_key", "product_description")
    )

    totals = silver.groupBy("product_key").agg(
        F.countDistinct("invoice_no").alias("invoice_count"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("line_revenue").alias("total_revenue"),
        F.min("unit_price").alias("min_unit_price"),
        F.max("unit_price").alias("max_unit_price"),
    )

    # broadcast: the resolved-description table is a few thousand rows, so shipping it to every
    # executor turns a shuffle join into a hash join. Without the hint Spark may still shuffle
    # both sides, which on a wide fact table is the most expensive thing in the job.
    return totals.join(F.broadcast(resolved), on="product_key", how="left").withColumn(
        "avg_selling_price",
        (F.col("total_revenue") / F.col("total_units_sold")).cast("decimal(12,4)"),
    )


def write_layer(
    df: DataFrame,
    layer: str,
    table: str,
    *,
    partition_by: list[str] | None = None,
    root: str | None = None,
) -> str:
    """Write a table, overwriting the layer.

    Overwrite rather than append: this source is a single static extract, and appending on a
    re-run would silently double every revenue figure derived from it.
    """
    path = lake_path(layer, table, root)
    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    return path


__all__ = [
    "BRONZE",
    "SILVER",
    "GOLD",
    "to_bronze",
    "to_silver",
    "gold_daily_sales",
    "gold_customer_rfm",
    "gold_product_performance",
    "write_layer",
]
