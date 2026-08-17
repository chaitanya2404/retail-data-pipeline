"""Run the full lakehouse build: bronze → silver → gold.

    python -m spark.jobs.run_medallion

The source is a 23MB Excel file, which Spark cannot read natively — there is no distributed .xlsx
reader, and there could not be one, since the format is a zip archive that must be decompressed
whole. It is loaded once with pandas and handed to Spark as a DataFrame. That is the correct shape
for this dataset and an honest thing to say out loud: Spark earns its place from here on, in the
transformations and the partitioned write, not in the read.
"""

from __future__ import annotations

import argparse
import logging
import time
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from spark import medallion
from spark.session import BRONZE, GOLD, RAW_SCHEMA, SILVER, build_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("medallion")


def _load_raw(spark, source: Path):
    """Read the source extract into Spark under the declared schema."""
    import pandas as pd

    pdf = pd.read_excel(source)

    # Align pandas' inferred types with RAW_SCHEMA before handing over. Spark applies the schema
    # strictly, so a float CustomerID column would fail against a StringType field; converting
    # here keeps the mismatch a two-line fix rather than a loosened schema.
    pdf["CustomerID"] = pdf["CustomerID"].map(
        lambda value: None if pd.isna(value) else str(int(float(value)))
    )
    pdf["InvoiceNo"] = pdf["InvoiceNo"].astype(str)
    pdf["StockCode"] = pdf["StockCode"].astype(str)
    pdf["Description"] = pdf["Description"].map(lambda v: None if pd.isna(v) else str(v))
    # DecimalType requires decimal.Decimal, not float — Spark refuses the implicit conversion,
    # and rightly so: going through a float is exactly the precision loss the decimal column
    # exists to prevent. Quantized to 4 places to match DecimalType(12, 4).
    pdf["UnitPrice"] = pdf["UnitPrice"].map(
        lambda v: Decimal(str(round(float(v), 4))).quantize(Decimal("0.0001"))
    )
    pdf["Quantity"] = pdf["Quantity"].astype("int32")

    return spark.createDataFrame(pdf, schema=RAW_SCHEMA)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the retail lakehouse with PySpark")
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("data/raw/online_retail.xlsx"),
        help="Path to the raw extract",
    )
    parser.add_argument("--lake-root", default=None, help="Override the lake root directory")
    args = parser.parse_args()

    if not args.source.exists():
        logger.error("source not found: %s — run the ETL extract stage first", args.source)
        return 1

    spark = build_session("retail-medallion")
    spark.sparkContext.setLogLevel("ERROR")
    started = time.perf_counter()

    try:
        raw = _load_raw(spark, args.source)

        bronze = medallion.to_bronze(raw)
        bronze_path = medallion.write_layer(
            bronze,
            BRONZE,
            "transactions",
            partition_by=["invoice_year", "invoice_month"],
            root=args.lake_root,
        )
        bronze_count = spark.read.parquet(bronze_path).count()
        logger.info("bronze: %s rows -> %s", f"{bronze_count:,}", bronze_path)

        # Read bronze back rather than reusing the in-memory DataFrame. It costs one scan and
        # proves the layer is genuinely durable and re-readable — the property the whole medallion
        # design rests on. Chaining the DataFrame instead would silently recompute from source and
        # a broken bronze write would go unnoticed.
        silver = medallion.to_silver(spark.read.parquet(bronze_path))

        # Cached because three gold aggregates read it. Without this Spark recomputes the whole
        # silver lineage once per aggregate.
        silver.cache()

        silver_path = medallion.write_layer(
            silver,
            SILVER,
            "sales",
            partition_by=["invoice_year", "invoice_month"],
            root=args.lake_root,
        )
        silver_count = silver.count()
        logger.info(
            "silver: %s rows (%s dropped as cancellations, adjustments or noise) -> %s",
            f"{silver_count:,}",
            f"{bronze_count - silver_count:,}",
            silver_path,
        )

        for name, frame, partitions in (
            ("daily_sales", medallion.gold_daily_sales(silver), ["invoice_year", "invoice_month"]),
            ("customer_rfm", medallion.gold_customer_rfm(silver), None),
            ("product_performance", medallion.gold_product_performance(silver), None),
        ):
            path = medallion.write_layer(
                frame, GOLD, name, partition_by=partitions, root=args.lake_root
            )
            logger.info("gold/%s: %s rows -> %s", name, f"{frame.count():,}", path)

        revenue = silver.agg(F.sum("line_revenue").alias("total")).collect()[0]["total"]
        logger.info("total revenue reconciled from silver: %s", f"{revenue:,.2f}")
        logger.info("lakehouse built in %.1fs", time.perf_counter() - started)

        silver.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
