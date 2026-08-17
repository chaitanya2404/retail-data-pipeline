"""Spark session construction and the lakehouse schema.

Two things live here because both must be identical across every job: how a session is built, and
what the data is shaped like. A job that declares its own schema inline is a job that will
disagree with its neighbour after the third edit.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAKE_ROOT = REPO_ROOT / "data" / "lake"

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"


#: The raw contract, declared rather than inferred.
#:
#: ``inferSchema`` reads the whole file twice — once to guess types, once to load — and then
#: guesses differently whenever a batch happens to contain no nulls or no decimals. A declared
#: schema makes the read single-pass and turns "the source changed" into a visible failure.
#:
#: Money is DecimalType, not Double. A double cannot represent 0.1 exactly, so summing hundreds of
#: thousands of prices accumulates error; on this dataset the drift is pennies, but pennies that
#: never reconcile against finance are worse than a slower job.
RAW_SCHEMA = StructType(
    [
        StructField("InvoiceNo", StringType(), nullable=False),
        StructField("StockCode", StringType(), nullable=False),
        StructField("Description", StringType(), nullable=True),
        StructField("Quantity", IntegerType(), nullable=False),
        StructField("InvoiceDate", TimestampType(), nullable=False),
        StructField("UnitPrice", DecimalType(12, 4), nullable=False),
        StructField("CustomerID", StringType(), nullable=True),
        StructField("Country", StringType(), nullable=False),
    ]
)


def lake_path(layer: str, table: str, root: Path | str | None = None) -> str:
    """Location of a table within the lake."""
    base = Path(root or os.environ.get("LAKE_ROOT", DEFAULT_LAKE_ROOT))
    return str(base / layer / table)


def build_session(app_name: str, *, shuffle_partitions: int = 8) -> SparkSession:
    """A local session configured for a laptop-sized dataset.

    The defaults Spark ships assume a cluster. Left alone on a single machine they make a
    half-million-row job slower than pandas and give a misleading impression of what Spark costs.
    """
    # Driver and workers must run the same minor version of Python or Spark refuses to start,
    # and the error names neither interpreter. Pinning both to the running one removes the class
    # of failure entirely rather than documenting a workaround in the README.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # 200 is the cluster default. On this data each partition would hold a few thousand rows,
        # so the job spends its time on task scheduling rather than on work.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Adaptive execution coalesces small post-shuffle partitions and switches join strategies
        # from real statistics rather than from the planner's estimate.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Spark 3 changed calendar handling; CORRECTED rejects ambiguous pre-Gregorian dates
        # instead of silently rebasing them, which is the safer failure for financial data.
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", os.environ.get("SPARK_UI", "false"))
        .getOrCreate()
    )
