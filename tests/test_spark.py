"""Tests for the PySpark medallion transformations.

A module-scoped local session is created once and reused: JVM startup dominates the runtime of a
small suite, so a per-test session would turn two seconds of assertions into a minute of waiting.

Skipped entirely when PySpark is unavailable, so the suite still runs on the 3.14 interpreter the
rest of the ETL targets.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark is not installed on this interpreter")

from spark import medallion  # noqa: E402
from spark.session import RAW_SCHEMA, build_session  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = build_session("tests", shuffle_partitions=2)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _row(invoice, code, desc, qty, when, price, customer, country):
    return (invoice, code, desc, qty, when, Decimal(price), customer, country)


@pytest.fixture
def raw(spark):
    """A batch containing every case the layers are supposed to handle."""
    rows = [
        _row("536365", "85123A", "HEART T-LIGHT", 6, datetime(2011, 1, 3, 8, 26), "2.5500", "17850", "United Kingdom"),
        _row("536365", "71053", "METAL LANTERN", 6, datetime(2011, 1, 3, 8, 26), "3.3900", "17850", "United Kingdom"),
        # No customer — must survive under UNKNOWN, not be dropped.
        _row("536368", "84406B", "COAT HANGER", 8, datetime(2011, 1, 10, 11, 30), "2.7500", None, "France"),
        # Cancellation: C prefix.
        _row("C536379", "22423", "CAKESTAND", -1, datetime(2011, 1, 11, 9, 0), "12.7500", "13047", "United Kingdom"),
        # Ledger adjustment: A prefix, positive price. The leg that inflated dbt's revenue.
        _row("A563185", "B", "Adjust bad debt", 1, datetime(2011, 8, 12, 14, 50), "11062.0600", None, "United Kingdom"),
        # Catalogue noise.
        _row("536380", "22423", "CAKESTAND", 0, datetime(2011, 1, 12, 10, 0), "12.7500", "13047", "United Kingdom"),
        _row("536381", "22423", "CAKESTAND", 3, datetime(2011, 1, 12, 10, 5), "0.0000", "13047", "United Kingdom"),
        # Exact duplicate of the first line — a re-delivery artefact, not a second sale.
        _row("536365", "85123A", "HEART T-LIGHT", 6, datetime(2011, 1, 3, 8, 26), "2.5500", "17850", "United Kingdom"),
    ]
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


class TestBronze:
    def test_keeps_every_source_row(self, raw):
        # Bronze is the replay buffer. Dropping anything here means a bad transform cannot be
        # re-run without going back to a source that may no longer hold the data.
        assert medallion.to_bronze(raw).count() == raw.count()

    def test_adds_partition_columns_from_invoice_date(self, raw):
        bronze = medallion.to_bronze(raw)

        assert {"invoice_year", "invoice_month", "ingested_at"} <= set(bronze.columns)
        first = bronze.filter(bronze.InvoiceNo == "536365").first()
        assert (first["invoice_year"], first["invoice_month"]) == (2011, 1)


class TestSilver:
    @pytest.fixture
    def silver(self, raw):
        return medallion.to_silver(medallion.to_bronze(raw))

    def test_drops_cancellations_adjustments_and_noise(self, silver):
        invoices = {r["invoice_no"] for r in silver.collect()}

        assert "C536379" not in invoices, "cancellation survived"
        assert "A563185" not in invoices, "ledger adjustment counted as a sale"
        assert "536380" not in invoices, "zero-quantity line survived"
        assert "536381" not in invoices, "zero-price line survived"

    def test_deduplicates_identical_lines(self, silver):
        # Two identical rows in, one out.
        matching = silver.filter(
            (silver.invoice_no == "536365") & (silver.product_key == "85123A")
        )
        assert matching.count() == 1

    def test_unattributed_rows_are_bucketed_not_dropped(self, silver):
        unknown = silver.filter(silver.customer_key == "UNKNOWN")

        # Dropping these would understate revenue; about a quarter of the real source has no
        # customer attached.
        assert unknown.count() == 1
        assert unknown.first()["invoice_no"] == "536368"

    def test_revenue_is_exact_decimal_arithmetic(self, silver):
        row = silver.filter(silver.product_key == "71053").first()

        # 6 x 3.39 is exactly 20.34. A double would give 20.339999999999996 and the totals
        # would stop reconciling against finance.
        assert row["line_revenue"] == Decimal("20.3400")


class TestGold:
    @pytest.fixture
    def silver(self, raw):
        return medallion.to_silver(medallion.to_bronze(raw))

    def test_daily_sales_aggregates_by_day_and_country(self, silver):
        daily = medallion.gold_daily_sales(silver)

        uk_jan3 = daily.filter(
            (daily.country == "United Kingdom") & (daily.date_key == "2011-01-03")
        ).first()
        # 6 x 2.55 + 6 x 3.39
        assert uk_jan3["revenue"] == Decimal("35.6400")
        assert uk_jan3["invoice_count"] == 1

    def test_rfm_marks_unattributed_separately(self, silver):
        rfm = medallion.gold_customer_rfm(silver)

        segment = rfm.filter(rfm.customer_key == "UNKNOWN").first()["rfm_segment"]
        # UNKNOWN is a bucket, not a person; scoring it would put a phantom at the top of every
        # best-customer list.
        assert segment == "Unattributed"

    def test_rfm_covers_every_customer_exactly_once(self, silver):
        rfm = medallion.gold_customer_rfm(silver)

        assert rfm.count() == rfm.select("customer_key").distinct().count()

    def test_product_description_resolves_deterministically(self, spark, silver):
        products = medallion.gold_product_performance(silver)

        row = products.filter(products.product_key == "85123A").first()
        assert row["product_description"] == "HEART T-LIGHT"
        assert row["avg_selling_price"] == Decimal("2.5500")


class TestRoundTrip:
    def test_layers_survive_a_parquet_round_trip(self, spark, raw, tmp_path):
        # The medallion design rests on each layer being durable and re-readable; chaining
        # DataFrames in memory would never exercise that.
        bronze = medallion.to_bronze(raw)
        path = medallion.write_layer(
            bronze,
            "bronze",
            "transactions",
            partition_by=["invoice_year", "invoice_month"],
            root=str(tmp_path),
        )

        reread = spark.read.parquet(path)
        assert reread.count() == bronze.count()
        # Partition columns come back as columns, not as lost directory names.
        assert {"invoice_year", "invoice_month"} <= set(reread.columns)
