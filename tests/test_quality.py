"""
Unit tests for src/etl/quality.py data-quality checks.

Every check is exercised with both a passing and a deliberately-failing
inline DataFrame, so the suite proves the checks actually *catch* bad data
rather than merely returning PASS on good data. Frames are tiny (a handful of
rows) and mirror the loaded SQL schema column names, not the raw Excel ones.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.etl.quality import (
    DataQualityError,
    QualityReport,
    check_customer_referential_integrity,
    check_invoice_date_range,
    check_no_negative_values,
    check_no_nulls,
    check_product_referential_integrity,
    check_revenue_sane,
    check_row_count,
    check_total_price_consistency,
    run_all_checks,
    validate_database,
)


def make_txn(**overrides) -> dict:
    """One transactions-schema row with valid defaults, overridable per test."""
    row = {
        "invoice_no": "536365",
        "stock_code": "85123A",
        "customer_id": 17850,
        "quantity": 6,
        "unit_price": 2.55,
        "total_price": 15.30,
        "invoice_date": pd.Timestamp("2010-12-01 08:26:00"),
        "country": "United Kingdom",
        "is_guest": False,
    }
    row.update(overrides)
    return row


def make_txns(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def make_customers(ids=(17850,)) -> pd.DataFrame:
    return pd.DataFrame(
        {"customer_id": list(ids), "country": ["United Kingdom"] * len(ids)}
    )


def make_products(codes=("85123A",)) -> pd.DataFrame:
    return pd.DataFrame({"stock_code": list(codes), "description": ["MUG"] * len(codes)})


def run_checks(transactions, customers=None, products=None) -> QualityReport:
    """``run_all_checks`` with row-count bounds sized for these tiny fixtures.

    The production bounds (400k-600k) would fail every fixture here, so the
    suite is pointed at a small window instead. Everything else is the real
    production check set.
    """
    return run_all_checks(
        transactions,
        make_customers() if customers is None else customers,
        make_products() if products is None else products,
        min_rows=1,
        max_rows=100,
    )


class TestCheckRowCount:
    def test_passes_within_range(self):
        df = make_txns([make_txn()] * 5)
        result = check_row_count(df, min_rows=1, max_rows=10)

        assert result.passed
        assert result.actual == 5

    def test_fails_on_empty_table(self):
        df = make_txns([make_txn()]).iloc[0:0]
        result = check_row_count(df, min_rows=1, max_rows=10)

        assert not result.passed
        assert result.actual == 0

    def test_fails_when_above_upper_bound(self):
        df = make_txns([make_txn()] * 20)
        result = check_row_count(df, min_rows=1, max_rows=10)

        assert not result.passed
        assert result.actual == 20

    def test_reports_actual_value_not_just_pass_fail(self):
        df = make_txns([make_txn()] * 3)
        result = check_row_count(df, min_rows=1, max_rows=10)

        assert "3" in str(result.actual)
        assert "between" in result.expected


class TestCheckNoNulls:
    def test_passes_when_required_columns_populated(self):
        df = make_txns([make_txn(), make_txn(invoice_no="536366")])
        result = check_no_nulls(df)

        assert result.passed
        assert "0 nulls" in str(result.actual)

    def test_allows_null_customer_id_for_guest_checkouts(self):
        # ~25% of the real dataset has no customer key; this is expected.
        df = make_txns([make_txn(customer_id=None, is_guest=True)])
        result = check_no_nulls(df)

        assert result.passed

    def test_fails_on_null_invoice_no(self):
        df = make_txns([make_txn(), make_txn(invoice_no=None)])
        result = check_no_nulls(df)

        assert not result.passed
        assert "invoice_no" in result.detail

    def test_fails_on_null_quantity_and_price(self):
        df = make_txns([make_txn(quantity=None), make_txn(unit_price=None)])
        result = check_no_nulls(df)

        assert not result.passed
        assert "quantity" in result.detail
        assert "unit_price" in result.detail

    def test_fails_when_required_column_missing_entirely(self):
        df = make_txns([make_txn()]).drop(columns=["stock_code"])
        result = check_no_nulls(df)

        assert not result.passed
        assert "absent" in result.detail


class TestCheckNoNegativeValues:
    def test_passes_on_positive_quantity_and_price(self):
        df = make_txns([make_txn(), make_txn(quantity=1, unit_price=0.01)])
        result = check_no_negative_values(df)

        assert result.passed

    def test_fails_on_negative_quantity(self):
        df = make_txns([make_txn(), make_txn(quantity=-5)])
        result = check_no_negative_values(df)

        assert not result.passed
        assert "1 rows with quantity<=0" in str(result.actual)

    def test_fails_on_zero_price(self):
        df = make_txns([make_txn(unit_price=0.0)])
        result = check_no_negative_values(df)

        assert not result.passed
        assert "1 rows with unit_price<=0" in str(result.actual)


class TestCustomerReferentialIntegrity:
    def test_passes_when_all_customer_ids_known(self):
        df = make_txns([make_txn(customer_id=17850), make_txn(customer_id=17851)])
        result = check_customer_referential_integrity(df, make_customers((17850, 17851)))

        assert result.passed

    def test_passes_when_customer_id_is_null(self):
        df = make_txns([make_txn(customer_id=None, is_guest=True)])
        result = check_customer_referential_integrity(df, make_customers((17850,)))

        assert result.passed

    def test_fails_on_orphan_customer_id(self):
        df = make_txns([make_txn(customer_id=99999)])
        result = check_customer_referential_integrity(df, make_customers((17850,)))

        assert not result.passed
        assert "99999" in result.detail


class TestProductReferentialIntegrity:
    def test_passes_when_all_stock_codes_known(self):
        df = make_txns([make_txn(stock_code="85123A"), make_txn(stock_code="22423")])
        result = check_product_referential_integrity(
            df, make_products(("85123A", "22423"))
        )

        assert result.passed

    def test_fails_on_orphan_stock_code(self):
        df = make_txns([make_txn(stock_code="NOT_A_PRODUCT")])
        result = check_product_referential_integrity(df, make_products(("85123A",)))

        assert not result.passed
        assert "NOT_A_PRODUCT" in result.detail


class TestRevenueSane:
    def test_passes_on_positive_finite_revenue(self):
        df = make_txns([make_txn(), make_txn()])
        result = check_revenue_sane(df)

        assert result.passed
        assert result.actual == pytest.approx(30.60)

    def test_fails_on_infinite_revenue(self):
        df = make_txns([make_txn(total_price=np.inf)])
        result = check_revenue_sane(df)

        assert not result.passed

    def test_fails_on_nan_revenue(self):
        df = make_txns([make_txn(total_price=np.nan)])
        result = check_revenue_sane(df)

        assert not result.passed

    def test_fails_on_zero_revenue(self):
        df = make_txns([make_txn(total_price=0.0)])
        result = check_revenue_sane(df)

        assert not result.passed


class TestTotalPriceConsistency:
    def test_passes_when_total_equals_quantity_times_price(self):
        df = make_txns([make_txn(quantity=6, unit_price=2.55, total_price=15.30)])
        result = check_total_price_consistency(df)

        assert result.passed

    def test_passes_within_rounding_tolerance(self):
        # transform rounds to 2dp, so a half-penny difference is legal.
        df = make_txns([make_txn(quantity=3, unit_price=4.955, total_price=14.87)])
        result = check_total_price_consistency(df)

        assert result.passed

    def test_fails_when_total_price_drifted(self):
        df = make_txns([make_txn(quantity=6, unit_price=2.55, total_price=99.99)])
        result = check_total_price_consistency(df)

        assert not result.passed
        assert "1 rows exceed tolerance" in str(result.actual)


class TestInvoiceDateRange:
    def test_passes_inside_expected_window(self):
        df = make_txns([make_txn(invoice_date=pd.Timestamp("2011-06-15"))])
        result = check_invoice_date_range(df)

        assert result.passed

    def test_fails_on_date_before_window(self):
        # Classic epoch/parse failure signature.
        df = make_txns([make_txn(invoice_date=pd.Timestamp("1970-01-01"))])
        result = check_invoice_date_range(df)

        assert not result.passed
        assert "1 before window" in str(result.actual)

    def test_fails_on_date_after_window(self):
        df = make_txns([make_txn(invoice_date=pd.Timestamp("2012-05-01"))])
        result = check_invoice_date_range(df)

        assert not result.passed
        assert "1 after window" in str(result.actual)

    def test_fails_on_unparseable_date(self):
        # Regression: NaT compares False against every bound, so before the
        # explicit null count a NaT row was reported as in-window.
        df = make_txns([make_txn(invoice_date=pd.NaT)])
        result = check_invoice_date_range(df)

        assert not result.passed
        assert "1 unparseable/null" in str(result.actual)

    def test_fails_on_garbage_date_string(self):
        df = make_txns([make_txn(invoice_date="not-a-date")])
        result = check_invoice_date_range(df)

        assert not result.passed
        assert "unparseable" in str(result.actual)

    def test_fails_on_future_date(self):
        future = pd.Timestamp.now() + pd.Timedelta(days=365)
        df = make_txns([make_txn(invoice_date=future)])
        result = check_invoice_date_range(
            df, min_date=pd.Timestamp("2010-01-01"), max_date=future + pd.Timedelta(days=1)
        )

        assert not result.passed
        assert "1 in the future" in str(result.actual)


class TestRunAllChecksAndReport:
    def test_all_checks_pass_on_clean_data(self):
        df = make_txns([make_txn(), make_txn(invoice_no="536366")])
        report = run_checks(df)

        assert report.ok
        assert report.passed_count == len(report.results) == 8
        assert report.failures == []

    def test_report_collects_every_failure_not_just_the_first(self):
        # Two independent problems: orphan stock code AND a negative quantity.
        df = make_txns([make_txn(stock_code="GHOST", quantity=-1, total_price=-2.55)])
        report = run_checks(df)

        failed_names = {r.name for r in report.failures}
        assert "fk_transactions_stock_code" in failed_names
        assert "no_non_positive_quantity_or_price" in failed_names
        assert len(report.failures) >= 2

    def test_raise_if_failed_raises_on_bad_data(self):
        df = make_txns([make_txn(quantity=-1)])
        report = run_checks(df)

        with pytest.raises(DataQualityError) as exc:
            report.raise_if_failed()
        assert "no_non_positive_quantity_or_price" in str(exc.value)

    def test_raise_if_failed_is_silent_on_good_data(self):
        df = make_txns([make_txn()])
        report = run_checks(df)

        report.raise_if_failed()  # must not raise

    def test_report_is_json_serializable_for_xcom(self):
        import json

        df = make_txns([make_txn()])
        report = run_checks(df)

        # Airflow XCom must be able to serialize whatever the task returns.
        encoded = json.dumps(report.to_dict())
        assert '"ok": true' in encoded

    def test_render_includes_actual_values(self):
        df = make_txns([make_txn()])
        report = run_checks(df)
        rendered = report.render()

        assert "PASS" in rendered
        assert "8/8 checks passed" in rendered


class TestValidateDatabase:
    def _write_db(self, tmp_path, transactions, customers, products):
        from sqlalchemy import create_engine

        db_path = tmp_path / "test_retail.db"
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            transactions.to_sql("transactions", conn, index=False)
            customers.to_sql("customers", conn, index=False)
            products.to_sql("products", conn, index=False)
        return db_path

    def test_passes_on_a_valid_database(self, tmp_path):
        db_path = self._write_db(
            tmp_path, make_txns([make_txn()]), make_customers(), make_products()
        )
        report = validate_database(db_path, min_rows=1, max_rows=100)

        assert report.ok

    def test_raises_on_an_invalid_database(self, tmp_path):
        db_path = self._write_db(
            tmp_path,
            make_txns([make_txn(quantity=-3)]),
            make_customers(),
            make_products(),
        )
        with pytest.raises(DataQualityError):
            validate_database(db_path, min_rows=1, max_rows=100)

    def test_can_collect_failures_without_raising(self, tmp_path):
        db_path = self._write_db(
            tmp_path,
            make_txns([make_txn(quantity=-3)]),
            make_customers(),
            make_products(),
        )
        report = validate_database(db_path, raise_on_failure=False, min_rows=1, max_rows=100)

        assert not report.ok

    def test_raises_when_database_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            validate_database(tmp_path / "does_not_exist.db")

    def test_out_of_range_date_survives_the_sqlite_round_trip(self, tmp_path):
        # Regression: SQLite stores datetimes as text, and read_sql's
        # parse_dates infers one format from the leading rows and coerces the
        # rest to NaT -- which hid an out-of-window date from the range check.
        rows = [
            make_txn(invoice_date=pd.Timestamp("2010-12-01 08:26:00.000000")),
            make_txn(invoice_no="536999", invoice_date=pd.Timestamp("2031-06-15 10:00:00")),
        ]
        db_path = self._write_db(
            tmp_path, make_txns(rows), make_customers(), make_products()
        )
        report = validate_database(
            db_path, raise_on_failure=False, min_rows=1, max_rows=100
        )

        date_check = next(
            r for r in report.results if r.name == "invoice_date_within_expected_window"
        )
        assert not date_check.passed
        assert "0 unparseable/null" in str(date_check.actual)
        assert "1 after window" in str(date_check.actual)
