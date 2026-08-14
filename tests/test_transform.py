"""
Unit tests for src/etl/transform.py cleaning functions.

Each test builds a small inline DataFrame that isolates one cleaning
decision (see docstring in transform.py) rather than relying on the full
~540k row dataset, so the suite runs in well under a second.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl.transform import (
    add_total_price,
    clean_data,
    deduplicate_rows,
    drop_missing_description,
    flag_guest_customers,
    remove_cancelled_orders,
    remove_invalid_quantity_price,
    standardize_columns,
)


def make_row(**overrides) -> dict:
    """Build one raw-schema row dict with sensible defaults, overridable per test."""
    row = {
        "InvoiceNo": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": 6,
        "InvoiceDate": "2010-12-01 08:26:00",
        "UnitPrice": 2.55,
        "CustomerID": 17850.0,
        "Country": "United Kingdom",
    }
    row.update(overrides)
    return row


def make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestStandardizeColumns:
    def test_casts_types_correctly(self):
        df = make_df([make_row()])
        out = standardize_columns(df)

        assert pd.api.types.is_datetime64_any_dtype(out["InvoiceDate"])
        assert out["Quantity"].iloc[0] == 6
        assert out["UnitPrice"].iloc[0] == 2.55
        assert out["CustomerID"].iloc[0] == 17850

    def test_strips_whitespace_from_text_fields(self):
        df = make_df([make_row(Description="  MUG  ", Country=" UK ")])
        out = standardize_columns(df)

        assert out["Description"].iloc[0] == "MUG"
        assert out["Country"].iloc[0] == "UK"

    def test_missing_customer_id_becomes_na(self):
        df = make_df([make_row(CustomerID=None)])
        out = standardize_columns(df)

        assert out["CustomerID"].isna().iloc[0]

    def test_raises_on_missing_expected_column(self):
        df = pd.DataFrame([{"InvoiceNo": "1"}])
        with pytest.raises(ValueError):
            standardize_columns(df)


class TestDropMissingDescription:
    def test_drops_null_description(self):
        df = standardize_columns(make_df([make_row(), make_row(Description=None)]))
        out = drop_missing_description(df)
        assert len(out) == 1

    def test_drops_empty_string_description(self):
        df = standardize_columns(make_df([make_row(), make_row(Description="")]))
        out = drop_missing_description(df)
        assert len(out) == 1

    def test_keeps_valid_descriptions(self):
        df = standardize_columns(make_df([make_row(), make_row(InvoiceNo="536366")]))
        out = drop_missing_description(df)
        assert len(out) == 2


class TestRemoveCancelledOrders:
    def test_removes_invoices_prefixed_with_c(self):
        df = standardize_columns(
            make_df([make_row(), make_row(InvoiceNo="C536365", Quantity=-6)])
        )
        out = remove_cancelled_orders(df)
        assert len(out) == 1
        assert not out["InvoiceNo"].str.startswith("C").any()

    def test_keeps_normal_invoices(self):
        df = standardize_columns(make_df([make_row(), make_row(InvoiceNo="536366")]))
        out = remove_cancelled_orders(df)
        assert len(out) == 2


class TestRemoveInvalidQuantityPrice:
    def test_drops_negative_quantity(self):
        df = standardize_columns(make_df([make_row(), make_row(Quantity=-1)]))
        out = remove_invalid_quantity_price(df)
        assert len(out) == 1
        assert (out["Quantity"] > 0).all()

    def test_drops_zero_quantity(self):
        df = standardize_columns(make_df([make_row(), make_row(Quantity=0)]))
        out = remove_invalid_quantity_price(df)
        assert len(out) == 1

    def test_drops_zero_or_negative_price(self):
        df = standardize_columns(
            make_df([make_row(), make_row(UnitPrice=0.0), make_row(UnitPrice=-5.0)])
        )
        out = remove_invalid_quantity_price(df)
        assert len(out) == 1
        assert (out["UnitPrice"] > 0).all()

    def test_keeps_valid_rows(self):
        df = standardize_columns(make_df([make_row(), make_row(InvoiceNo="536366")]))
        out = remove_invalid_quantity_price(df)
        assert len(out) == 2


class TestFlagGuestCustomers:
    def test_flags_missing_customer_id(self):
        df = standardize_columns(make_df([make_row(CustomerID=None)]))
        out = flag_guest_customers(df)
        assert out["is_guest"].iloc[0] == True  # noqa: E712

    def test_does_not_flag_known_customer(self):
        df = standardize_columns(make_df([make_row()]))
        out = flag_guest_customers(df)
        assert out["is_guest"].iloc[0] == False  # noqa: E712


class TestDeduplicateRows:
    def test_drops_exact_duplicate_rows(self):
        df = standardize_columns(make_df([make_row(), make_row()]))
        out = deduplicate_rows(df)
        assert len(out) == 1

    def test_keeps_distinct_rows(self):
        df = standardize_columns(make_df([make_row(), make_row(Quantity=12)]))
        out = deduplicate_rows(df)
        assert len(out) == 2


class TestAddTotalPrice:
    def test_computes_quantity_times_price(self):
        df = standardize_columns(make_df([make_row(Quantity=3, UnitPrice=2.5)]))
        out = add_total_price(df)
        assert out["TotalPrice"].iloc[0] == pytest.approx(7.5)

    def test_rounds_to_two_decimals(self):
        df = standardize_columns(make_df([make_row(Quantity=3, UnitPrice=0.1)]))
        out = add_total_price(df)
        assert out["TotalPrice"].iloc[0] == pytest.approx(0.3, abs=1e-9)


class TestCleanDataIntegration:
    def test_full_pipeline_on_mixed_sample(self):
        rows = [
            make_row(),  # valid
            make_row(InvoiceNo="536366", Quantity=2),  # valid, different invoice
            make_row(InvoiceNo="C536367", Quantity=-6),  # cancelled -> dropped
            make_row(InvoiceNo="536368", Quantity=-1),  # negative qty -> dropped
            make_row(InvoiceNo="536369", UnitPrice=0.0),  # zero price -> dropped
            make_row(InvoiceNo="536370", Description=None),  # missing desc -> dropped
            make_row(InvoiceNo="536371", CustomerID=None),  # guest -> kept
            make_row(),  # exact duplicate of row 1 -> dropped
        ]
        raw = make_df(rows)
        out = clean_data(raw)

        # 3 valid unique rows survive: the original, the qty=2 one, and the guest one
        assert len(out) == 3
        assert "TotalPrice" in out.columns
        assert "is_guest" in out.columns
        assert out["is_guest"].sum() == 1
        assert not out["InvoiceNo"].str.startswith("C").any()
        assert (out["Quantity"] > 0).all()
        assert (out["UnitPrice"] > 0).all()

    def test_raises_on_missing_columns(self):
        with pytest.raises(ValueError):
            clean_data(pd.DataFrame([{"foo": 1}]))
