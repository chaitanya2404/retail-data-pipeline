"""Tests for ingestion-time validation: schema contracts, drift severity and the GX suite.

Every frame here is built inline, so the suite needs no network and no source dataset — the same
constraint the existing tests hold to.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.etl.validation.contract import (
    ColumnContract,
    DriftSeverity,
    SchemaContract,
    SchemaDriftError,
    TypeKind,
    check_contract,
)
from src.etl.validation import EXPECTATIONS_AVAILABLE

# Great Expectations pins itself below Python 3.14, which this project's primary interpreter is.
# The contract tests above are pure pandas and must run everywhere; only the suite below needs GX,
# so it is skipped rather than failing the whole file on an interpreter GX does not support.
gx_required = pytest.mark.skipif(
    not EXPECTATIONS_AVAILABLE,
    reason="great_expectations is not installed (requires Python < 3.14)",
)

if EXPECTATIONS_AVAILABLE:
    from src.etl.validation.expectations import DataValidationError, validate_batch


def _valid_batch(rows: int = 4) -> pd.DataFrame:
    """A batch that satisfies both the contract and the expectation suite."""
    return pd.DataFrame(
        {
            "InvoiceNo": [f"53636{i}" for i in range(rows)],
            "StockCode": ["85123A", "71053", "84406B", "22423"][:rows],
            "Description": ["HEART T-LIGHT", "METAL LANTERN", "COAT HANGER", "CAKESTAND"][:rows],
            "Quantity": [6, 6, 8, 2][:rows],
            "InvoiceDate": pd.to_datetime(
                ["2011-01-03", "2011-01-03", "2011-01-10", "2011-02-14"][:rows]
            ),
            "UnitPrice": [2.55, 3.39, 2.75, 12.75][:rows],
            "CustomerID": [17850.0, 17850.0, None, 12583.0][:rows],
            "Country": ["United Kingdom", "United Kingdom", "France", "France"][:rows],
        }
    )


def _contract() -> SchemaContract:
    return SchemaContract.load()


class TestSchemaDrift:
    def test_matching_batch_reports_no_drift(self):
        report = check_contract(_valid_batch(), _contract())

        assert report.findings == []
        assert not report.has_breaking_drift

    def test_new_column_is_additive_not_breaking(self):
        batch = _valid_batch()
        batch["PromotionCode"] = "SPRING24"

        report = check_contract(batch, _contract())

        # The pipeline must not fail here. Nothing downstream referenced this column yesterday,
        # and blocking on it teaches people to disable the check.
        assert not report.has_breaking_drift
        assert [f.column for f in report.additive] == ["PromotionCode"]
        assert report.additive[0].severity is DriftSeverity.ADDITIVE

    def test_missing_column_is_breaking(self):
        batch = _valid_batch().drop(columns=["Country"])

        with pytest.raises(SchemaDriftError) as excinfo:
            check_contract(batch, _contract())

        assert "Country" in str(excinfo.value)

    def test_retyped_column_is_breaking(self):
        # The quiet killer: an id arriving as text still loads and still joins to nothing.
        batch = _valid_batch()
        batch["Quantity"] = batch["Quantity"].astype(str)

        report = check_contract(batch, _contract(), enforce=False)

        assert report.has_breaking_drift
        finding = next(f for f in report.breaking if f.column == "Quantity")
        assert finding.kind == "type_change"
        assert "integer" in finding.detail and "string" in finding.detail

    def test_integer_where_float_expected_is_tolerated(self):
        # Widening loses nothing. UnitPrice arriving as whole numbers is not an incident.
        batch = _valid_batch()
        batch["UnitPrice"] = [3, 4, 5, 13]

        report = check_contract(batch, _contract())

        assert not report.has_breaking_drift

    def test_nulls_in_non_nullable_column_are_breaking(self):
        batch = _valid_batch()
        batch.loc[0, "Country"] = None

        report = check_contract(batch, _contract(), enforce=False)

        assert report.has_breaking_drift
        assert any(f.kind == "unexpected_nulls" for f in report.breaking)

    def test_nulls_in_nullable_column_are_fine(self):
        # CustomerID is knowingly incomplete; flagging it would fail every real batch.
        batch = _valid_batch()
        batch.loc[0, "CustomerID"] = None

        report = check_contract(batch, _contract(), enforce=False)

        assert not any(f.column == "CustomerID" for f in report.breaking)

    def test_all_null_column_is_not_reported_as_a_type_change(self):
        # An entirely empty column has no observable type: pandas makes it float64 or object
        # depending on construction, and calling either "drift" is a false positive. This caught
        # a real bug — the first version reported CustomerID as string-instead-of-float whenever
        # a batch happened to have no attribution at all.
        batch = _valid_batch()
        batch["CustomerID"] = None

        report = check_contract(batch, _contract(), enforce=False)

        assert not any(
            f.column == "CustomerID" and f.kind == "type_change" for f in report.findings
        )

    def test_all_null_non_nullable_column_is_still_breaking(self):
        # Skipping the type check must not swallow the finding that matters.
        batch = _valid_batch()
        batch["Country"] = None

        report = check_contract(batch, _contract(), enforce=False)

        assert report.has_breaking_drift
        assert any(
            f.column == "Country" and f.kind == "unexpected_nulls" for f in report.breaking
        )

    def test_enforce_false_reports_without_raising(self):
        batch = _valid_batch().drop(columns=["Country"])

        report = check_contract(batch, _contract(), enforce=False)

        assert report.has_breaking_drift
        assert "1 breaking" in report.summary()


class TestContractRoundTrip:
    def test_contract_file_matches_the_shipped_dataset_shape(self):
        contract = _contract()

        assert contract.name == "online_retail"
        assert contract.column_names == (
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        )

    def test_save_and_load_preserves_the_contract(self, tmp_path):
        original = SchemaContract(
            name="probe",
            version=2,
            columns=(ColumnContract(name="a", type_kind=TypeKind.INTEGER, nullable=True),),
        )

        reloaded = SchemaContract.load(original.save(tmp_path / "probe.json"))

        assert reloaded == original

    def test_inferred_contract_marks_complete_columns_non_nullable(self):
        # Documents the known limitation: inference sees one batch, so a column that happens to
        # be complete today is marked non-nullable and would reject the first batch with a gap.
        inferred = SchemaContract.infer(_valid_batch(), name="probe")

        by_name = {c.name: c for c in inferred.columns}
        assert by_name["Country"].nullable is False
        assert by_name["CustomerID"].nullable is True


@gx_required
class TestExpectationSuite:
    def test_valid_batch_passes(self):
        outcome = validate_batch(_valid_batch())

        assert outcome.success
        assert outcome.failures == []

    def test_empty_batch_fails(self):
        # The most common silent failure in a scheduled pipeline: the job "succeeds", the load
        # truncates, and every dashboard reads zero.
        empty = _valid_batch().iloc[0:0]

        with pytest.raises(DataValidationError):
            validate_batch(empty)

    def test_missing_column_fails_the_suite(self):
        batch = _valid_batch().drop(columns=["UnitPrice"])

        outcome = validate_batch(batch, enforce=False)

        assert not outcome.success

    def test_wholly_unattributed_batch_fails_completeness_threshold(self):
        # A quarter missing is normal; all of it missing means attribution broke upstream.
        batch = _valid_batch()
        batch["CustomerID"] = None

        outcome = validate_batch(batch, enforce=False)

        assert not outcome.success
        assert any(f.column == "CustomerID" for f in outcome.failures)

    def test_country_cardinality_explosion_fails(self):
        # Simulates a picklist being replaced with a free-text field upstream.
        batch = _valid_batch(rows=4)
        wide = pd.concat([batch] * 25, ignore_index=True)
        wide["Country"] = [f"Country {i}" for i in range(len(wide))]

        outcome = validate_batch(wide, enforce=False)

        assert not outcome.success
        assert any("unique_value_count" in f.expectation for f in outcome.failures)

    def test_outcome_serialises_for_logging(self):
        outcome = validate_batch(_valid_batch())

        payload = outcome.to_dict()
        assert payload["success"] is True
        assert payload["checks_run"] > 0
