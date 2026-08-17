"""Great Expectations suite for the raw retail extract.

Runs at ingestion, before anything is landed. That placement is the whole point: the same checks
run after loading would tell you the warehouse is already wrong, which is a different and much
more expensive conversation than refusing a bad batch at the door.

The context is ephemeral rather than file-backed. A persisted GX project directory carries its own
state, uncommitted local edits and a validation store that drifts between machines; building the
suite in code means the checks live in version control next to the pipeline that runs them, and
CI validates exactly what production will.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import great_expectations as gx
import pandas as pd

logger = logging.getLogger(__name__)

SUITE_NAME = "online_retail_raw"

#: Tolerance for the one column that is legitimately incomplete. Roughly a quarter of the source
#: has no customer id, so a strict not-null check would fail every run for a known, accepted
#: property of the data. 0.60 leaves headroom above the observed ~0.75 completeness while still
#: catching a batch where attribution collapses entirely.
CUSTOMER_ID_MIN_COMPLETENESS = 0.60


@dataclass
class ExpectationOutcome:
    """One expectation's result, flattened into something loggable and JSON-serialisable."""

    expectation: str
    column: str | None
    success: bool
    observed: Any = None

    def to_dict(self) -> dict:
        return {
            "expectation": self.expectation,
            "column": self.column,
            "success": self.success,
            "observed": self.observed,
        }


@dataclass
class ValidationOutcome:
    success: bool
    results: list[ExpectationOutcome] = field(default_factory=list)

    @property
    def failures(self) -> list[ExpectationOutcome]:
        return [r for r in self.results if not r.success]

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "checks_run": len(self.results),
            "failures": [r.to_dict() for r in self.failures],
        }

    def summary(self) -> str:
        passed = len(self.results) - len(self.failures)
        return f"{passed}/{len(self.results)} expectations passed"


class DataValidationError(RuntimeError):
    """Raised when the batch fails validation and the caller asked for enforcement."""

    def __init__(self, outcome: ValidationOutcome):
        self.outcome = outcome
        detail = "; ".join(
            f"{f.expectation}({f.column})" if f.column else f.expectation for f in outcome.failures
        )
        super().__init__(f"data validation failed: {detail}")


def build_suite() -> gx.ExpectationSuite:
    """The expectations that describe a usable raw batch.

    Each one encodes a rule that has a downstream consequence, not merely a property that happens
    to hold today — an expectation nobody would act on is noise that erodes trust in the ones that
    matter.
    """
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # Structure. A batch missing a column cannot be repaired downstream.
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "InvoiceNo",
                "StockCode",
                "Description",
                "Quantity",
                "InvoiceDate",
                "UnitPrice",
                "CustomerID",
                "Country",
            ],
            # Extra columns are tolerated here because the contract check already classifies them
            # as additive; failing twice for one event helps nobody.
            exact_match=False,
        )
    )

    # An empty extract is the most common silent failure in a scheduled pipeline: the job
    # "succeeds", the load truncates, and every dashboard goes to zero.
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    for column in ("InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice", "Country"):
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

    # CustomerID is knowingly incomplete — see the constant above.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="CustomerID", mostly=CUSTOMER_ID_MIN_COMPLETENESS
        )
    )

    # Ranges below are set from what the source actually contains, not from what a retail dataset
    # "should" contain. The first version of this suite used tidy round numbers and failed on
    # every run against two legitimate rows — an expectation that always fails gets muted, and a
    # muted expectation protects nothing.
    #
    # Observed extremes across all 541,909 raw rows:
    #   Quantity  -80,995 .. 80,995   (invoice 581483, a bulk order, and its cancellation C581484)
    #   UnitPrice -11,062.06 .. 38,970 (A563186/A563187 "Adjust bad debt"; "AMAZON FEE" lines)
    #
    # The raw extract is a ledger, not a sales list: it carries cancellations, fee lines and
    # balance adjustments that the transform strips later. The bounds admit those with headroom
    # and still catch an order-of-magnitude change.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Quantity", min_value=-100_000, max_value=100_000
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="UnitPrice", min_value=-20_000, max_value=50_000
        )
    )

    # The economic check the structural bound cannot make. Negative prices are real but should be
    # a rounding error — two rows in half a million. `mostly` tolerates those while still failing
    # a batch where signs invert wholesale, which is what a broken upstream join or a
    # credit/debit mix-up actually looks like.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="UnitPrice", min_value=0, mostly=0.99)
    )

    # Country is a small controlled vocabulary. Cardinality exploding usually means a free-text
    # field replaced a picklist upstream, which breaks every geography grouping downstream.
    suite.add_expectation(
        gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
            column="Country", min_value=1, max_value=80
        )
    )

    return suite


def validate_batch(
    df: pd.DataFrame,
    *,
    enforce: bool = True,
    suite: gx.ExpectationSuite | None = None,
) -> ValidationOutcome:
    """Run the suite against a frame.

    :param enforce: raise on failure. A backfill or profiling run passes False to observe the
        result without stopping.
    """
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas("retail_ingestion")
    asset = data_source.add_dataframe_asset("raw_batch")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_batch")

    registered_suite = context.suites.add(suite or build_suite())
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"{SUITE_NAME}_validation", data=batch_definition, suite=registered_suite
        )
    )

    raw_result = validation_definition.run(batch_parameters={"dataframe": df})

    outcome = ValidationOutcome(
        success=bool(raw_result.success),
        results=[
            ExpectationOutcome(
                expectation=result.expectation_config.type,
                column=result.expectation_config.kwargs.get("column"),
                success=bool(result.success),
                observed=result.result.get("observed_value")
                if result.result.get("observed_value") is not None
                else result.result.get("unexpected_percent"),
            )
            for result in raw_result.results
        ],
    )

    logger.info("great expectations: %s", outcome.summary())
    for failure in outcome.failures:
        logger.error(
            "expectation failed: %s on %s (observed %s)",
            failure.expectation,
            failure.column,
            failure.observed,
        )

    if enforce and not outcome.success:
        raise DataValidationError(outcome)

    return outcome
