"""Ingestion-time data quality: schema contracts, drift detection and expectation suites.

Separate from :mod:`src.etl.quality`, which asserts business rules on the *cleaned* data after
transformation. This package runs earlier and asks a different question — not "is the output
right" but "is this batch even the dataset we agreed to receive".

The two halves have deliberately different dependency weights:

* Contracts and drift detection are pure pandas. They always import, on any interpreter the
  pipeline supports.
* The expectation suite needs Great Expectations, which declares ``>=3.10,<3.14`` while the rest
  of this project runs on 3.14. Importing it eagerly here would make ``import src.etl.stages``
  fail on the primary interpreter and take the entire ETL down for the sake of an optional check.

So ``great_expectations`` is imported lazily and :data:`EXPECTATIONS_AVAILABLE` says whether it
worked. Callers screen for structural drift unconditionally and add the expectation suite when it
is installed.
"""

from src.etl.validation.contract import (  # noqa: F401
    ColumnContract,
    DriftFinding,
    DriftReport,
    DriftSeverity,
    SchemaContract,
    SchemaDriftError,
    TypeKind,
    check_contract,
)

try:  # pragma: no cover - exercised by whether GX is installed, not by a branch test
    from src.etl.validation.expectations import (  # noqa: F401
        DataValidationError,
        ExpectationOutcome,
        ValidationOutcome,
        build_suite,
        validate_batch,
    )

    EXPECTATIONS_AVAILABLE = True
except ImportError:  # pragma: no cover
    EXPECTATIONS_AVAILABLE = False

    class DataValidationError(RuntimeError):  # type: ignore[no-redef]
        """Placeholder so callers can still reference the type in an except clause."""

    def validate_batch(*_args, **_kwargs):  # type: ignore[no-redef]
        raise ImportError(
            "great_expectations is not installed. Contract and drift checks still run; "
            "install requirements-validation.txt on Python 3.12 to enable the expectation suite."
        )


__all__ = [
    "ColumnContract",
    "DriftFinding",
    "DriftReport",
    "DriftSeverity",
    "SchemaContract",
    "SchemaDriftError",
    "TypeKind",
    "check_contract",
    "DataValidationError",
    "ValidationOutcome",
    "build_suite",
    "validate_batch",
    "EXPECTATIONS_AVAILABLE",
]
