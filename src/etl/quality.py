"""
quality.py
==========

Data-quality checks that run *after* the load stage and fail the pipeline when
the warehouse contents are wrong.

Why hand-rolled instead of Great Expectations / Soda / Pandera
--------------------------------------------------------------
This was an install-compatibility decision, measured rather than assumed. The
project runs on Python 3.14 with pandas 3.0.5 / numpy 2.5.2:

- **Great Expectations** resolves only by *downgrading* pandas to 2.3.3 and
  numpy to 1.26.4. Adding a quality tool that silently regresses the library
  the whole pipeline is built on is a bad trade, so it was rejected.
- **soda-core** resolves but downgrades ``requests``, and is designed around
  YAML scan definitions against a configured warehouse -- a lot of
  configuration surface for eight checks.
- **Pandera** installs cleanly and is a good fit for *column/schema* rules,
  but roughly half of the checks below are cross-table referential integrity
  and aggregate invariants over SQLite, which sit outside its schema model.

The checks here are plain pandas over ~525k rows, which runs in well under a
second, and every check reports the value it actually measured rather than a
bare pass/fail. Each check function is a pure function of its input
DataFrames, which is what makes them unit-testable with small inline frames
(see ``tests/test_quality.py``).

What each check guards against is documented on the individual functions.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# --- Expected bounds -------------------------------------------------------
# The Online Retail dataset is a fixed historical archive covering
# 01/12/2010 - 09/12/2011. The row-count window is deliberately wide enough to
# absorb reasonable variation in the cleaning rules but narrow enough to catch
# a truncated load or a silently empty extract.
MIN_TRANSACTION_ROWS = 400_000
MAX_TRANSACTION_ROWS = 600_000

EXPECTED_MIN_DATE = pd.Timestamp("2010-12-01")
EXPECTED_MAX_DATE = pd.Timestamp("2011-12-31")

# transform.add_total_price rounds to 2dp, so quantity*unit_price can legally
# differ from the stored total by up to half a penny.
TOTAL_PRICE_TOLERANCE = 0.01

# Columns that must never contain a null in the transactions fact table.
# customer_id is intentionally excluded: ~25% of rows are guest checkouts with
# no customer key, which is a documented, expected property of this dataset
# (see transform.py decision #5), not a defect.
NON_NULL_COLUMNS = ["invoice_no", "stock_code", "quantity", "unit_price", "total_price"]


class DataQualityError(AssertionError):
    """Raised when one or more data-quality checks fail."""


@dataclass
class CheckResult:
    """The outcome of a single data-quality check.

    ``actual`` carries the measured value so that a report is informative on
    success as well as failure -- "0 nulls across 5 columns" is a more useful
    log line than "PASS".
    """

    name: str
    passed: bool
    actual: Any
    expected: str
    detail: str = ""

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        line = f"[{status}] {self.name}: actual={self.actual} | expected {self.expected}"
        if self.detail:
            line += f" | {self.detail}"
        return line

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "actual": _jsonable(self.actual),
            "expected": self.expected,
            "detail": self.detail,
        }


@dataclass
class QualityReport:
    """Aggregated results of a full check run."""

    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def ok(self) -> bool:
        return not self.failures

    def render(self) -> str:
        lines = [r.summary() for r in self.results]
        lines.append(
            f"--- {self.passed_count}/{len(self.results)} checks passed "
            f"({len(self.failures)} failed) ---"
        )
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "total_checks": len(self.results),
            "passed": self.passed_count,
            "failed": len(self.failures),
            "results": [r.to_dict() for r in self.results],
        }

    def raise_if_failed(self) -> None:
        if self.failures:
            detail = "\n".join(r.summary() for r in self.failures)
            raise DataQualityError(
                f"{len(self.failures)} data-quality check(s) failed:\n{detail}"
            )


def _jsonable(value: Any) -> Any:
    """Coerce numpy/pandas scalars to plain Python types for XCom/JSON."""
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (bool, str, int, float)):
        return value
    # numpy scalars expose .item()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


# --- Individual checks -----------------------------------------------------


def check_row_count(
    transactions: pd.DataFrame,
    min_rows: int = MIN_TRANSACTION_ROWS,
    max_rows: int = MAX_TRANSACTION_ROWS,
) -> CheckResult:
    """Transaction count is non-zero and within the expected window.

    Guards against: an empty or truncated load (upstream download served an
    error page, transform dropped everything, load wrote a partial table), and
    against a duplicated load roughly doubling the fact table.
    """
    n = len(transactions)
    return CheckResult(
        name="row_count_in_range",
        passed=min_rows <= n <= max_rows,
        actual=n,
        expected=f"between {min_rows:,} and {max_rows:,} rows",
    )


def check_no_nulls(
    transactions: pd.DataFrame, columns: list[str] | None = None
) -> CheckResult:
    """No nulls in columns that must always be populated.

    Guards against: rows that cannot be attributed to an invoice, a product,
    or a price -- which would silently corrupt every downstream aggregate.
    """
    columns = list(NON_NULL_COLUMNS if columns is None else columns)
    present = [c for c in columns if c in transactions.columns]
    missing_cols = [c for c in columns if c not in transactions.columns]

    null_counts = {c: int(transactions[c].isna().sum()) for c in present}
    total_nulls = sum(null_counts.values())
    offenders = {c: n for c, n in null_counts.items() if n > 0}

    detail = ""
    if missing_cols:
        detail = f"columns absent from table: {missing_cols}"
    elif offenders:
        detail = f"columns with nulls: {offenders}"

    return CheckResult(
        name="no_nulls_in_required_columns",
        passed=total_nulls == 0 and not missing_cols,
        actual=f"{total_nulls} nulls across {len(present)} columns {list(present)}",
        expected="0 nulls in invoice_no, stock_code, quantity, unit_price, total_price",
        detail=detail,
    )


def check_no_negative_values(transactions: pd.DataFrame) -> CheckResult:
    """No non-positive quantities or unit prices survived cleaning.

    Guards against: the cleaning rules silently failing to apply (cancelled
    orders, refunds, and zero-price stock adjustments leaking into the fact
    table would understate or distort revenue).
    """
    bad_qty = int((transactions["quantity"] <= 0).sum())
    bad_price = int((transactions["unit_price"] <= 0).sum())
    total = bad_qty + bad_price

    return CheckResult(
        name="no_non_positive_quantity_or_price",
        passed=total == 0,
        actual=f"{bad_qty} rows with quantity<=0, {bad_price} rows with unit_price<=0",
        expected="0 rows with non-positive quantity or unit_price",
    )


def check_customer_referential_integrity(
    transactions: pd.DataFrame, customers: pd.DataFrame
) -> CheckResult:
    """Every non-null transactions.customer_id exists in customers.

    Guards against: a broken dimension build producing orphaned facts, which
    would make customer-level analysis (RFM) silently drop or misattribute
    revenue. Null customer_id is allowed by design (guest checkouts).
    """
    known = set(customers["customer_id"].dropna().astype("int64").tolist())
    txn_ids = transactions["customer_id"].dropna()
    txn_ids = set(txn_ids.astype("int64").tolist()) if len(txn_ids) else set()

    orphans = sorted(txn_ids - known)
    n_orphan_rows = 0
    if orphans:
        mask = transactions["customer_id"].isin(orphans)
        n_orphan_rows = int(mask.sum())

    return CheckResult(
        name="fk_transactions_customer_id",
        passed=not orphans,
        actual=(
            f"{len(orphans)} orphan customer_id values "
            f"({n_orphan_rows} rows); {len(txn_ids)} distinct ids checked "
            f"against {len(known)} customers"
        ),
        expected="every non-null customer_id present in customers",
        detail=f"first orphans: {orphans[:5]}" if orphans else "",
    )


def check_product_referential_integrity(
    transactions: pd.DataFrame, products: pd.DataFrame
) -> CheckResult:
    """Every transactions.stock_code exists in products.

    Guards against: orphaned product references, which would break product-level
    reporting and any join to the product dimension.
    """
    known = set(products["stock_code"].astype(str).tolist())
    txn_codes = set(transactions["stock_code"].astype(str).tolist())

    orphans = sorted(txn_codes - known)
    n_orphan_rows = 0
    if orphans:
        n_orphan_rows = int(transactions["stock_code"].astype(str).isin(orphans).sum())

    return CheckResult(
        name="fk_transactions_stock_code",
        passed=not orphans,
        actual=(
            f"{len(orphans)} orphan stock_code values "
            f"({n_orphan_rows} rows); {len(txn_codes)} distinct codes checked "
            f"against {len(known)} products"
        ),
        expected="every stock_code present in products",
        detail=f"first orphans: {orphans[:5]}" if orphans else "",
    )


def check_revenue_sane(transactions: pd.DataFrame) -> CheckResult:
    """Total revenue is finite and strictly positive.

    Guards against: NaN/inf contamination in total_price propagating into
    headline figures (a single inf would make the reported revenue meaningless).
    """
    total = float(transactions["total_price"].sum())
    finite = math.isfinite(total)
    return CheckResult(
        name="revenue_finite_and_positive",
        passed=finite and total > 0,
        actual=round(total, 2) if finite else str(total),
        expected="finite total revenue > 0",
    )


def check_total_price_consistency(
    transactions: pd.DataFrame, tolerance: float = TOTAL_PRICE_TOLERANCE
) -> CheckResult:
    """total_price equals quantity * unit_price within a float tolerance.

    Guards against: a derived column drifting out of sync with its inputs --
    e.g. a partial reload where quantity was updated but total_price was not.
    """
    expected_total = transactions["quantity"] * transactions["unit_price"]
    diff = (transactions["total_price"] - expected_total).abs()
    mismatches = int((diff > tolerance).sum())
    max_diff = float(diff.max()) if len(diff) else 0.0

    return CheckResult(
        name="total_price_matches_quantity_times_price",
        passed=mismatches == 0,
        actual=f"{mismatches} rows exceed tolerance; max abs diff {max_diff:.6f}",
        expected=f"|total_price - quantity*unit_price| <= {tolerance}",
    )


def check_invoice_date_range(
    transactions: pd.DataFrame,
    min_date: pd.Timestamp = EXPECTED_MIN_DATE,
    max_date: pd.Timestamp = EXPECTED_MAX_DATE,
    now: pd.Timestamp | None = None,
) -> CheckResult:
    """All invoice dates fall inside the dataset window and none are in the future.

    Guards against: date parsing errors (epoch-zero or year-1970 rows), and
    against a future-dated row indicating a clock or timezone bug.

    Note this is a *range* check, not a conventional freshness check. The
    Online Retail dataset is a fixed 2010-2011 archive, so asserting "data is
    recent" would fail by construction and would be a dishonest check to ship.
    The meaningful invariants for a static source are that every date sits in
    the expected historical window and that nothing is dated in the future.

    Unparseable dates (NaT) are counted and failed explicitly. This matters:
    comparisons against NaT evaluate to False, so a NaT row would otherwise
    slip through every range comparison below and be reported as in-window --
    the exact parsing failure this check exists to catch.
    """
    dates = pd.to_datetime(transactions["invoice_date"], errors="coerce")
    now = pd.Timestamp.now() if now is None else now

    unparseable = int(dates.isna().sum())
    observed_min = dates.min()
    observed_max = dates.max()
    before = int((dates < min_date).sum())
    after = int((dates > max_date).sum())
    future = int((dates > now).sum())

    return CheckResult(
        name="invoice_date_within_expected_window",
        passed=(before == 0 and after == 0 and future == 0 and unparseable == 0),
        actual=(
            f"min={observed_min}, max={observed_max}; "
            f"{before} before window, {after} after window, {future} in the future, "
            f"{unparseable} unparseable/null"
        ),
        expected=(
            f"all invoice_date parseable and between {min_date.date()} and "
            f"{max_date.date()}, none in the future"
        ),
    )


# --- Orchestration of the suite -------------------------------------------


def run_all_checks(
    transactions: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    min_rows: int = MIN_TRANSACTION_ROWS,
    max_rows: int = MAX_TRANSACTION_ROWS,
) -> QualityReport:
    """Run every check and collect the results into a report.

    All checks run even if an early one fails, so a single run reports every
    problem rather than only the first.

    ``min_rows``/``max_rows`` are parameterised so the suite can be pointed at
    a smaller dataset (or a test fixture) without editing module constants.
    """
    results = [
        check_row_count(transactions, min_rows=min_rows, max_rows=max_rows),
        check_no_nulls(transactions),
        check_no_negative_values(transactions),
        check_customer_referential_integrity(transactions, customers),
        check_product_referential_integrity(transactions, products),
        check_revenue_sane(transactions),
        check_total_price_consistency(transactions),
        check_invoice_date_range(transactions),
    ]
    return QualityReport(results=results)


def read_tables(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read the three tables out of SQLite for validation."""
    if not Path(db_path).exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. Run the load stage first."
        )
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        transactions = pd.read_sql("SELECT * FROM transactions", conn)
        customers = pd.read_sql("SELECT * FROM customers", conn)
        products = pd.read_sql("SELECT * FROM products", conn)

    # SQLite has no native datetime type, so invoice_date comes back as text.
    # Parse with format="mixed" rather than read_sql(parse_dates=...): the
    # latter infers a single format from the leading rows and silently coerces
    # every row that does not match it to NaT, which would hide out-of-range
    # dates from check_invoice_date_range. errors="coerce" keeps genuinely
    # unparseable values as NaT so that check can report them as failures.
    transactions["invoice_date"] = pd.to_datetime(
        transactions["invoice_date"], format="mixed", errors="coerce"
    )
    return transactions, customers, products


def validate_database(
    db_path: Path,
    raise_on_failure: bool = True,
    min_rows: int = MIN_TRANSACTION_ROWS,
    max_rows: int = MAX_TRANSACTION_ROWS,
) -> QualityReport:
    """Read the loaded tables and run the full quality suite against them.

    Parameters
    ----------
    db_path : Path
        SQLite database produced by the load stage.
    raise_on_failure : bool
        If True (default) a failing check raises ``DataQualityError``, which
        is what fails the orchestrator task and therefore the pipeline run.
    min_rows, max_rows : int
        Expected bounds for the transactions row count.
    """
    transactions, customers, products = read_tables(Path(db_path))
    logger.info(
        "Validating %s: %d transactions, %d customers, %d products",
        db_path,
        len(transactions),
        len(customers),
        len(products),
    )

    report = run_all_checks(
        transactions, customers, products, min_rows=min_rows, max_rows=max_rows
    )
    for line in report.render().splitlines():
        logger.info(line)

    if raise_on_failure:
        report.raise_if_failed()
    return report


if __name__ == "__main__":
    from src.etl.load import DEFAULT_DB_PATH

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    validate_database(DEFAULT_DB_PATH)
