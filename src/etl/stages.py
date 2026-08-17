"""
stages.py
=========

Thin, independently-callable wrappers around the ETL steps so that each stage
can run as its own orchestrator task (Airflow) *and* as part of the plain
``python -m src.etl.pipeline`` run, without duplicating logic in either place.

Why this module exists
----------------------
The original ``pipeline.py`` passed DataFrames between stages in memory. That
is fine for a single process, but an orchestrator runs each task in its own
process (and, in a real deployment, potentially on a different machine), so
in-memory handoff is not available. Airflow's XCom is explicitly *not* a data
channel -- it is backed by the metadata database and is meant for small
scalars, so pushing a 525k-row DataFrame through it would be an anti-pattern.

The approach taken here:

- Each stage reads its input from a **file or the database** and writes its
  output to a **file or the database**.
- Each stage returns a small, **JSON-serializable dict** of metadata (paths,
  row counts, durations). That is what travels over XCom.
- ``data/interim/clean_retail.parquet`` is the handoff artifact between
  transform and load. Parquet is used rather than CSV because it round-trips
  the dtypes the pipeline depends on exactly: the nullable ``Int64``
  CustomerID, the ``datetime64`` InvoiceDate, and the boolean ``is_guest``
  flag all survive a write/read cycle, whereas CSV would degrade them to
  strings and force error-prone re-parsing.

Note on stage boundaries: ``stage_extract`` deliberately only *downloads* the
raw file and does not parse it. Reading the 23MB .xlsx with openpyxl is the
single slowest operation in the pipeline, so parsing it in both extract (just
to count rows) and transform would roughly double the runtime for no benefit.
Transform reads it once and reports both the raw and cleaned row counts.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd

from src.etl import extract, load, observability, quality, transform
from src.etl.validation import (
    EXPECTATIONS_AVAILABLE,
    DataValidationError,
    SchemaDriftError,
    check_contract,
    validate_batch,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INTERIM_DIR = REPO_ROOT / "data" / "interim"
DEFAULT_INTERIM_FILENAME = "clean_retail.parquet"


def stage_extract(
    raw_dir: Path | str = extract.DEFAULT_RAW_DIR,
    filename: str = extract.DEFAULT_RAW_FILENAME,
    force_download: bool = False,
) -> dict:
    """Download the raw dataset if it is not already on disk.

    Network-bound stage; this is the one that benefits from orchestrator
    retries. Idempotent -- an existing file is reused unless ``force_download``.

    Returns
    -------
    dict with ``raw_path``, ``size_mb`` and ``duration_sec``.
    """
    start = time.perf_counter()
    raw_path = extract.download_dataset(
        dest_dir=Path(raw_dir), filename=filename, force=force_download
    )
    size_mb = round(raw_path.stat().st_size / (1024 * 1024), 2)
    duration = round(time.perf_counter() - start, 2)

    logger.info("Extract complete: %s (%.2f MB) in %.2fs", raw_path, size_mb, duration)
    return {"raw_path": str(raw_path), "size_mb": size_mb, "duration_sec": duration}


def stage_screen(
    raw_path: Path | str,
    *,
    enforce: bool = True,
) -> dict:
    """Screen the raw batch before a single row is transformed or loaded.

    Placed here rather than after the load on purpose. The existing ``stage_validate`` asks
    whether the warehouse ended up correct; this asks whether the batch was ever fit to process.
    Catching a renamed column or an empty extract at the front door costs one failed task —
    catching it downstream means the load already happened and the dashboards already moved.

    Two independent checks, because they fail differently:

    * The contract catches *structural* change — a column gone, a type switched, nulls where the
      producer promised none. Breaking findings abort; a new column is recorded and waved through.
    * The expectation suite catches *distributional* change — an empty extract, attribution
      collapsing, prices inverting, country cardinality exploding.

    Returns
    -------
    dict with ``drift``, ``expectations``, ``rows`` and ``duration_sec``.
    """
    start = time.perf_counter()

    raw_df = extract.load_raw(Path(raw_path))

    # enforce=False here, then decide below: the expectation suite should still run even when the
    # contract has already failed, so one task reports everything wrong with the batch rather than
    # revealing the next problem only after the first is fixed.
    drift = check_contract(raw_df, enforce=False)

    # Great Expectations pins itself below Python 3.14, which the rest of the pipeline runs on.
    # Structural screening is the part that must never be skipped, so a missing GX degrades this
    # stage rather than failing it — and says so, instead of quietly checking less than it claims.
    expectations = None
    if EXPECTATIONS_AVAILABLE:
        expectations = validate_batch(raw_df, enforce=False)
    else:
        logger.warning(
            "great_expectations unavailable on this interpreter; screening with the schema "
            "contract only. Install requirements-validation.txt on Python 3.12 for the full suite."
        )

    for finding in drift.additive:
        logger.warning("additive schema drift: %s (%s)", finding.column, finding.detail)

    duration = round(time.perf_counter() - start, 2)

    # One event per finding, plus a stage-level summary. Emitted before the enforcement check
    # below so a batch that is about to be rejected still leaves a trail explaining why — the
    # failing run is exactly the one whose telemetry matters.
    expectations_payload = expectations.to_dict() if expectations else {"failures": []}
    observability.emit_quality_findings("screen", drift.to_dict(), expectations_payload)
    observability.emit(
        "screen",
        "failure" if (drift.has_breaking_drift or (expectations and not expectations.success))
        else "success",
        duration_ms=round(duration * 1000, 2),
        metrics={
            "rows": len(raw_df),
            "breaking_drift": len(drift.breaking),
            "additive_drift": len(drift.additive),
            "expectations_run": len(expectations.results) if expectations else 0,
            "expectations_failed": len(expectations.failures) if expectations else 0,
        },
    )

    logger.info(
        "Screen complete: %d rows, %s, %s in %.2fs",
        len(raw_df),
        drift.summary(),
        expectations.summary() if expectations else "expectation suite skipped",
        duration,
    )

    if enforce:
        if drift.has_breaking_drift:
            raise SchemaDriftError(drift)
        if expectations is not None and not expectations.success:
            raise DataValidationError(expectations)

    return {
        "rows": len(raw_df),
        "drift": drift.to_dict(),
        "expectations": expectations.to_dict() if expectations else {"skipped": True},
        "duration_sec": duration,
    }


def stage_transform(
    raw_path: Path | str,
    interim_path: Path | str = DEFAULT_INTERIM_DIR / DEFAULT_INTERIM_FILENAME,
) -> dict:
    """Read the raw file, apply the cleaning rules, and write a Parquet artifact.

    Returns
    -------
    dict with ``interim_path``, ``rows_in``, ``rows_out``, ``rows_dropped``,
    ``pct_dropped`` and ``duration_sec``.
    """
    start = time.perf_counter()
    interim_path = Path(interim_path)
    interim_path.parent.mkdir(parents=True, exist_ok=True)

    raw_df = extract.load_raw(Path(raw_path))
    clean_df = transform.clean_data(raw_df)
    clean_df.to_parquet(interim_path, index=False)

    rows_in = len(raw_df)
    rows_out = len(clean_df)
    rows_dropped = rows_in - rows_out
    pct_dropped = round(100 * rows_dropped / rows_in, 1) if rows_in else 0.0
    duration = round(time.perf_counter() - start, 2)

    logger.info(
        "Transform complete: %d -> %d rows (%d dropped, %.1f%%) in %.2fs -> %s",
        rows_in,
        rows_out,
        rows_dropped,
        pct_dropped,
        duration,
        interim_path,
    )
    return {
        "interim_path": str(interim_path),
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_dropped": rows_dropped,
        "pct_dropped": pct_dropped,
        "duration_sec": duration,
    }


def stage_load(
    interim_path: Path | str = DEFAULT_INTERIM_DIR / DEFAULT_INTERIM_FILENAME,
    db_path: Path | str = load.DEFAULT_DB_PATH,
) -> dict:
    """Read the cleaned Parquet artifact and load it into SQLite.

    Returns
    -------
    dict with per-table row counts and ``duration_sec``.
    """
    start = time.perf_counter()
    interim_path = Path(interim_path)
    if not interim_path.exists():
        raise FileNotFoundError(
            f"Cleaned interim file not found at {interim_path}. Run the transform stage first."
        )

    clean_df = pd.read_parquet(interim_path)
    counts = load.load_to_sqlite(clean_df, db_path=Path(db_path))
    duration = round(time.perf_counter() - start, 2)

    logger.info("Load complete: %s in %.2fs", counts, duration)
    return {**counts, "db_path": str(db_path), "duration_sec": duration}


def stage_validate(db_path: Path | str = load.DEFAULT_DB_PATH) -> dict:
    """Run the data-quality suite against the loaded database.

    Raises
    ------
    quality.DataQualityError
        If any check fails. In an orchestrator this marks the task failed,
        which is the intended "fail the pipeline on bad data" behaviour.

    Returns
    -------
    dict summary of the check run (JSON-serializable, safe for XCom).
    """
    start = time.perf_counter()
    report = quality.validate_database(Path(db_path))
    duration = round(time.perf_counter() - start, 2)

    summary = report.to_dict()
    summary["duration_sec"] = duration
    logger.info(
        "Validate complete: %d/%d checks passed in %.2fs",
        report.passed_count,
        len(report.results),
        duration,
    )
    return summary
