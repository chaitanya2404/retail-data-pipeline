"""
retail_etl_dag.py
=================

Airflow DAG wrapping the retail ETL pipeline as four independently
retryable tasks:

    extract >> transform >> load >> validate

Design notes
------------
**No ETL logic lives in this file.** Every task is a thin call into
``src.etl.stages``, which is the same code path used by
``python -m src.etl.pipeline``. The DAG contributes scheduling, retries,
dependency ordering and failure semantics -- nothing else.

**Data handoff.** Airflow runs each task in its own process, and XCom is
backed by the metadata database, so it is not a channel for bulk data. Each
task therefore passes only a small JSON-serializable dict (file paths and row
counts) over XCom; the actual 525k-row dataset moves between transform and
load as a Parquet file on disk, and between load and validate via the SQLite
database. See ``src/etl/stages.py`` for the rationale.

**Failure semantics.** Every task uses Airflow's default ``all_success``
trigger rule, so any failed or upstream-failed task leaves the rest of the
chain in ``upstream_failed`` and the DAG run is marked failed. Nothing
downstream of a failure executes -- in particular a failed ``validate`` marks
the whole run failed, which is the point of having it.

**Retries.** Only ``extract`` talks to the network, so it is the task that
gets a real retry budget with exponential backoff. The remaining tasks are
deterministic local compute against files already on disk; retrying them
would usually just re-run a failure, so they get a single retry to absorb
transient filesystem/locking blips.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

# The DAG file lives in <repo>/dags but imports <repo>/src, which is not on
# sys.path when Airflow parses the dags folder. Add the repo root explicitly.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.etl import stages  # noqa: E402  (import must follow the sys.path fix)


@dag(
    dag_id="retail_etl",
    description="Online Retail ETL: download -> clean -> load to SQLite -> validate",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    # The source is a fixed historical archive, so replaying every day since
    # the start_date would re-run identical work. Backfill is off.
    catchup=False,
    # A single run at a time; the load stage replaces tables wholesale, so
    # concurrent runs would race on the same SQLite file.
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["retail", "etl", "portfolio"],
)
def retail_etl():
    """Extract, clean, load and validate the UCI Online Retail dataset."""

    @task(
        # Network-bound: the only task where retrying is genuinely useful.
        # 3 attempts after the first, backing off 30s -> 60s -> 120s, capped
        # at 5 minutes.
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=15),
    )
    def extract() -> dict:
        """Download the raw .xlsx into data/raw/ (idempotent)."""
        return stages.stage_extract()

    @task(execution_timeout=timedelta(minutes=20))
    def transform(extract_result: dict) -> dict:
        """Apply the cleaning rules and write data/interim/clean_retail.parquet."""
        return stages.stage_transform(raw_path=extract_result["raw_path"])

    @task(execution_timeout=timedelta(minutes=20))
    def load(transform_result: dict) -> dict:
        """Load the cleaned Parquet into SQLite (customers/products/transactions)."""
        return stages.stage_load(interim_path=transform_result["interim_path"])

    @task(execution_timeout=timedelta(minutes=10))
    def validate(load_result: dict) -> dict:
        """Run the data-quality suite; raises DataQualityError and fails the run."""
        return stages.stage_validate(db_path=load_result["db_path"])

    # Explicit linear dependency chain: each task consumes the previous task's
    # XCom, which is what establishes the edges in the graph.
    validate(load(transform(extract())))


retail_etl()
