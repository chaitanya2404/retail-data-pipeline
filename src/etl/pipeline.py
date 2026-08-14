"""
pipeline.py
===========

Single runnable entrypoint that orchestrates extract -> transform -> load ->
validate, logging row counts and duration for each stage.

This module is the *plain Python* runner. The same four stages are also wired
into an Airflow DAG (``dags/retail_etl_dag.py``); both call the identical
functions in ``src.etl.stages``, so there is exactly one implementation of
each stage and the two entrypoints cannot drift apart.

Usage
-----
    python -m src.etl.pipeline

or

    from src.etl.pipeline import run_pipeline
    run_pipeline()
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from src.etl import extract, load, stages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    raw_dir: Path = extract.DEFAULT_RAW_DIR,
    db_path: Path = load.DEFAULT_DB_PATH,
    interim_path: Path = stages.DEFAULT_INTERIM_DIR / stages.DEFAULT_INTERIM_FILENAME,
    force_download: bool = False,
    validate: bool = True,
) -> dict:
    """Run the full ETL pipeline end to end and return a summary dict.

    Parameters
    ----------
    validate : bool
        Run the data-quality suite after loading (default True). A failing
        check raises ``quality.DataQualityError`` and aborts the run.
    """
    pipeline_start = time.perf_counter()
    summary: dict = {}

    # --- Extract ---
    logger.info("=== STAGE 1/4: EXTRACT ===")
    summary["extract"] = stages.stage_extract(
        raw_dir=raw_dir, force_download=force_download
    )

    # --- Transform ---
    logger.info("=== STAGE 2/4: TRANSFORM ===")
    summary["transform"] = stages.stage_transform(
        raw_path=summary["extract"]["raw_path"], interim_path=interim_path
    )

    # --- Load ---
    logger.info("=== STAGE 3/4: LOAD ===")
    summary["load"] = stages.stage_load(interim_path=interim_path, db_path=db_path)

    # --- Validate ---
    if validate:
        logger.info("=== STAGE 4/4: VALIDATE ===")
        summary["validate"] = stages.stage_validate(db_path=db_path)
    else:
        logger.info("=== STAGE 4/4: VALIDATE (skipped) ===")

    total_duration = time.perf_counter() - pipeline_start
    summary["total_duration_sec"] = round(total_duration, 2)
    logger.info("=== PIPELINE COMPLETE in %.2fs === DB at %s", total_duration, db_path)

    return summary


if __name__ == "__main__":
    run_pipeline()
