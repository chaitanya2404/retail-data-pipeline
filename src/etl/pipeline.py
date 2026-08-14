"""
pipeline.py
===========

Single runnable entrypoint that orchestrates extract -> transform -> load,
logging row counts and duration for each stage.

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

from src.etl import extract, load, transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    raw_dir: Path = extract.DEFAULT_RAW_DIR,
    db_path: Path = load.DEFAULT_DB_PATH,
    force_download: bool = False,
) -> dict:
    """Run the full ETL pipeline end to end and return a summary dict."""
    pipeline_start = time.perf_counter()
    summary: dict = {}

    # --- Extract ---
    stage_start = time.perf_counter()
    logger.info("=== STAGE 1/3: EXTRACT ===")
    raw_path = extract.download_dataset(dest_dir=raw_dir, force=force_download)
    raw_df = extract.load_raw(raw_path)
    stage_duration = time.perf_counter() - stage_start
    logger.info(
        "Extract complete: %d rows, %d columns in %.2fs",
        raw_df.shape[0],
        raw_df.shape[1],
        stage_duration,
    )
    summary["extract"] = {"rows": len(raw_df), "duration_sec": round(stage_duration, 2)}

    # --- Transform ---
    stage_start = time.perf_counter()
    logger.info("=== STAGE 2/3: TRANSFORM ===")
    clean_df = transform.clean_data(raw_df)
    stage_duration = time.perf_counter() - stage_start
    rows_dropped = len(raw_df) - len(clean_df)
    pct_dropped = 100 * rows_dropped / len(raw_df) if len(raw_df) else 0
    logger.info(
        "Transform complete: %d -> %d rows (%d dropped, %.1f%%) in %.2fs",
        len(raw_df),
        len(clean_df),
        rows_dropped,
        pct_dropped,
        stage_duration,
    )
    summary["transform"] = {
        "rows_in": len(raw_df),
        "rows_out": len(clean_df),
        "rows_dropped": rows_dropped,
        "duration_sec": round(stage_duration, 2),
    }

    # --- Load ---
    stage_start = time.perf_counter()
    logger.info("=== STAGE 3/3: LOAD ===")
    counts = load.load_to_sqlite(clean_df, db_path=db_path)
    stage_duration = time.perf_counter() - stage_start
    logger.info("Load complete: %s in %.2fs", counts, stage_duration)
    summary["load"] = {**counts, "duration_sec": round(stage_duration, 2)}

    total_duration = time.perf_counter() - pipeline_start
    summary["total_duration_sec"] = round(total_duration, 2)
    logger.info("=== PIPELINE COMPLETE in %.2fs === DB at %s", total_duration, db_path)

    return summary


if __name__ == "__main__":
    run_pipeline()
