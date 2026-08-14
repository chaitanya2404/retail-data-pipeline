"""
extract.py
==========

Responsible for fetching the raw "Online Retail" dataset (UCI Machine Learning
Repository) into ``data/raw/`` so the pipeline is reproducible from a fresh
clone without shipping the raw file in version control.

Dataset
-------
UCI Online Retail Data Set: real, anonymized transactions from a UK-based,
registered, non-store online retailer between 01/12/2010 and 09/12/2011.
~541,909 rows, 8 columns (InvoiceNo, StockCode, Description, Quantity,
InvoiceDate, UnitPrice, CustomerID, Country).

Source: https://archive.ics.uci.edu/dataset/352/online+retail
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Primary source: UCI ML repository hosted archive.
# Mirror: the same file re-hosted on the UCI static content server, used as a
# fallback if the primary path changes (both point to the identical dataset).
DATASET_URLS = [
    "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx",
    "https://archive.ics.uci.edu/static/public/352/online+retail.zip",
]

DEFAULT_RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
DEFAULT_RAW_FILENAME = "online_retail.xlsx"


def download_dataset(
    dest_dir: Path = DEFAULT_RAW_DIR,
    filename: str = DEFAULT_RAW_FILENAME,
    force: bool = False,
    timeout: int = 120,
) -> Path:
    """Download the raw Online Retail dataset into ``dest_dir``.

    Idempotent: if the destination file already exists and ``force`` is
    False, the download is skipped and the existing path is returned. This
    keeps repeated pipeline runs fast and avoids hammering the UCI server.

    Parameters
    ----------
    dest_dir : Path
        Directory the raw file is written into (created if missing).
    filename : str
        Local filename to save as.
    force : bool
        Re-download even if the file already exists.
    timeout : int
        Per-request timeout in seconds.

    Returns
    -------
    Path to the downloaded (or already-present) raw file.

    Raises
    ------
    RuntimeError if none of the known source URLs succeed.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    if dest_path.exists() and not force:
        logger.info("Raw file already present at %s, skipping download.", dest_path)
        return dest_path

    last_error: Exception | None = None
    for url in DATASET_URLS:
        try:
            logger.info("Attempting download from %s", url)
            response = requests.get(url, timeout=timeout, stream=True)
            response.raise_for_status()

            # The .xlsx source downloads directly. The .zip mirror would need
            # extraction; we only keep it as a documented fallback and raise
            # a clear error if it's ever hit, rather than silently mis-saving
            # a zip file with an .xlsx extension.
            if url.endswith(".zip"):
                raise NotImplementedError(
                    "ZIP mirror selected but extraction is not implemented; "
                    "falling back to next source."
                )

            tmp_path = dest_path.with_suffix(".tmp")
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            tmp_path.rename(dest_path)

            size_mb = dest_path.stat().st_size / (1024 * 1024)
            logger.info("Downloaded raw dataset to %s (%.1f MB)", dest_path, size_mb)
            return dest_path
        except Exception as exc:  # noqa: BLE001 - we want to try all sources
            logger.warning("Download from %s failed: %s", url, exc)
            last_error = exc
            continue

    raise RuntimeError(
        f"Failed to download the Online Retail dataset from all known sources: {last_error}"
    )


def load_raw(raw_path: Path = DEFAULT_RAW_DIR / DEFAULT_RAW_FILENAME) -> pd.DataFrame:
    """Read the raw Excel file into a DataFrame with no transformation applied.

    Parameters
    ----------
    raw_path : Path
        Path to the raw .xlsx file (as produced by ``download_dataset``).

    Returns
    -------
    Raw, untouched DataFrame straight off disk.
    """
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw data file not found at {raw_path}. Run extract.download_dataset() first."
        )
    logger.info("Reading raw data from %s", raw_path)
    df = pd.read_excel(raw_path, engine="openpyxl")
    logger.info("Loaded raw dataframe with shape %s", df.shape)
    return df


def extract(dest_dir: Path = DEFAULT_RAW_DIR, filename: str = DEFAULT_RAW_FILENAME) -> pd.DataFrame:
    """Convenience wrapper: download (if needed) then load the raw dataset."""
    path = download_dataset(dest_dir=dest_dir, filename=filename)
    return load_raw(path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract()
