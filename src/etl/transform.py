"""
transform.py
============

Cleaning and shaping logic for the raw Online Retail dataset. Every step is a
small, pure, independently testable function that takes a DataFrame and
returns a new DataFrame. ``clean_data`` composes them into the full pipeline.

Documented cleaning decisions
------------------------------
1. **Cancelled orders** (InvoiceNo starting with "C"): these represent order
   cancellations/returns, not completed sales. They are removed from the
   analytical dataset because downstream revenue/RFM analysis targets actual
   completed sales. They are NOT silently mixed in with negative quantities;
   they are identified explicitly by invoice prefix.
2. **Negative or zero Quantity** (after cancellations are already removed):
   any remaining non-positive quantities are treated as data-entry
   adjustments/damages and dropped, since they do not represent a completed
   sale line.
3. **Non-positive UnitPrice**: rows with UnitPrice <= 0 are dropped. These
   are typically stock adjustments, samples, or free items rather than paid
   transactions, and would distort revenue figures.
4. **Missing Description**: rows with a null Description are dropped. In the
   raw data these coincide almost entirely with UnitPrice == 0 adjustment
   entries (bank charges, manual corrections, etc.), so they carry no usable
   product information.
5. **Missing CustomerID**: NOT dropped. ~25% of rows have no CustomerID
   (likely guest/non-account checkouts). These are still real sales and are
   kept for revenue/product/time-series analysis, but are excluded from
   customer-level analysis (e.g. RFM) since there's no customer key to
   aggregate on. A boolean ``is_guest`` flag is added for transparency.
6. **Duplicates**: exact duplicate rows (same invoice, stock code, quantity,
   price, date, customer) are dropped, keeping the first occurrence.
7. **TotalPrice**: computed as ``Quantity * UnitPrice`` per line item.
8. **Whitespace / casing**: Description and Country text fields are
   stripped of leading/trailing whitespace for consistent grouping.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

RAW_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure expected columns exist and have consistent dtypes.

    - InvoiceNo, StockCode -> string
    - Description, Country -> string, stripped of surrounding whitespace
    - InvoiceDate -> datetime64
    - Quantity -> int
    - UnitPrice -> float
    - CustomerID -> nullable Int64 (pandas nullable integer, keeps NaN)
    """
    df = df.copy()

    missing = set(RAW_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Input dataframe is missing expected columns: {missing}")

    df["InvoiceNo"] = df["InvoiceNo"].astype(str).str.strip()
    df["StockCode"] = df["StockCode"].astype(str).str.strip()
    df["Description"] = df["Description"].astype("string").str.strip()
    df["Country"] = df["Country"].astype("string").str.strip()
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce").astype("Int64")

    return df


def drop_missing_description(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with a null/empty Description (see decision #4 in module docstring)."""
    before = len(df)
    out = df[df["Description"].notna() & (df["Description"].str.len() > 0)].copy()
    logger.debug("drop_missing_description: %d -> %d rows", before, len(out))
    return out


def remove_cancelled_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows whose InvoiceNo indicates a cancellation (prefix 'C').

    See decision #1 in module docstring.
    """
    before = len(df)
    is_cancelled = df["InvoiceNo"].astype(str).str.startswith("C")
    out = df[~is_cancelled].copy()
    logger.debug("remove_cancelled_orders: %d -> %d rows", before, len(out))
    return out


def remove_invalid_quantity_price(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with non-positive Quantity or non-positive UnitPrice.

    See decisions #2 and #3 in module docstring. Assumes cancellations have
    already been removed, so any remaining negative quantities are treated
    as adjustments rather than legitimate returns.
    """
    before = len(df)
    out = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()
    logger.debug("remove_invalid_quantity_price: %d -> %d rows", before, len(out))
    return out


def flag_guest_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Add an ``is_guest`` boolean column marking rows with a null CustomerID.

    See decision #5 in module docstring: these rows are kept, not dropped.
    """
    df = df.copy()
    df["is_guest"] = df["CustomerID"].isna()
    return df


def deduplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop exact duplicate transaction rows, keeping the first occurrence."""
    before = len(df)
    subset = ["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID"]
    subset = [c for c in subset if c in df.columns]
    out = df.drop_duplicates(subset=subset, keep="first").copy()
    logger.debug("deduplicate_rows: %d -> %d rows", before, len(out))
    return out


def add_total_price(df: pd.DataFrame) -> pd.DataFrame:
    """Add a TotalPrice column computed as Quantity * UnitPrice."""
    df = df.copy()
    df["TotalPrice"] = (df["Quantity"] * df["UnitPrice"]).round(2)
    return df


def clean_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Run the full cleaning pipeline on a raw Online Retail dataframe.

    Order of operations matters and mirrors the numbered decisions in the
    module docstring: standardize types -> drop missing descriptions ->
    remove cancellations -> remove invalid qty/price -> flag guests ->
    dedupe -> compute TotalPrice.

    Parameters
    ----------
    raw_df : pd.DataFrame
        Raw dataframe as read from the source Excel file.

    Returns
    -------
    Cleaned DataFrame ready to load into the database.
    """
    logger.info("Starting transform on %d raw rows", len(raw_df))

    df = standardize_columns(raw_df)
    df = drop_missing_description(df)
    df = remove_cancelled_orders(df)
    df = remove_invalid_quantity_price(df)
    df = flag_guest_customers(df)
    df = deduplicate_rows(df)
    df = add_total_price(df)
    df = df.reset_index(drop=True)

    logger.info("Transform complete: %d clean rows remain", len(df))
    return df
