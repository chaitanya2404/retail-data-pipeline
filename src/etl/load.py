"""
load.py
=======

Loads the cleaned Online Retail DataFrame into a local SQLite database via
SQLAlchemy, using a lightly normalized schema:

- ``customers``    : one row per CustomerID (customer_id, country)
- ``products``      : one row per StockCode (stock_code, description)
- ``transactions``  : one row per order line, foreign-keyed to customers and
  products (nullable customer_id for guest checkouts)

This is deliberately a light normalization (star-schema-ish fact table +
two dimension tables) rather than full 3NF, since the source data doesn't
warrant more complexity than that for a portfolio-scale pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "processed" / "retail.db"

metadata = MetaData()

customers_table = Table(
    "customers",
    metadata,
    Column("customer_id", Integer, primary_key=True),
    Column("country", String, nullable=True),
)

products_table = Table(
    "products",
    metadata,
    Column("stock_code", String, primary_key=True),
    Column("description", String, nullable=True),
)

transactions_table = Table(
    "transactions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("invoice_no", String, nullable=False, index=True),
    Column("stock_code", String, ForeignKey("products.stock_code"), nullable=False, index=True),
    Column("customer_id", Integer, ForeignKey("customers.customer_id"), nullable=True, index=True),
    Column("quantity", Integer, nullable=False),
    Column("unit_price", Float, nullable=False),
    Column("total_price", Float, nullable=False),
    Column("invoice_date", DateTime, nullable=False, index=True),
    Column("country", String, nullable=True),
    Column("is_guest", Boolean, nullable=False),
)


def get_engine(db_path: Path = DEFAULT_DB_PATH) -> Engine:
    """Create (or connect to) the SQLite database at ``db_path``."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")
    return engine


def create_schema(engine: Engine) -> None:
    """Create all tables (customers, products, transactions) if they don't exist."""
    metadata.create_all(engine)
    logger.info("Schema ensured (customers, products, transactions)")


def _build_dim_products(df: pd.DataFrame) -> pd.DataFrame:
    """Build the products dimension table: one row per StockCode.

    When a StockCode has multiple distinct descriptions in the source data
    (a known quirk of this dataset), the most frequent description is kept.
    """
    products = (
        df.groupby("StockCode")["Description"]
        .agg(lambda s: s.value_counts().idxmax() if s.notna().any() else None)
        .reset_index()
        .rename(columns={"StockCode": "stock_code", "Description": "description"})
    )
    return products


def _build_dim_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Build the customers dimension table: one row per non-null CustomerID.

    Country is taken as the most frequent country associated with that
    customer, since a small number of customers have >1 country recorded.
    """
    known = df[df["CustomerID"].notna()]
    customers = (
        known.groupby("CustomerID")["Country"]
        .agg(lambda s: s.value_counts().idxmax() if s.notna().any() else None)
        .reset_index()
        .rename(columns={"CustomerID": "customer_id", "Country": "country"})
    )
    customers["customer_id"] = customers["customer_id"].astype(int)
    return customers


def _build_fact_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Build the transactions fact table matching the SQL schema's column names."""
    out = df.rename(
        columns={
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code",
            "CustomerID": "customer_id",
            "Quantity": "quantity",
            "UnitPrice": "unit_price",
            "TotalPrice": "total_price",
            "InvoiceDate": "invoice_date",
            "Country": "country",
            "is_guest": "is_guest",
        }
    )[
        [
            "invoice_no",
            "stock_code",
            "customer_id",
            "quantity",
            "unit_price",
            "total_price",
            "invoice_date",
            "country",
            "is_guest",
        ]
    ].copy()

    # customer_id must be a plain Python int or None for SQLite, not pandas NA
    out["customer_id"] = out["customer_id"].astype(object).where(out["customer_id"].notna(), None)
    out["customer_id"] = out["customer_id"].apply(lambda v: int(v) if v is not None else None)
    return out


def load_to_sqlite(df: pd.DataFrame, db_path: Path = DEFAULT_DB_PATH, if_exists: str = "replace") -> dict:
    """Load a cleaned DataFrame into the SQLite database as three tables.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned dataframe as produced by ``transform.clean_data``.
    db_path : Path
        Where to write the SQLite database file.
    if_exists : str
        Passed to ``DataFrame.to_sql`` for each table: 'replace' (default,
        makes the load idempotent/rerunnable) or 'append'.

    Returns
    -------
    dict of row counts written per table, e.g.
    {'customers': 4372, 'products': 3684, 'transactions': 392692}
    """
    engine = get_engine(db_path)
    create_schema(engine)

    products = _build_dim_products(df)
    customers = _build_dim_customers(df)
    transactions = _build_fact_transactions(df)

    with engine.begin() as conn:
        products.to_sql("products", conn, if_exists=if_exists, index=False)
        customers.to_sql("customers", conn, if_exists=if_exists, index=False)
        transactions.to_sql("transactions", conn, if_exists=if_exists, index=False)

    counts = {
        "customers": len(customers),
        "products": len(products),
        "transactions": len(transactions),
    }
    logger.info("Loaded to %s: %s", db_path, counts)
    return counts
