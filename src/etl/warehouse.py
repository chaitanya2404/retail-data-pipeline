"""Load the cleaned transactions into the Postgres landing zone that dbt models read.

This is deliberately separate from :mod:`src.etl.load`, which builds a small star schema directly
in SQLite. The two exist for different reasons: SQLite keeps the project runnable with no services
at all, while Postgres is where the dbt models are defined and where the dimensional layer is
actually built. The Python side stops at the landing zone — everything downstream of `raw` belongs
to dbt, so transformation logic lives in exactly one place.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
RAW_TABLE = "transactions"

#: Chunk size for the bulk insert. Large enough that round trips are amortised, small enough that
#: a failed load does not sit on one enormous uncommitted transaction.
CHUNK_SIZE = 10_000


def warehouse_url() -> str:
    """Connection URL, assembled from environment with local-development defaults.

    Defaults match ``docker-compose.yml`` so a fresh clone runs without configuration. The port is
    55432 rather than 5432 because a locally installed Postgres very often already holds 5432, and
    that clash surfaces as a confusing authentication error rather than a port conflict.
    """
    return os.environ.get(
        "WAREHOUSE_URL",
        "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
            user=os.environ.get("WAREHOUSE_USER", "retail"),
            password=os.environ.get("WAREHOUSE_PASSWORD", "retail"),
            host=os.environ.get("WAREHOUSE_HOST", "localhost"),
            port=os.environ.get("WAREHOUSE_PORT", "55432"),
            db=os.environ.get("WAREHOUSE_DB", "retail_warehouse"),
        ),
    )


def get_warehouse_engine(url: str | None = None) -> Engine:
    """Engine with pre-ping enabled so a connection idled out by the server is replaced silently."""
    return create_engine(url or warehouse_url(), pool_pre_ping=True, future=True)


def _to_landing_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Rename the cleaned frame to the landing-zone contract declared in ``_sources.yml``."""
    out = df.rename(
        columns={
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "UnitPrice": "unit_price",
            "CustomerID": "customer_id",
            "Country": "country",
        }
    )

    expected = [
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
    ]
    missing = [column for column in expected if column not in out.columns]
    if missing:
        raise ValueError(f"cleaned frame is missing expected columns: {missing}")

    out = out[expected].copy()

    # customer_id is text in the landing zone even though the source holds numbers. The warehouse
    # never does arithmetic on it, and keeping it numeric forces every join to agree on whether
    # 17850 and 17850.0 are the same customer — they are not, to a database.
    out["customer_id"] = (
        out["customer_id"]
        .astype("object")
        .where(out["customer_id"].notna(), None)
        .map(lambda value: None if value is None else str(int(float(value))))
    )

    # Stamped once per batch rather than per row, so source-freshness reflects when the load ran
    # rather than how long it took.
    out["loaded_at"] = datetime.now(timezone.utc)
    return out


def create_landing_schema(engine: Engine) -> None:
    """Create the landing schema and table if they do not exist.

    Typed explicitly instead of letting pandas infer them: inference makes ``quantity`` a bigint on
    one run and an integer on the next depending on the batch, and dbt's casts then behave
    differently between environments.
    """
    with engine.begin() as connection:
        connection.execute(text(f"create schema if not exists {RAW_SCHEMA}"))
        connection.execute(
            text(
                f"""
                create table if not exists {RAW_SCHEMA}.{RAW_TABLE} (
                    invoice_no    varchar(20)   not null,
                    stock_code    varchar(20)   not null,
                    description   text,
                    quantity      integer       not null,
                    invoice_date  timestamp     not null,
                    unit_price    numeric(12,4) not null,
                    customer_id   varchar(20),
                    country       varchar(64)   not null,
                    loaded_at     timestamptz   not null
                )
                """
            )
        )
        # The dbt models filter and group by these; without them every staging view build is a
        # sequential scan of the full landing table.
        connection.execute(
            text(
                f"create index if not exists {RAW_TABLE}_invoice_date_idx "
                f"on {RAW_SCHEMA}.{RAW_TABLE} (invoice_date)"
            )
        )
        connection.execute(
            text(
                f"create index if not exists {RAW_TABLE}_loaded_at_idx "
                f"on {RAW_SCHEMA}.{RAW_TABLE} (loaded_at)"
            )
        )


def load_to_warehouse(
    df: pd.DataFrame,
    engine: Engine | None = None,
    *,
    truncate: bool = True,
) -> dict:
    """Land the cleaned transactions in Postgres.

    :param truncate: replace the landing zone rather than appending. Full-refresh is correct for
        this dataset — the source is a single static extract, and appending on every run would
        silently double the row count and every revenue figure derived from it.
    :returns: a summary suitable for logging or pushing through an Airflow XCom.
    """
    engine = engine or get_warehouse_engine()
    landing = _to_landing_frame(df)

    create_landing_schema(engine)

    if truncate:
        with engine.begin() as connection:
            connection.execute(text(f"truncate table {RAW_SCHEMA}.{RAW_TABLE}"))

    landing.to_sql(
        RAW_TABLE,
        engine,
        schema=RAW_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=CHUNK_SIZE,
        method="multi",
    )

    with engine.connect() as connection:
        row_count = connection.execute(
            text(f"select count(*) from {RAW_SCHEMA}.{RAW_TABLE}")
        ).scalar_one()

    logger.info("landed %s rows into %s.%s", row_count, RAW_SCHEMA, RAW_TABLE)
    return {
        "schema": RAW_SCHEMA,
        "table": RAW_TABLE,
        "rows_written": int(len(landing)),
        "rows_in_table": int(row_count),
    }
