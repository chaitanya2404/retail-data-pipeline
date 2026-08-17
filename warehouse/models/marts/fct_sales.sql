{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_key'], 'type': 'btree'},
      {'columns': ['customer_key'], 'type': 'btree'},
      {'columns': ['product_key'], 'type': 'btree'},
    ]
) }}

-- Sales fact. Grain: one row per invoice line — the finest the source supports, and the only
-- grain that lets both "revenue by month" and "units of product X" be answered from one table
-- without a second aggregate.
--
-- Indexed on each foreign key because every dashboard query filters or joins on at least one of
-- them; without the indexes a star join degrades into three sequential scans of half a million
-- rows.

with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    -- Surrogate key over the natural composite. An invoice can legitimately contain the same
    -- product on two lines (a correction, or a different price), so invoice_no + product_key is
    -- not unique on real data and a uniqueness test on it would fail.
    {{ dbt_utils.generate_surrogate_key([
        'invoice_no', 'product_key', 'invoiced_at', 'unit_price', 'quantity'
    ]) }} as sales_key,

    invoice_no,
    date_key,
    customer_key,
    product_key,
    country,

    quantity,
    unit_price,
    line_revenue,

    invoiced_at
from transactions
