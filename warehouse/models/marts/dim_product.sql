{{ config(materialized='table') }}

-- Product dimension.
--
-- The source has no product master, so the dimension is derived from the transactions themselves.
-- That makes description conflicts a real problem: the same stock code appears with several
-- spellings across a year of data, and picking arbitrarily would make a product's name change
-- every time the model rebuilds.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

-- Resolve each product to the description it was sold under most often, breaking ties on the
-- most recent use so a renamed product settles on its current name rather than its historical one.
description_ranking as (
    select
        product_key,
        product_description,
        count(*)                                     as times_used,
        max(invoiced_at)                             as last_used_at,
        row_number() over (
            partition by product_key
            order by count(*) desc, max(invoiced_at) desc
        )                                            as description_rank
    from transactions
    where product_description is not null
    group by product_key, product_description
),

product_facts as (
    select
        product_key,
        count(distinct invoice_no)                   as invoice_count,
        sum(quantity)                                as total_units_sold,
        sum(line_revenue)                            as total_revenue,
        min(unit_price)                              as min_unit_price,
        max(unit_price)                              as max_unit_price,
        -- Revenue-weighted, not a plain average of the price column: selling 1000 units at £1 and
        -- one unit at £50 is a £1 product, and avg(unit_price) would call it £25.50.
        (sum(line_revenue) / nullif(sum(quantity), 0))::numeric(12, 4) as avg_selling_price
    from transactions
    group by product_key
)

select
    pf.product_key,
    coalesce(dr.product_description, 'UNKNOWN PRODUCT') as product_description,
    pf.invoice_count,
    pf.total_units_sold,
    pf.total_revenue,
    pf.min_unit_price,
    pf.max_unit_price,
    pf.avg_selling_price,
    -- A spread this wide usually means a genuine price change or a data-entry error; surfacing it
    -- as a flag lets the quality suite assert on it instead of an analyst noticing by accident.
    (pf.max_unit_price > pf.min_unit_price * 3) as has_volatile_pricing
from product_facts pf
left join description_ranking dr
    on pf.product_key = dr.product_key
   and dr.description_rank = 1
