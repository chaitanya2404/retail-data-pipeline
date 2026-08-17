{{ config(materialized='view') }}

-- Staging does exactly three things: rename to the warehouse's vocabulary, cast to the types the
-- marts rely on, and derive the surrogate keys. No business logic and no filtering beyond what is
-- provably junk, so that a question about a number can always be traced to a single mart model
-- rather than to something quietly dropped three layers down.

with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        invoice_no::varchar                          as invoice_no,
        stock_code::varchar                          as product_key,
        nullif(trim(description), '')                as product_description,
        quantity::integer                            as quantity,
        invoice_date::timestamp                      as invoiced_at,
        unit_price::numeric(12, 4)                   as unit_price,

        -- Unattributed rows are kept, not dropped: about a quarter of the source has no customer
        -- and discarding them would understate revenue. They are bucketed under a single
        -- 'UNKNOWN' key so every fact row still joins to the customer dimension.
        coalesce(customer_id::varchar, 'UNKNOWN')    as customer_key,

        country::varchar                             as country,

        -- Date key as a real date, not the YYYYMMDD integer the textbooks use. Postgres can then
        -- range-scan and date-truncate the fact table directly; an integer key forces a cast on
        -- every time-series query and throws the index away.
        invoice_date::date                           as date_key,

        (quantity * unit_price)::numeric(14, 4)      as line_revenue,
        loaded_at::timestamp                         as loaded_at
    from source
)

select * from renamed
-- Zero-quantity or zero-price lines are catalogue noise rather than sales; they would otherwise
-- inflate line counts while contributing nothing to revenue.
where quantity > 0
  and unit_price > 0
