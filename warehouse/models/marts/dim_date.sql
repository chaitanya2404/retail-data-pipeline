{{ config(materialized='table') }}

-- Date dimension, generated rather than derived from the facts.
--
-- Building it from distinct invoice dates would silently omit days with no sales, and a time
-- series with missing days makes every "revenue by day" chart lie: the gap closes up and a dead
-- Sunday looks like it never existed. generate_series guarantees an unbroken spine.

with bounds as (
    select
        min(date_key) as start_date,
        max(date_key) as end_date
    from {{ ref('stg_transactions') }}
),

spine as (
    select generate_series(
        (select start_date from bounds),
        (select end_date from bounds),
        interval '1 day'
    )::date as date_key
)

select
    date_key,
    extract(year from date_key)::int                 as calendar_year,
    extract(quarter from date_key)::int              as calendar_quarter,
    extract(month from date_key)::int                as calendar_month,
    to_char(date_key, 'YYYY-MM')                     as year_month,
    trim(to_char(date_key, 'Month'))                 as month_name,
    extract(day from date_key)::int                  as day_of_month,

    -- ISO day of week: Monday = 1, Sunday = 7. Postgres' `dow` starts the week on Sunday at 0,
    -- which quietly disagrees with every European retail report.
    extract(isodow from date_key)::int               as day_of_week,
    trim(to_char(date_key, 'Day'))                   as day_name,
    (extract(isodow from date_key) >= 6)             as is_weekend,

    date_trunc('week', date_key)::date               as week_start_date,
    date_trunc('month', date_key)::date              as month_start_date,
    (date_trunc('month', date_key) + interval '1 month - 1 day')::date as month_end_date

from spine
