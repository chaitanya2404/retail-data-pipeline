{{ config(materialized='table') }}

-- Customer dimension with RFM segmentation baked in.
--
-- The segmentation lives here rather than in the BI tool on purpose: Power BI, a notebook and an
-- ad-hoc SQL query would each otherwise implement "churned" slightly differently, and the first
-- question in any review meeting becomes whose number is right.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

-- The dataset ends in December 2011, so "today" is meaningless for recency. Anchoring to the last
-- observed invoice keeps the segments stable no matter when the model is rebuilt — using
-- current_date would silently reclassify every customer as churned.
as_of as (
    select max(date_key) as as_of_date from transactions
),

customer_facts as (
    select
        t.customer_key,
        min(t.date_key)                              as first_purchase_date,
        max(t.date_key)                              as last_purchase_date,
        count(distinct t.invoice_no)                 as invoice_count,
        sum(t.quantity)                              as total_units,
        sum(t.line_revenue)                          as lifetime_revenue,
        -- mode() rather than max(): a customer's country should be where they actually order
        -- from most often, not whichever name sorts last alphabetically.
        mode() within group (order by t.country)     as primary_country
    from transactions t
    group by t.customer_key
),

rfm as (
    select
        cf.*,
        (a.as_of_date - cf.last_purchase_date)       as recency_days,

        -- ntile over the whole customer base rather than fixed thresholds: hard-coded cutoffs
        -- rot as the business grows, quintiles re-rank themselves on every run.
        ntile(5) over (order by (a.as_of_date - cf.last_purchase_date) desc) as recency_score,
        ntile(5) over (order by cf.invoice_count)                            as frequency_score,
        ntile(5) over (order by cf.lifetime_revenue)                         as monetary_score
    from customer_facts cf
    cross join as_of a
)

select
    customer_key,
    primary_country,
    first_purchase_date,
    last_purchase_date,
    recency_days,
    invoice_count,
    total_units,
    lifetime_revenue,
    recency_score,
    frequency_score,
    monetary_score,

    case
        -- 'UNKNOWN' is the bucket for unattributed rows, not a person. Scoring it as a customer
        -- would put a phantom at the top of every "best customer" list, since it aggregates a
        -- quarter of all revenue.
        when customer_key = 'UNKNOWN' then 'Unattributed'
        when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champion'
        when recency_score >= 4 and frequency_score >= 3 then 'Loyal'
        when recency_score >= 4 then 'Recent'
        when recency_score <= 2 and monetary_score >= 4 then 'At Risk'
        when recency_score <= 2 then 'Churned'
        else 'Regular'
    end as rfm_segment

from rfm
