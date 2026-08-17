-- Minimal landing-zone fixture for CI.
--
-- CI does not download the 23MB source dataset — the existing jobs deliberately avoid network and
-- data dependencies. These rows are hand-picked to exercise the parts of the models that actually
-- break: a null customer, a stock code with conflicting descriptions, a zero-price line that
-- staging must filter, and a gap in the calendar so the generated date spine is not just
-- `distinct(date)` in disguise.

create schema if not exists raw;

drop table if exists raw.transactions;

create table raw.transactions (
    invoice_no    varchar(20)   not null,
    stock_code    varchar(20)   not null,
    description   text,
    quantity      integer       not null,
    invoice_date  timestamp     not null,
    unit_price    numeric(12,4) not null,
    customer_id   varchar(20),
    country       varchar(64)   not null,
    loaded_at     timestamptz   not null
);

insert into raw.transactions
    (invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id, country, loaded_at)
values
    -- A normal customer with several invoices, spanning a weekend and a weekday.
    ('536365', '85123A', 'WHITE HANGING HEART T-LIGHT HOLDER', 6,  '2011-01-03 08:26', 2.55, '17850', 'United Kingdom', now()),
    ('536365', '71053',  'WHITE METAL LANTERN',                6,  '2011-01-03 08:26', 3.39, '17850', 'United Kingdom', now()),
    ('536366', '85123A', 'WHITE HANGING HEART T-LIGHT HOLDER', 12, '2011-01-08 09:01', 2.55, '17850', 'United Kingdom', now()),

    -- Same stock code, different description. dim_product must resolve to the more frequent one
    -- rather than picking arbitrarily, or a product renames itself on every rebuild.
    ('536367', '85123A', 'CREAM HANGING HEART T-LIGHT HOLDER', 2,  '2011-01-09 10:15', 2.55, '13047', 'United Kingdom', now()),

    -- No customer id. Must survive into the facts under the UNKNOWN key; dropping it would
    -- understate revenue, and failing the not_null test would be wrong.
    ('536368', '84406B', 'CREAM CUPID HEARTS COAT HANGER',     8,  '2011-01-10 11:30', 2.75, null,    'France',         now()),

    -- Zero price and zero quantity: catalogue noise that staging is expected to filter out.
    ('536369', '22423',  'REGENCY CAKESTAND 3 TIER',           1,  '2011-01-10 11:45', 0.00, '13047', 'United Kingdom', now()),
    ('536370', '22423',  'REGENCY CAKESTAND 3 TIER',           0,  '2011-01-10 11:50', 12.75,'13047', 'United Kingdom', now()),

    -- A high-value customer, so the RFM quintiles have something to separate.
    ('536371', '22423',  'REGENCY CAKESTAND 3 TIER',           48, '2011-02-14 14:20', 12.75,'12583', 'France',         now()),
    ('536372', '84879',  'ASSORTED COLOUR BIRD ORNAMENT',      32, '2011-02-14 14:25', 1.69, '12583', 'France',         now()),

    -- Deliberately much later, leaving a multi-week hole in the calendar. dim_date must still
    -- produce an unbroken spine across the gap.
    ('536373', '84879',  'ASSORTED COLOUR BIRD ORNAMENT',      10, '2011-03-21 16:05', 1.69, '14688', 'Germany',        now()),

    -- Wide price spread on one product, which dim_product flags as volatile pricing.
    ('536374', '84879',  'ASSORTED COLOUR BIRD ORNAMENT',      5,  '2011-03-22 09:10', 9.95, '14688', 'Germany',        now());
