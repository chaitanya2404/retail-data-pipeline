# Retail Data Pipeline

A portfolio project demonstrating two halves of a real data role on a single
real-world dataset:

1. **Data Engineering** — a proper `extract → transform → load` pipeline
   (`src/etl/`) that downloads a real ~540k-row transaction dataset,
   cleans it with documented, testable rules, and loads it into a
   normalized SQLite database.
2. **Data Analysis** — a Jupyter notebook (`notebooks/analysis.ipynb`) that
   queries the cleaned data to answer real business questions: sales
   trends, top products/countries, and customer segmentation via RFM
   (Recency, Frequency, Monetary) analysis.

## Dataset

[UCI "Online Retail" Data Set](https://archive.ics.uci.edu/dataset/352/online+retail) —
541,909 real, anonymized transactions from a UK-based, registered, non-store
online retailer between 01-Dec-2010 and 09-Dec-2011. Columns: `InvoiceNo`,
`StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`,
`CustomerID`, `Country`.

The raw file is **not** committed to this repository (it's ~23MB) — `src/etl/extract.py`
downloads it directly from the UCI archive into `data/raw/` on first run, so the
whole pipeline is reproducible from a fresh clone.

## Project Structure

```
retail-data-pipeline/
├── src/etl/
│   ├── extract.py       # Downloads raw dataset into data/raw/
│   ├── transform.py     # Cleaning functions (testable, documented decisions)
│   ├── load.py          # Loads cleaned data into SQLite via SQLAlchemy
│   └── pipeline.py       # Orchestrates extract -> transform -> load, with logging
├── tests/
│   └── test_transform.py  # pytest unit tests for the cleaning logic
├── notebooks/
│   └── analysis.ipynb     # Sales trends, top products/countries, RFM segmentation
├── outputs/figures/        # Saved chart PNGs (viewable on GitHub without running code)
├── data/
│   ├── raw/                # Downloaded raw .xlsx (gitignored)
│   └── processed/          # Cleaned SQLite DB (gitignored, regenerate via pipeline.py)
├── requirements.txt
├── pytest.ini
├── LICENSE
└── README.md
```

## Data Engineering: Pipeline Architecture

```
 extract.py            transform.py                 load.py
┌────────────┐       ┌────────────────────┐       ┌──────────────────┐
│ Download   │  -->  │ Clean & validate    │  -->  │ Load into SQLite │
│ raw .xlsx  │       │ (drop cancellations,│       │ (customers,      │
│ from UCI   │       │ invalid rows, dedupe│       │  products,       │
│            │       │ compute TotalPrice) │       │  transactions)   │
└────────────┘       └────────────────────┘       └──────────────────┘
        \_______________________  ________________________/
                                \/
                        pipeline.py orchestrates all
                     three stages with stage-level logging
                      (row counts + duration per stage)
```

**Cleaning decisions** (see full docstring in `src/etl/transform.py`):

| Rule | Rationale |
|---|---|
| Drop invoices starting with `C` (cancellations) | Represent returns, not completed sales |
| Drop non-positive `Quantity` / `UnitPrice` | Stock adjustments / free items, not paid sales |
| Drop rows with missing `Description` | No usable product info; overlaps heavily with £0 adjustment rows |
| **Keep** rows with missing `CustomerID` | Real sales from guest checkouts (~25% of rows) — flagged with `is_guest`, excluded only from customer-level (RFM) analysis |
| Drop exact duplicate rows | Data entry duplication |
| Compute `TotalPrice = Quantity * UnitPrice` | Needed for all revenue analysis |

**Schema** (SQLite, `data/processed/retail.db`):
- `customers(customer_id PK, country)`
- `products(stock_code PK, description)`
- `transactions(id PK, invoice_no, stock_code FK, customer_id FK nullable, quantity, unit_price, total_price, invoice_date, country, is_guest)`

A lightweight star-schema (one fact table + two dimension tables) — enough
structure to demonstrate normalization without over-engineering a dataset
this size.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run the pipeline

```bash
python -m src.etl.pipeline
```

This downloads the raw dataset (if not already present in `data/raw/`),
cleans it, and writes `data/processed/retail.db`. Each stage logs its row
counts and duration:

```
=== STAGE 1/3: EXTRACT ===
Extract complete: 541909 rows, 8 columns in 10.61s
=== STAGE 2/3: TRANSFORM ===
Transform complete: 541909 -> 524876 rows (17033 dropped, 3.1%) in 0.36s
=== STAGE 3/3: LOAD ===
Load complete: {'customers': 4338, 'products': 3922, 'transactions': 524876} in 2.82s
=== PIPELINE COMPLETE in 13.79s ===
```

### Run the tests

```bash
pytest
```

21 tests covering every cleaning rule in `transform.py`, using small inline
sample DataFrames (not the full dataset).

### Open the analysis notebook

```bash
jupyter notebook notebooks/analysis.ipynb
```

The notebook connects to `data/processed/retail.db` (run the pipeline first),
so run it top-to-bottom after `python -m src.etl.pipeline`. All charts are
also pre-rendered as PNGs in `outputs/figures/` for viewing without running
anything.

## Data Analysis: Key Findings

*(Computed from the real, cleaned dataset — 524,876 order lines, 4,338
identified customers, Dec 2010 – Dec 2011.)*

1. **Revenue & scale.** The cleaned dataset totals **£10,641,558.95** in
   revenue across **19,960 invoices**. 541,909 raw rows were reduced to
   524,876 clean rows (3.1% dropped — cancellations, invalid
   quantities/prices, or missing descriptions).
2. **Seasonality.** Revenue peaks in **November 2011 at £1,503,329.78** —
   nearly triple the February 2011 low of £522,545.56 — consistent with
   pre-Christmas ordering. December 2011 is a partial month in the source
   data (cut off on the 9th).
3. **Geographic concentration.** **84.6% of revenue (£9,001,192)** comes
   from the United Kingdom; every other country contributes under 3%
   individually — the Netherlands, EIRE (Ireland), Germany, and France are
   the next largest markets.
4. **Top products.** Beyond shipping/adjustment line items, the top
   physical product by revenue is the **REGENCY CAKESTAND 3 TIER
   (£174,156.54)**, followed by **PAPER CRAFT, LITTLE BIRDIE
   (£168,469.60)** and the iconic **WHITE HANGING HEART T-LIGHT HOLDER
   (£106,415.23)**.
5. **Customer value is highly concentrated.** RFM segmentation of the 4,338
   identified customers found **1,267 "Champions" (29.2% of customers)**
   generating **76.8% of total customer revenue** — a textbook Pareto
   pattern. The single largest customer contributed **£280,206.02** across
   73 orders. **300 customers (6.9%) are "Lost"** — a concrete win-back
   marketing target.
6. **Order economics.** Average order value is **£533.14** (median
   £303.30); the mean/median gap points to a right-skewed distribution
   driven by large wholesale-style orders.

**Business takeaways:** prioritize retention spend on the ~1,267 Champions
(≈77% of revenue), build a win-back campaign for the ~300 Lost customers,
and treat non-UK markets as a growth opportunity given how concentrated
revenue currently is domestically.

### Charts

**Monthly revenue trend:**

![Monthly Revenue](outputs/figures/01_monthly_revenue.png)

**Customer segments (RFM):**

![RFM Segments](outputs/figures/05_rfm_segments.png)

See `outputs/figures/` for all 6 charts (monthly & weekly revenue, top
products, top countries, RFM segment distribution, RFM scatter plot) and
`notebooks/analysis.ipynb` for the full analysis with commentary.

## Tech Stack

pandas · SQLAlchemy · SQLite · matplotlib · seaborn · pytest · Jupyter

## License

MIT — see [LICENSE](LICENSE).
