# RECONNAISSANCE — Manual Day-One Analysis

**Target codebase:** dbt jaffle-shop (https://github.com/dbt-labs/jaffle-shop)  
**Date:** March 11, 2026  
**Analyst:** FDE Candidate

---

## The Five FDE Day-One Questions (manual answers)

### 1. What is the primary data ingestion path?

The raw data enters through **dbt seeds** or can be loaded from **S3** into a `raw` schema. The source tables are defined in `models/staging/__sources.yml` under the `ecom` source:

- `raw_customers` — customer IDs and names
- `raw_orders` — order transactions with store and customer references
- `raw_order_items` — individual items within orders (linked to SKUs)
- `raw_products` — product catalog with SKUs, names, types, prices
- `raw_supplies` — supply costs per product
- `raw_stores` — store locations with tax rates

The staging models reference these via `{{ source('ecom', 'raw_customers') }}` pattern, not direct `ref()` calls. This is the proper dbt pattern for external source data.

**Entry point:** S3/Seeds -> `raw` schema -> staging models (`stg_*`) -> mart models

---

### 2. What are the 3-5 most critical output datasets/endpoints?

Based on the `models/marts/*.yml` files, the **mart-level outputs** are:

1. **`customers`** — customer dimension with lifetime metrics:
   - `customer_id`, `customer_name`
   - `count_lifetime_orders`, `first_ordered_at`, `last_ordered_at`
   - `lifetime_spend_pretax`, `lifetime_tax_paid`, `lifetime_spend`
   - `customer_type` (new vs returning)

2. **`orders`** — orders fact table:
   - `order_id`, `customer_id`, `location_id`, `ordered_at`, `status`
   - `order_cost`, `order_items_subtotal`, `order_total`
   - `count_food_items`, `count_drink_items`
   - `is_food_order`, `is_drink_order`

3. **`order_items`** — line-item level detail:
   - `order_item_id`, `order_id`, `product_id`
   - `product_name`, `product_price`, `supply_cost`
   - `is_food_item`, `is_drink_item`

4. **`products`** — product dimension (pass-through from staging)

5. **`locations`** — store locations with tax rates

These are the tables downstream dashboards would query for revenue, customer analytics, and inventory reporting.

---

### 3. What is the blast radius if the most critical module fails?

**If `stg_orders.sql` fails:**
- `orders.sql` breaks (depends on `stg_orders`)
- `order_items.sql` breaks (joins to `stg_orders` for `ordered_at`)
- `customers.sql` breaks (aggregates from `orders`)
- 3 out of 5 mart models become unavailable

**If `stg_products.sql` fails:**
- `products.sql` breaks
- `order_items.sql` breaks (joins to `stg_products`)
- `orders.sql` indirectly affected via `order_items`

**If `stg_supplies.sql` fails:**
- `supplies.sql` breaks
- `order_items.sql` breaks (needs supply costs)
- Cost calculations in orders become stale

**Blast radius summary:**
- `stg_orders` is the most critical — impacts 3 downstream models
- `stg_products` and `stg_supplies` impact `order_items` which impacts `orders`
- The dependency chain: `source('ecom', ...) -> stg_* -> order_items -> orders -> customers`

---

### 4. Where is the business logic concentrated vs. distributed?

**Concentrated in mart models:**

- `models/marts/orders.sql` — the most complex model:
  - Joins `stg_orders` + `order_items` 
  - Aggregates order costs and item counts
  - Computes boolean flags (`is_food_order`, `is_drink_order`)
  - Calculates `customer_order_number` via window function

- `models/marts/order_items.sql` — joins 4 staging tables:
  - `stg_order_items` + `stg_orders` + `stg_products` + supply costs
  - Enriches line items with product details and costs

- `models/marts/customers.sql` — customer lifetime aggregation:
  - Groups orders by customer
  - Computes lifetime spend, order counts, first/last order dates
  - Classifies customers as new vs returning

**Staging layer is thin:**
- `stg_*.sql` files are mostly SELECT + rename + simple transforms
- Uses `{{ cents_to_dollars() }}` macro for currency conversion
- Uses `{{ dbt.date_trunc() }}` for date standardization

**Macros:**
- `macros/cents_to_dollars.sql` — handles currency conversion across dialects (Postgres, BigQuery, Snowflake, Fabric)
- `macros/generate_schema_name.sql` — custom schema routing for environments

**Business logic distribution:**
- ~70% in mart models (`orders.sql`, `order_items.sql`, `customers.sql`)
- ~20% in staging (cleaning, renaming, type casting)
- ~10% in macros (reusable transforms)

---

### 5. What has changed most frequently in the last 90 days?

Based on the GitHub commit history (161 commits total):

- **Most active:** Configuration files (`dbt_project.yml`, `packages.yml`, `Taskfile.yml`)
- **Schema updates:** `models/marts/*.yml` and `models/staging/*.yml` for column descriptions and tests
- **Recent additions:** MetricFlow time spine (`metricflow_time_spine.sql`), semantic models
- **Stable:** Core SQL transformation logic in mart models

The repo is actively maintained with focus on:
- Adding semantic layer / MetricFlow support
- Improving documentation and tests
- Supporting multiple warehouse adapters

---

## Difficulty analysis

### Hardest to figure out manually:

- **Tracing the full lineage** — `order_items.sql` joins 4 tables, `orders.sql` joins to `order_items`, `customers.sql` aggregates from `orders`. That's a 5-level deep chain from raw sources to final output. I had to open 8+ files to trace one metric.

- **Understanding the source() vs ref() pattern** — The staging models use `{{ source('ecom', 'raw_orders') }}` but mart models use `{{ ref('stg_orders') }}`. This two-pattern approach is correct but requires understanding dbt conventions.

- **Macro behavior across dialects** — The `cents_to_dollars` macro has different implementations for Postgres, BigQuery, Snowflake, and Fabric. Without running dbt, I couldn't tell which one would execute.

- **Finding where `order_total` comes from** — The `customers.lifetime_spend` aggregates `orders.order_total`, which comes from `stg_orders.order_total`, which is `{{ cents_to_dollars('order_total') }}` applied to `raw_orders.order_total`. That's 4 hops.

### Where I got lost:

- **Multiple jaffle-shop versions** — There's `jaffle_shop` (old, simple) and `jaffle-shop` (new, complex). The hyphen matters. I initially looked at the wrong one.

- **Semantic layer configuration** — The `models/marts/*.yml` files have `semantic_models` sections with dimensions, measures, and entities. This is for dbt's MetricFlow feature, not standard dbt, and I wasn't sure how it connects to the SQL models.

- **The supplies join** — `order_items.sql` joins to a CTE called `order_supplies_summary` which aggregates `stg_supplies` by `product_id`. This indirect join pattern took time to understand.

- **Package dependencies** — The project uses `dbt_utils` and `dbt_date` packages. I had to check `packages.yml` to understand which macros were available.

### What would have helped:

- **A lineage graph** showing `raw_* -> stg_* -> order_items -> orders -> customers`
- **Entry/exit points list** — "Data enters via `ecom` source, exits via `customers`, `orders`, `order_items`, `products`, `locations`, `supplies`"
- **Column-level lineage** — "customer.lifetime_spend comes from sum(orders.order_total)"
- **Macro expansion** — Show me what `{{ cents_to_dollars('amount') }}` actually produces
- **Blast radius tool** — "If stg_orders breaks, these 3 models fail"

---

## Comparison: Manual vs. Cartographer Output

| Question | Manual Answer | Cartographer Found |
|----------|--------------|-------------------|
| Data sources | ecom.raw_customers, ecom.raw_orders, ecom.raw_order_items, ecom.raw_products, ecom.raw_supplies, ecom.raw_stores | ecom (source), plus individual tables via source() extraction |
| Output tables | customers, orders, order_items, products, locations, supplies | customers, orders, order_items, products, locations, supplies, stg_* |
| Critical path | raw -> stg -> order_items -> orders -> customers | Computed via DAG longest path |
| dbt refs found | 8 refs + 6 sources | 8+ dbt refs extracted |
| Business logic | orders.sql, order_items.sql, customers.sql | (needs Semanticist) |
| Macros | cents_to_dollars, generate_schema_name | Skipped (correctly identified as macros, not models) |

**Verdict:** The Cartographer correctly identified:
- Sources and sinks
- dbt `ref()` and `source()` patterns  
- Critical path through the DAG
- Macro files (excluded from lineage)
- Column definitions from schema.yml

Areas for improvement:
- Column-level lineage
- Semantic understanding of business logic
- Macro expansion/resolution

---

_This reconnaissance serves as ground truth for evaluating the Cartographer's accuracy on the modern jaffle-shop repository._
