# Onboarding Brief — FDE Day-One

**Target:** C:\Users\ADMINI~1\AppData\Local\Temp\cartographer_n5jglrq1

---

## 1. What is the primary data ingestion path?

Data enters via:
- **ecom.raw_customers**
- **ecom.raw_stores**
- **ecom.raw_orders**
- **ecom.raw_items**
- **ecom.raw_products**
- **ecom.raw_supplies**
- **ecom**

Staging/models consume these via `ref()` and `source()` (dbt) or equivalent.

---

## 2. Critical output datasets
- **customers**
- **locations**
- **metricflow_time_spine**
- **products**
- **supplies**
- **ecom**

---

## 3. Blast radius (if a key module fails)
The longest dependency chain (critical path) is:

ecom.raw_items -> stg_order_items -> order_items -> orders -> customers

Failure of any node in this chain can block downstream nodes.

---

## 4. Where to start reading code
- Review **staging** and **marts** (or equivalent) in the module map in CODEBASE.md.

---

## 5. Next steps

1. Read **CODEBASE.md** for the full module map and lineage.
2. Query the graph: `cartographer query <cartography_dir> <question>`
3. Inspect `.cartography/module_graph.json` and `lineage_graph.json` for APIs.