# LLM vs Heuristics Comparison — Submission Reference

This document compares two runs of Brownfield Cartographer on **dbt-labs/jaffle-shop**: one with **Semanticist heuristics only** (no LLM) and one with **LLM-powered Semanticist** (OpenRouter). Both outputs are kept for final document submission.

---

## 1. Run summary

| | **No LLM (heuristics only)** | **With LLM (OpenRouter)** |
|--|------------------------------|---------------------------|
| **Command** | `uv run cartographer analyze https://github.com/dbt-labs/jaffle-shop -o ./jaffle_shop_no_llm --no-llm` | `uv run cartographer analyze https://github.com/dbt-labs/jaffle-shop -o ./jaffle_shop_llm` |
| **Output folder** | `jaffle_shop_no_llm/.cartography/` | `jaffle_shop_llm/.cartography/` |
| **Duration** | ~15 s | ~96 s |
| **Semanticist** | heuristics only | LLM: 32 calls |
| **Files / lineage** | 32 files, 20 datasets, 13 transformations (same) | Same |

---

## 2. Where to compare

- **`module_graph.json`** — Compare `purpose_statement` and `domain_cluster` per node.
- **`CODEBASE.md`** — “Module map (by domain)” section: each line shows purpose text after the em dash.
- **`onboarding_brief.md`** — Structure is the same; lineage (sources, sinks, critical path) is identical; any “key modules” or narrative can differ if derived from purpose/domain.

---

## 3. Example differences (Semanticist output)

### Purpose statements

| File | Heuristics only | With LLM |
|------|------------------|----------|
| `dbt_project.yml` | dbt project configuration | Configuration file for a dbt project. |
| `packages.yml` | YAML config: package, version, package | Lock file for managing package dependencies in a project. |
| `models/marts/customers.sql` | dbt mart model | dbt model for customer mart data. |
| `models/marts/orders.sql` | dbt mart model | dbt model for aggregating order data. |
| `macros/cents_to_dollars.sql` | dbt macro | Converts cents to dollars for financial calculations. |
| `models/staging/stg_customers.sql` | dbt staging model | dbt staging model for customer data. |

### Domain clusters

| File | Heuristics only | With LLM |
|------|------------------|----------|
| `dbt_project.yml` | null | configuration |
| `packages.yml` | null | configuration |
| `taskfile.yml` | null | pipeline |
| `models/marts/customers.yml` | analytics | configuration (YAML as config) |
| `models/staging/stg_customers.sql` | ingestion | analytics |

**Takeaway:** Heuristics use path/language patterns (e.g. “staging” → ingestion, “marts” → analytics). The LLM produces more specific purpose text and can assign domains like “configuration” or “pipeline” where heuristics leave `null` or use a generic label.

---

## 4. Artifacts to include for submission

- **No-LLM run:** `jaffle_shop_no_llm/.cartography/` (module_graph.json, lineage_graph.json, CODEBASE.md, onboarding_brief.md, trace, GraphML).
- **LLM run:** `jaffle_shop_llm/.cartography/` (same set).
- **This file:** `LLM_vs_Heuristics_Comparison.md` (this comparison).

Lineage (sources, sinks, critical path, blast radius) is **identical** in both runs; only Semanticist-generated purpose and domain differ.
