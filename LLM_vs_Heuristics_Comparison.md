# LLM vs Heuristics Comparison — Submission Reference

This document compares two runs of Brownfield Cartographer on **dbt-labs/jaffle-shop**: one with **Semanticist heuristics only** (no LLM) and one with **LLM-powered Semanticist** (OpenRouter). All other previous test/run outputs have been removed; only these two output folders remain in the project for submission.

**Pipeline alignment:** Both outputs were produced by the current pipeline (Surveyor → Hydrologist → Semanticist → Archivist). The artifact set and JSON schema are identical; only Semanticist-generated fields (`purpose_statement`, `domain_cluster`) differ between LLM and no-LLM runs.

---

## 1. Run summary (current runs)

| | **No LLM (heuristics only)** | **With LLM (OpenRouter)** |
|--|------------------------------|---------------------------|
| **Command** | `uv run cartographer analyze https://github.com/dbt-labs/jaffle-shop -o ./jaffle_shop_no_llm --no-llm` | `uv run cartographer analyze https://github.com/dbt-labs/jaffle-shop -o ./jaffle_shop_llm` |
| **Output folder** | `jaffle_shop_no_llm/.cartography/` | `jaffle_shop_llm/.cartography/` |
| **Duration** | **8.04 s** | **60.16 s** |
| **Semanticist** | heuristics only | LLM: 32 calls |
| **Files / lineage** | 32 files, 983 LOC, 20 datasets, 13 transformations | Same |

**CLI output (no LLM):**
```
Analysis complete in 8.04s
Files: 32 analyzed (983 LOC)
Languages: sql, yaml
Datasets: 20 | Transformations: 13 | dbt refs found: 17
Semanticist: heuristics only
```

**CLI output (LLM):**
```
Analysis complete in 60.16s
Files: 32 analyzed (983 LOC)
Languages: sql, yaml
Datasets: 20 | Transformations: 13 | dbt refs found: 17
Semanticist: LLM: 32 calls
```

---

## 2. Where to compare

- **`module_graph.json`** — Compare `purpose_statement` and `domain_cluster` per node.
- **`CODEBASE.md`** — “Module map (by domain)” section: each line shows purpose text after the em dash. New runs also include “Known debt”, “High-velocity files”, and “Module purpose index”.
- **`onboarding_brief.md`** — Structure is the same; lineage (sources, sinks, critical path) is identical.

---

## 3. Example differences (Semanticist output, from current runs)

### Purpose statements

| File | Heuristics only | With LLM |
|------|------------------|----------|
| `dbt_project.yml` | dbt project configuration | Configuration file for a dbt project. |
| `packages.yml` | YAML config: package, version, package | Dependency management for dbt packages. |
| `taskfile.yml` | YAML config: YEARS, DB, venv | Taskfile for managing project tasks and dependencies. |
| `macros/cents_to_dollars.sql` | dbt macro | Converts cents to dollars for financial calculations. |
| `macros/generate_schema_name.sql` | dbt macro | Generates schema names for database objects. |
| `models/marts/customers.sql` | dbt mart model | dbt model for customer mart data. |
| `models/marts/orders.sql` | dbt mart model | dbt model for order data aggregation. |
| `models/marts/metricflow_time_spine.sql` | dbt mart model | SQL query for generating a time spine for metrics in the data mart. |

### Domain clusters

| File | Heuristics only | With LLM |
|------|------------------|----------|
| `dbt_project.yml` | null | configuration |
| `packages.yml` | null | configuration |
| `taskfile.yml` | null | configuration |
| `macros/generate_schema_name.sql` | data | configuration |
| `models/marts/customers.yml` | analytics | configuration |

**Takeaway:** Heuristics use path/language patterns (e.g. “staging” → ingestion, “marts” → analytics). The LLM produces more specific purpose text and assigns domains like “configuration” where heuristics leave `null` or use a generic label.

---

## 4. Artifacts in the project (only these run outputs)

- **No-LLM run:** `jaffle_shop_no_llm/.cartography/` — module_graph.json, lineage_graph.json, CODEBASE.md, onboarding_brief.md, cartography_trace.jsonl, *.graphml, last_run.json.
- **LLM run:** `jaffle_shop_llm/.cartography/` — same set.
- **This file:** `LLM_vs_Heuristics_Comparison.md` (this comparison).

Lineage (sources, sinks, critical path, blast radius) is **identical** in both runs; only Semanticist-generated purpose and domain differ.
