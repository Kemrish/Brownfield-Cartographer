# Full Code Checklist — Purpose Fulfilled

This document verifies that every component from the GitHub Code (full system) spec is present in the project and its purpose fulfilled.

---

## GitHub Code (full system)

| Component | Path | Purpose fulfilled |
|-----------|------|-------------------|
| **CLI** | `src/cli.py` | Subcommands: **analyze** (full pipeline), **query** (Navigator interactive mode), **export-graphml**. Options: `-o`, `--no-llm`, `--incremental`. |
| **Orchestrator** | `src/orchestrator.py` | Full pipeline: **Surveyor → Hydrologist → Semanticist → Archivist**. Resilient (partial results on failure), incremental (git diff), trace writing. |
| **Models** | `src/models/` | Pydantic schemas: Node types (ModuleNode, DatasetNode, TransformationNode, ConfigNode, etc.), Edge types (GraphEdge, EdgeType), Graph types (ModuleGraph, LineageGraph, NetworkMetrics, ColumnLineageEdge). |
| **Tree-sitter analyzer** | `src/analyzers/tree_sitter_analyzer.py` | Multi-language AST parsing with **LanguageRouter**; Python/SQL/YAML/JS/TS; complexity, imports, functions, classes. |
| **SQL lineage** | `src/analyzers/sql_lineage.py` | **sqlglot**-based SQL dependency extraction; tables, CTEs, column lineage, dbt ref/source. |
| **DAG config parser** | `src/analyzers/dag_config_parser.py` | Airflow/dbt YAML config parsing (dbt_project.yml, schema.yml, sources). |
| **Surveyor** | `src/agents/surveyor.py` | Module graph, **PageRank**, **git velocity**, **dead code candidates**, entry points, hub modules. |
| **Hydrologist** | `src/agents/hydrologist.py` | **DataLineageGraph**, **blast_radius**, **find_sources** / **find_sinks**, critical path, column lineage. |
| **Semanticist** | `src/agents/semanticist.py` | **LLM** purpose statements (OpenRouter/OpenAI), **doc drift detection**, domain clustering, Day-One Q&A (feeds Navigator/brief), **ContextWindowBudget** (token budget). |
| **Archivist** | `src/agents/archivist.py` | **CODEBASE.md** generation, **onboarding brief**, **trace logging** (timestamp, evidence_sources, confidence per artifact). |
| **Navigator** | `src/agents/navigator.py` | Agent with **4 tools**: **find_implementation**, **trace_lineage**, **blast_radius**, **explain_module**; NL routing; file/line citations and attribution. |
| **Knowledge graph** | `src/graph/knowledge_graph.py` | **NetworkX** wrapper with serialization (module/lineage JSON, GraphML), metrics, blast_radius. |
| **Incremental update** | `src/orchestrator.py` | **Incremental mode**: re-analyze only changed files via **git diff**; `.cartography/last_run.json` stores last commit. |
| **Dependencies** | `pyproject.toml` | Locked deps (uv); `uv sync` to install. |
| **README** | `README.md` | How to run against any GitHub URL; **analyze** and **query** modes; pipeline order; options. |

---

## Cartography Artifacts (2+ target codebases)

Each target has its own `.cartography/` with the required files:

| Artifact | Present |
|----------|--------|
| `.cartography/CODEBASE.md` | ✅ |
| `.cartography/onboarding_brief.md` | ✅ |
| `.cartography/module_graph.json` | ✅ |
| `.cartography/lineage_graph.json` | ✅ |
| `.cartography/cartography_trace.jsonl` | ✅ |

**Target codebases in this repo:**

1. **Root** — `.cartography/` (copied from latest LLM run for reference).
2. **jaffle_shop_no_llm** — `jaffle_shop_no_llm/.cartography/` (heuristics-only run).
3. **jaffle_shop_llm** — `jaffle_shop_llm/.cartography/` (LLM run).

---

## Quick verification

```bash
uv sync
uv run cartographer analyze https://github.com/dbt-labs/jaffle-shop -o ./out
uv run cartographer query ./out "critical path"
ls out/.cartography/
# CODEBASE.md, onboarding_brief.md, module_graph.json, lineage_graph.json, cartography_trace.jsonl
```
