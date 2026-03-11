# Interim Submission — Answers

## 1. What is the minimum working system you need to deliver tomorrow?

The **minimum working system** for the interim deadline is:

- **Entry point:** `src/cli.py` — takes a repo path (local or GitHub URL) and runs the analysis pipeline via the `analyze` command.
- **Orchestrator:** `src/orchestrator.py` — runs **Surveyor** then **Hydrologist** in sequence and writes all outputs to `.cartography/`.
- **Models:** `src/models/` — Pydantic schemas for all node types (ModuleNode, DatasetNode, FunctionNode, TransformationNode), edge types (IMPORTS, PRODUCES, CONSUMES, etc.), and serializable graph types (ModuleGraph, LineageGraph).
- **Analyzers:**
  - `src/analyzers/tree_sitter_analyzer.py` — multi-language AST parsing with a LanguageRouter (Python, SQL, YAML by extension); extracts imports, public functions, and classes from Python.
  - `src/analyzers/sql_lineage.py` — sqlglot-based SQL dependency extraction (SELECT/FROM/JOIN/CTE, INSERT, Create) for table-level lineage.
  - `src/analyzers/dag_config_parser.py` — Airflow/dbt YAML config parsing (dbt schema.yml sources/models, Airflow DAG heuristics).
- **Agents:**
  - `src/agents/surveyor.py` — builds the module import graph, PageRank, git velocity (30d), strongly connected components, dead-code candidates; writes `.cartography/module_graph.json`.
  - `src/agents/hydrologist.py` — builds the DataLineageGraph from SQL, Python data read/write patterns, and YAML; implements `blast_radius()`, `find_sources()`, `find_sinks()`; writes `.cartography/lineage_graph.json`.
- **Graph:** `src/graph/knowledge_graph.py` — NetworkX-based module and lineage graphs with JSON serialization.
- **Dependencies:** `pyproject.toml` with locked dependencies (uv); Python 3.11–3.13.
- **Docs:** `README.md` with install instructions and the `analyze` command documented.

**Cartography artifacts (at least one target codebase):**

- `.cartography/module_graph.json`
- `.cartography/lineage_graph.json` (partial acceptable; at minimum SQL lineage via sqlglot)

**Single PDF report** containing:

- RECONNAISSANCE.md (manual Day-One analysis for the chosen target)
- Architecture diagram of the four-agent pipeline with data flow (for interim: Surveyor + Hydrologist only)
- Progress summary: what’s working, what’s in progress
- Early accuracy: does the module graph look right? Does the lineage graph match reality?
- Known gaps and plan for final submission

---

## 2. Which target codebase did you choose and why?

**Primary choice: dbt Jaffle Shop**  
(https://github.com/dbt-labs/jaffle_shop)

**Reasons:**

- **Canonical dbt example:** Mixed SQL + YAML + (optional) Python; the challenge explicitly requires extracting the dbt DAG and verifying lineage against dbt’s own lineage.
- **Right size:** Small enough to run quickly and validate by hand; large enough to exercise SQL parsing, ref()-style dependencies, and schema.yml.
- **Clear success criterion:** “Your lineage graph must match dbt’s built-in lineage visualization” — gives a concrete accuracy check for the Hydrologist and sqlglot integration.
- **FDE-relevant:** Representative of typical brownfield data-engineering stacks (dbt-centric), so the Cartographer’s behavior here is directly applicable to real engagements.

**Optional second target for interim:** The Brownfield-Cartographer repo itself (self-referential). It exercises the Surveyor (module graph, Python files) and shows graceful behavior when there is little or no SQL/data lineage (empty lineage graph, no spurious sources/sinks).

---

## 3. What assumptions are you making about the structure of the codebase?

- **Layout and discovery**
  - Source files live under the repo root (or the root of the cloned repo). We discover by walking the tree and filtering by extension (`.py`, `.sql`, `.yml`, `.yaml`, optionally `.js`, `.ts`).
  - We skip directories such as `.git`, `__pycache__`, `node_modules`, `.venv`, and `venv` so that dependencies and generated artifacts are not analyzed as part of the project.

- **Python**
  - Imports follow standard Python syntax so that tree-sitter (and the LanguageRouter) can parse them; we resolve relative imports to repo-relative paths where possible.
  - “Public” API is inferred by excluding names that start with `_`. We do not require a specific packaging layout (e.g. `src/` or flat).

- **SQL**
  - SQL is parseable by sqlglot in a supported dialect (we default to Postgres; DuckDB, Snowflake, BigQuery, Spark are plausible). We assume conventional SELECT/FROM/JOIN/CTE and INSERT/Create patterns so that table-level sources and targets can be extracted.
  - For dbt: model SQL lives in `.sql` files and we use the filename (stem) as a fallback target when no explicit target table is found in the statement.

- **Config / dbt**
  - dbt projects use `schema.yml` / `sources.yml` (or similar) with the usual `sources:` and `models:` list structures so that the DAG config parser can extract source and model names.
  - Airflow DAGs are Python files; we use heuristic patterns (e.g. task_id, `>>` / `set_downstream`) and do not execute the code.

- **Git**
  - If the path is a git repository, we use `git log --follow` for change velocity. If it is not (e.g. a fresh clone with no history, or a non-git folder), we assume zero velocity and do not fail.

- **Reproducibility**
  - For GitHub URLs we assume the repo is publicly cloneable (or that credentials are available in the environment). We clone to a temporary directory and run analysis there; we do not assume a specific clone depth beyond what’s needed for the interim (e.g. `--depth 1` is sufficient).

- **Scale and failures**
  - We assume the codebase is large enough to be interesting (dozens of files) but not so large that we must shard or sample for the interim. Unparseable or unsupported files are skipped (log and continue) rather than failing the whole run.

These assumptions keep the minimum system well-defined and testable on dbt Jaffle Shop and the Cartographer repo itself, while leaving room to relax or extend them for the final submission (e.g. more dialects, deeper dbt ref() resolution, or incremental analysis).
