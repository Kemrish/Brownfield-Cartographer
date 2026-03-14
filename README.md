# Brownfield Cartographer

Multi-agent codebase intelligence system for rapid FDE (Forward-Deployed Engineering) onboarding. Analyzes repositories to produce a **module graph** (imports, PageRank, network metrics), a **data lineage graph** (SQL, Python, YAML, column-level), and **living documentation** (CODEBASE.md, onboarding_brief.md).

## Install

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
cd Brownfield-Cartographer
uv sync
```

For development and tests:

```bash
uv sync --extra dev
```

Or with pip: `pip install -e .`

## Run analysis

Analyze a **local repository**:

```bash
uv run cartographer analyze /path/to/your/repo
```

Or a **GitHub URL** (cloned to a temp directory):

```bash
uv run cartographer analyze https://github.com/dbt-labs/jaffle-shop -o ./artifacts
```

### Output artifacts (`.cartography/`)

| Artifact | Description |
|----------|-------------|
| `module_graph.json` | Module nodes, import edges, PageRank, SCCs, hub modules, **network metrics** (betweenness, communities, degree stats) |
| `lineage_graph.json` | Datasets, transformations, sources, sinks, critical path, **column lineage**, **network metrics** |
| `CODEBASE.md` | Living map: lineage overview, critical path, module map by domain, hub modules |
| `onboarding_brief.md` | FDE Day-One quick start: ingestion path, outputs, blast radius, where to start reading |
| `cartography_trace.jsonl` | Audit log of analysis steps and stats |
| `module_graph.graphml` | Module graph in GraphML (Gephi, Cytoscape) |
| `lineage_graph.graphml` | Lineage graph in GraphML |

### Options

- `-o, --output-dir PATH` — Write `.cartography/` under this directory.
- `-f, --full-history` — Clone full git history (enables git velocity).

## Query the cartography

After running `analyze`, query the artifacts in natural language:

```bash
uv run cartographer query ./artifacts "sources"
uv run cartographer query ./artifacts "sinks"
uv run cartographer query ./artifacts "critical path"
uv run cartographer query ./artifacts "blast radius of stg_orders"
uv run cartographer query ./artifacts "hub modules"
uv run cartographer query ./artifacts "column lineage for customers"
```

Without a question, the CLI prints example queries.

## Visualize graphs (Pyvis)

Generate interactive HTML graphs for module and lineage (requires `pyvis`):

```bash
uv sync --extra viz
uv run python visualize_network.py
```

By default this writes `module_graph.html` and `lineage_graph.html` into `jaffle_shop_llm/.cartography/` and `jaffle_shop_no_llm/.cartography/`. To target one folder:

```bash
uv run python visualize_network.py jaffle_shop_llm/.cartography
```

Open the generated `.html` files in a browser to explore the graphs.

## Export graphs (GraphML)

Export module and lineage graphs for visualization in Gephi or Cytoscape:

```bash
uv run cartographer export-graphml /path/to/repo
uv run cartographer export-graphml /path/to/.cartography -o ./exports
```

## Four-agent pipeline

1. **Surveyor** — Static structure: tree-sitter AST (Python, SQL, YAML), module import graph, PageRank, git velocity (30d), dead-code candidates, entry points.
2. **Hydrologist** — Data lineage: sqlglot SQL parsing, dbt `ref()`/`source()` extraction, Python I/O heuristics, YAML configs, **column-level lineage**, critical path.
3. **Semanticist** — Infers purpose statements and domain clusters (LLM or heuristics), doc-drift detection, ContextWindowBudget (token budget).
4. **Archivist** — Generates CODEBASE.md and onboarding_brief.md from the graphs; trace logging per artifact.

The orchestrator runs **Surveyor → Hydrologist → Semanticist → Archivist**, writes JSON and GraphML, then writes the trace. Incremental mode (`--incremental`) re-analyzes only changed files (git diff since last run).

## Network analysis

Both graphs include **network metrics**:

- **Module graph**: PageRank top-N, betweenness centrality top-N, greedy modularity communities, in/out degree stats.
- **Lineage graph**: Betweenness top-N, communities, degree stats.

Use the metrics in `module_graph.json` and `lineage_graph.json` under `network_metrics` for impact analysis and clustering.

## Tests

```bash
uv run pytest tests/ -v
uv run pytest tests/ --cov=src --cov-report=term-missing
```

## Project layout

```
src/
  cli.py              # Entry point: analyze, query, export-graphml
  orchestrator.py     # Surveyor → Semanticist → Hydrologist → Archivist, trace
  models/             # Pydantic schemas (nodes, edges, ModuleGraph, LineageGraph, NetworkMetrics, ColumnLineageEdge)
  analyzers/          # tree_sitter_analyzer, sql_lineage (column lineage), dag_config_parser, analyzer_service
  agents/             # surveyor, semanticist, hydrologist, archivist, navigator
  graph/              # knowledge_graph (NetworkX, metrics, GraphML), trace_writer
tests/
  test_semanticist.py
  test_navigator.py
  test_sql_lineage.py
  test_network_metrics.py
```

## License

As per course / project terms.
