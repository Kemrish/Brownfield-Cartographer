# Brownfield Cartographer

Multi-agent codebase intelligence system for rapid FDE (Forward-Deployed Engineering) onboarding. Analyzes repositories to produce a **module graph** (imports, PageRank, git velocity) and a **data lineage graph** (SQL, Python, YAML).

## Install

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
cd Brownfield-Cartographer
uv sync
```

Or with pip:

```bash
pip install -e .
```

## Run analysis

Analyze a **local repository**:

```bash
uv run cartographer analyze /path/to/your/repo
```

Or a **GitHub URL** (cloned to a temp directory):

```bash
uv run cartographer analyze https://github.com/dbt-labs/jaffle_shop
```

Output is written under the repo (or `--output-dir` if set):

- `.cartography/module_graph.json` — module nodes, import edges, PageRank, strongly connected components, high-velocity files
- `.cartography/lineage_graph.json` — datasets, transformations, sources, sinks

### Options

- `-o, --output-dir PATH` — Write `.cartography/` under this directory instead of inside the repo.

## Interim scope (Week 4)

- **Surveyor**: tree-sitter AST (Python, SQL, YAML), module import graph, PageRank, git velocity (30d), dead-code candidates
- **Hydrologist**: SQL lineage (sqlglot), Python read/write heuristics, dbt schema YAML
- **Orchestrator**: Surveyor → Hydrologist, serializes to `.cartography/`

Final submission will add Semanticist, Archivist, Navigator (query agent), and living artifacts (CODEBASE.md, onboarding_brief.md, cartography_trace.jsonl).

## Project layout

```
src/
  cli.py              # Entry point: cartographer analyze [repo_path]
  orchestrator.py     # Surveyor → Hydrologist, write .cartography/
  models/             # Pydantic schemas (nodes, edges, graph types)
  analyzers/          # tree_sitter_analyzer, sql_lineage, dag_config_parser
  agents/             # surveyor, hydrologist
  graph/              # knowledge_graph (NetworkX + serialization)
```

## License

As per course / project terms.
