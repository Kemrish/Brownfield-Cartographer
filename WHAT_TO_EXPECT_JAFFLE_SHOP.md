# What to Expect: `uv run cartographer analyze https://github.com/dbt-labs/jaffle_shop`

## Command behavior

1. **Clone** — The repo is cloned into a **temporary directory** (e.g. `C:\Users\...\AppData\Local\Temp\cartographer_xxxxx` on Windows). Analysis runs on that clone.
2. **Surveyor** — Walks all `.py`, `.sql`, `.yml`/`.yaml` files; builds the module graph (imports, PageRank, git velocity, SCC, dead-code candidates); writes `.cartography/module_graph.json` **inside that temp directory**.
3. **Hydrologist** — Parses SQL (sqlglot), dbt schema YAML, and Python data read/write; builds the lineage graph; writes `.cartography/lineage_graph.json` **inside that temp directory**.
4. **Terminal output** — You see “Analysis complete”, the paths to the two JSON files, and (if any) tables for “Lineage sources” and “Lineage sinks”.

## Important: where the files go

- With **only** the URL (no `-o`), `.cartography/` is created **inside the temp clone**, not in your project folder.
- You’ll see the full path in the output (e.g. `C:\...\Temp\cartographer_xyz\.cartography\module_graph.json`). You can open that path to inspect files, but the temp dir may be removed by the OS later.
- To keep artifacts in a place you control, run:
  ```bash
  uv run cartographer analyze https://github.com/dbt-labs/jaffle_shop -o ./jaffle_shop_artifacts
  ```
  Then you’ll get `./jaffle_shop_artifacts/.cartography/module_graph.json` and `lineage_graph.json`.

## What you should see in the terminal

- `Analyzing: https://github.com/dbt-labs/jaffle_shop`
- `Analysis complete.`
- `Repo: <path>` (the temp clone path)
- `Module graph: <path>\.cartography\module_graph.json`
- `Lineage graph: <path>\.cartography\lineage_graph.json`
- A **Lineage sources** table (e.g. raw source tables / refs that have no upstream in our graph).
- A **Lineage sinks** table (e.g. final models that have no downstream in our graph).
- A tip line: *Repo was cloned to a temp dir. Use -o ./jaffle_shop_artifacts to write .cartography to a folder you can keep.*

## What should be in the artifacts (jaffle_shop)

- **module_graph.json** — Nodes for each file (e.g. `models/customers.sql`, `models/orders.sql`, `models/stg_customers.sql`, `dbt_project.yml`, macros, etc.), import edges between Python modules if any, PageRank scores, high-velocity files (if git history was cloned), and strongly connected components.
- **lineage_graph.json** — For jaffle_shop’s dbt models you should see:
  - **datasets** — Tables referenced in SQL and/or listed in schema.yml (e.g. `raw_customers`, `raw_orders`, `raw_payments`, and models like `customers`, `orders`, `stg_customers`, `stg_orders`, `stg_payments`).
  - **transformations** — One per SQL file (e.g. `sql:models/customers.sql`) with `source_datasets` and `target_datasets` derived from FROM/JOIN and the model name.
  - **sources** — Tables with no incoming edges (typically raw sources).
  - **sinks** — Tables with no outgoing edges (typically final marts like `customers`, `orders`).

If something fails (e.g. clone error, parse error), you’ll see a red `Error:` line and a non-zero exit.

## Recommended command for keeping results

```bash
uv run cartographer analyze https://github.com/dbt-labs/jaffle_shop -o ./jaffle_shop_artifacts
```

Then inspect:

- `jaffle_shop_artifacts/.cartography/module_graph.json`
- `jaffle_shop_artifacts/.cartography/lineage_graph.json`
