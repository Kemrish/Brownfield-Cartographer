# Onboarding Brief — FDE Day-One

**Target:** .

---

## 1. What is the primary data ingestion path?

*(Run cartographer to populate.)*

---

## 2. Critical output datasets
*(Run cartographer to populate.)*

---

## 3. Blast radius (if a key module fails)
*(Run cartographer to compute critical path.)*

---

## 4. Where to start reading code
- **Entry points:** `src/cli.py`, `src/analyzers/tree_sitter_analyzer.py`

---

## 5. Next steps

1. Read **CODEBASE.md** for the full module map and lineage.
2. Query the graph: `cartographer query <cartography_dir> <question>`
3. Inspect `.cartography/module_graph.json` and `lineage_graph.json` for APIs.