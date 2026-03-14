"""Hydrologist agent: data lineage graph, blast_radius, find_sources/find_sinks."""

import re
from pathlib import Path

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.analyzers.dag_config_parser import DAGConfigParser
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DatasetNode, TransformationNode, ConfigNode, EdgeType, ColumnLineageEdge


def _extract_python_data_refs(source: str, path: str) -> tuple[list[str], list[str]]:
    """Heuristic: find string literals in read_csv/read_sql/to_sql etc. calls. Returns (sources, targets)."""
    sources: list[str] = []
    targets: list[str] = []

    # pandas read operations
    read_calls = re.findall(r"pd\.read_(?:csv|parquet|sql|table|excel|json)\s*\(\s*[\"']([^\"']+)[\"']", source, re.IGNORECASE)
    read_calls += re.findall(r"\.read_(?:csv|parquet|sql|table|excel|json)\s*\(\s*[\"']([^\"']+)[\"']", source, re.IGNORECASE)

    # spark read operations
    spark_reads = re.findall(r'\.load\s*\(\s*["\']([^"\']+)["\']', source)
    spark_reads += re.findall(r'\.table\s*\(\s*["\']([^"\']+)["\']', source)

    for s in read_calls + spark_reads:
        s = s.strip()
        if s and not s.startswith("http") and not s.startswith("{") and "/.venv/" not in path:
            sources.append(s)

    # pandas write operations
    write_calls = re.findall(r"\.to_(?:csv|parquet|sql|excel|json)\s*\(\s*[\"']([^\"']+)[\"']", source, re.IGNORECASE)

    # spark write operations
    spark_writes = re.findall(r'\.save\s*\(\s*["\']([^"\']+)["\']', source)
    spark_writes += re.findall(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']', source)
    spark_writes += re.findall(r'\.insertInto\s*\(\s*["\']([^"\']+)["\']', source)

    for t in write_calls + spark_writes:
        t = t.strip()
        if t and "/.venv/" not in path:
            targets.append(t)

    return list(dict.fromkeys(sources)), list(dict.fromkeys(targets))


class Hydrologist:
    """Data flow and lineage analyst: Python + SQL + YAML → DataLineageGraph."""

    def __init__(self, repo_root: str | Path) -> None:
        self.repo_root = Path(repo_root)
        self.sql_analyzer = SQLLineageAnalyzer()
        self.dag_parser = DAGConfigParser()
        self.tree_sitter = TreeSitterAnalyzer()
        self.graph = KnowledgeGraph()
        self.configs: list[ConfigNode] = []
        self.column_lineage: list[ColumnLineageEdge] = []
        self._stats = {"sql_files": 0, "python_files": 0, "yaml_files": 0, "dbt_refs_found": 0, "column_lineage_edges": 0}

    def _add_sql_file(self, path: Path) -> None:
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return
        path_posix = path.relative_to(self.repo_root).as_posix()
        model_name = path.stem  # dbt model name
        nodes_ds, nodes_trans, edges, col_lineage = self.sql_analyzer.analyze_file(path, source, model_name_hint=model_name)

        self._stats["sql_files"] += 1
        self.column_lineage.extend(col_lineage)
        self._stats["column_lineage_edges"] = len(self.column_lineage)

        for n in nodes_ds:
            self.graph.add_lineage_dataset(n)
        for t in nodes_trans:
            self.graph.add_lineage_transformation(t)
            self._stats["dbt_refs_found"] += len(t.dbt_refs) + len(t.dbt_sources)
    def _add_python_data_flow(self, path: Path) -> None:
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return
        path_posix = path.relative_to(self.repo_root).as_posix()
        sources, targets = _extract_python_data_refs(source, path_posix)
        if not sources and not targets:
            return

        self._stats["python_files"] += 1
        trans_id = f"python:{path_posix}"

        for s in sources:
            self.graph.add_lineage_dataset(
                DatasetNode(name=s, storage_type="file", source_file=path_posix)
            )
        for t in targets:
            self.graph.add_lineage_dataset(
                DatasetNode(name=t, storage_type="file", source_file=path_posix)
            )
        self.graph.add_lineage_transformation(
            TransformationNode(
                id=trans_id,
                source_datasets=sources,
                target_datasets=targets,
                transformation_type="python",
                source_file=path_posix,
            )
        )

    def _add_yaml_config(self, path: Path) -> None:
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return

        self._stats["yaml_files"] += 1

        # Handle dbt_project.yml separately
        if path.name == "dbt_project.yml":
            cfg = self.dag_parser.parse_dbt_project_yml(path, source)
            if cfg:
                self.configs.append(cfg)
            return

        nodes, edges, configs = self.dag_parser.parse_dbt_schema_yml(path, source)
        for n in nodes:
            self.graph.add_lineage_dataset(n)
        for e in edges:
            self.graph.lineage_digraph.add_edge(e.source, e.target, edge_type=e.edge_type)
        self.configs.extend(configs)

    def run(self) -> KnowledgeGraph:
        """Build lineage graph from SQL, Python, and YAML under repo root."""
        skip_dirs = {".git", "node_modules", ".venv", "venv", "__pycache__", "target", "dbt_packages"}

        def should_skip(rel_path: Path) -> bool:
            return any(part in skip_dirs for part in rel_path.parts)

        # SQL files (dbt models, raw SQL)
        for p in self.repo_root.rglob("*.sql"):
            try:
                rel = p.relative_to(self.repo_root)
                if should_skip(rel):
                    continue
                self._add_sql_file(p)
            except Exception:
                pass

        # Python (data read/write)
        for p in self.repo_root.rglob("*.py"):
            try:
                rel = p.relative_to(self.repo_root)
                if should_skip(rel):
                    continue
                self._add_python_data_flow(p)
            except Exception:
                pass

        # dbt / YAML configs
        for ext in ("*.yml", "*.yaml"):
            for p in self.repo_root.rglob(ext):
                try:
                    rel = p.relative_to(self.repo_root)
                    if should_skip(rel):
                        continue
                    self._add_yaml_config(p)
                except Exception:
                    pass

        return self.graph

    def compute_critical_path(self) -> list[str]:
        """Find the longest path in the lineage DAG (critical path)."""
        import networkx as nx
        try:
            if not nx.is_directed_acyclic_graph(self.graph.lineage_digraph):
                return []
            return nx.dag_longest_path(self.graph.lineage_digraph)
        except Exception:
            return []

    def get_stats(self) -> dict:
        """Return analysis statistics."""
        return {
            **self._stats,
            "total_datasets": len(self.graph.lineage_datasets),
            "total_transformations": len(self.graph.lineage_transformations),
            "total_edges": self.graph.lineage_digraph.number_of_edges(),
            "sources_count": len(self.find_sources()),
            "sinks_count": len(self.find_sinks()),
        }

    def blast_radius(self, node: str, direction: str = "downstream") -> list[str]:
        """What would break if this node changed? Downstream = dependents."""
        return self.graph.blast_radius(node, direction=direction)

    def find_sources(self) -> list[str]:
        """Entry points: in-degree 0."""
        return self.graph.lineage_sources()

    def find_sinks(self) -> list[str]:
        """Exit points: out-degree 0."""
        return self.graph.lineage_sinks()

    def write_lineage_graph(self, out_dir: str | Path, metadata: dict | None = None) -> str:
        """Write .cartography/lineage_graph.json. Returns path to file."""
        out_path = Path(out_dir) / ".cartography" / "lineage_graph.json"
        critical_path = self.compute_critical_path()
        self.graph.write_lineage_graph_json(
            out_path,
            configs=self.configs,
            critical_path=critical_path,
            metadata=metadata,
            column_lineage=self.column_lineage,
        )
        return str(out_path)
