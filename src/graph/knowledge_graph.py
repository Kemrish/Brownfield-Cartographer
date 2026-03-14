"""NetworkX-based knowledge graph with serialization to .cartography/."""

import json
from pathlib import Path
from enum import Enum
from typing import Any, Optional

import networkx as nx

from src.models.schemas import (
    AnalysisMetadata,
    ModuleNode,
    ModuleGraph,
    LineageGraph,
    GraphEdge,
    EdgeType,
    DatasetNode,
    TransformationNode,
    ConfigNode,
    NetworkMetrics,
    ColumnLineageEdge,
)


class KnowledgeGraph:
    """Wrapper around NetworkX for module graph and lineage graph with serialization."""

    def __init__(self) -> None:
        self.module_digraph: nx.DiGraph = nx.DiGraph()
        self.lineage_digraph: nx.DiGraph = nx.DiGraph()
        self.module_nodes: dict[str, ModuleNode] = {}
        self.lineage_datasets: dict[str, DatasetNode] = {}
        self.lineage_transformations: dict[str, TransformationNode] = {}
        self.lineage_configs: list[ConfigNode] = []

    def add_module_node(self, node: ModuleNode) -> None:
        self.module_nodes[node.path] = node
        self.module_digraph.add_node(node.path, **node.model_dump())

    def add_import_edge(self, source_path: str, target_path: str, weight: float = 1.0) -> None:
        if self.module_digraph.has_edge(source_path, target_path):
            self.module_digraph[source_path][target_path]["weight"] = (
                self.module_digraph[source_path][target_path].get("weight", 0) + weight
            )
        else:
            self.module_digraph.add_edge(source_path, target_path, weight=weight)

    def compute_module_pagerank(self) -> dict[str, float]:
        try:
            return nx.pagerank(self.module_digraph, weight="weight")
        except Exception:
            return dict.fromkeys(self.module_digraph.nodes(), 0.0)

    def compute_strongly_connected_components(self) -> list[list[str]]:
        try:
            return list(nx.strongly_connected_components(self.module_digraph))
        except Exception:
            return []

    def compute_module_network_metrics(self, top_n: int = 15) -> NetworkMetrics:
        """Compute centrality and community metrics for the module graph only."""
        metrics = NetworkMetrics()
        G = self.module_digraph
        metrics.module_node_count = G.number_of_nodes()
        metrics.module_edge_count = G.number_of_edges()
        try:
            pr = nx.pagerank(G, weight="weight")
            metrics.module_pagerank_top = sorted(pr.items(), key=lambda x: -x[1])[:top_n]
        except Exception:
            metrics.module_pagerank_top = []
        try:
            bc = nx.betweenness_centrality(G)
            metrics.module_betweenness_top = sorted(bc.items(), key=lambda x: -x[1])[:top_n]
        except Exception:
            metrics.module_betweenness_top = []
        try:
            from networkx.algorithms import community
            undir = G.to_undirected()
            comm = community.greedy_modularity_communities(undir)
            metrics.module_communities = [list(c) for c in comm if len(c) > 1]
        except Exception:
            metrics.module_communities = []
        in_deg = dict(G.in_degree())
        out_deg = dict(G.out_degree())
        if in_deg:
            v = list(in_deg.values())
            metrics.module_degree_stats.update({"in_degree_min": min(v), "in_degree_max": max(v), "in_degree_mean": sum(v) / len(v)})
        if out_deg:
            v = list(out_deg.values())
            metrics.module_degree_stats.update({"out_degree_min": min(v), "out_degree_max": max(v), "out_degree_mean": sum(v) / len(v)})
        return metrics

    def compute_lineage_network_metrics(self, top_n: int = 15) -> NetworkMetrics:
        """Compute centrality and community metrics for the lineage graph only."""
        metrics = NetworkMetrics()
        G = self.lineage_digraph
        metrics.lineage_node_count = G.number_of_nodes()
        metrics.lineage_edge_count = G.number_of_edges()
        try:
            bc = nx.betweenness_centrality(G)
            metrics.lineage_betweenness_top = sorted(bc.items(), key=lambda x: -x[1])[:top_n]
        except Exception:
            metrics.lineage_betweenness_top = []
        try:
            from networkx.algorithms import community
            undir = G.to_undirected()
            comm = community.greedy_modularity_communities(undir)
            metrics.lineage_communities = [list(c) for c in comm if len(c) > 1]
        except Exception:
            metrics.lineage_communities = []
        in_deg = dict(G.in_degree())
        out_deg = dict(G.out_degree())
        if in_deg:
            v = list(in_deg.values())
            metrics.lineage_degree_stats.update({"in_degree_min": min(v), "in_degree_max": max(v), "in_degree_mean": sum(v) / len(v)})
        if out_deg:
            v = list(out_deg.values())
            metrics.lineage_degree_stats.update({"out_degree_min": min(v), "out_degree_max": max(v), "out_degree_mean": sum(v) / len(v)})
        return metrics

    def compute_network_metrics(
        self,
        module_top_n: int = 15,
        lineage_top_n: int = 15,
    ) -> NetworkMetrics:
        """Compute metrics for both graphs (use when both are populated in same instance)."""
        m = self.compute_module_network_metrics(module_top_n)
        l = self.compute_lineage_network_metrics(lineage_top_n)
        m.lineage_node_count = l.lineage_node_count
        m.lineage_edge_count = l.lineage_edge_count
        m.lineage_betweenness_top = l.lineage_betweenness_top
        m.lineage_communities = l.lineage_communities
        m.lineage_degree_stats = l.lineage_degree_stats
        return m

    def write_graphml(self, path: str | Path, graph_type: str = "module") -> None:
        """Export graph to GraphML for visualization (e.g. Gephi, Cytoscape).
        Uses only GraphML-safe attributes (string, int, float, bool); lists/dicts as strings.
        Lineage node IDs (e.g. sql:C:\\path) are sanitized so XML id attributes stay valid.
        """
        G_raw = self.module_digraph if graph_type == "module" else self.lineage_digraph
        if G_raw.number_of_nodes() == 0:
            return

        # Lineage nodes often have ids like "sql:C:\path\to\file.sql" - colons/backslashes
        # break XML id attributes. Use a safe id for GraphML and keep original in "label".
        def _safe_id(node_id: str, seen: Optional[set[str]] = None) -> str:
            if graph_type != "lineage":
                return node_id
            # GraphML/XML id must be a valid NCName (no : or \ in value for many parsers)
            s = str(node_id).replace("\\", "_").replace(":", "_").replace("/", "_")
            s = s[:200] if len(s) > 200 else s
            if seen is not None:
                if s in seen:
                    s = f"{s}_{id(node_id) & 0xFFFF}"  # disambiguate collision
                seen.add(s)
            return s

        def _safe_val(v: Any) -> str | int | float | bool:
            if v is None:
                return ""
            if isinstance(v, Enum):
                return str(v.value) if hasattr(v, "value") else str(v.name)
            if isinstance(v, (str, int, float, bool)):
                return v
            if isinstance(v, (list, dict)):
                return str(v)[:500]
            return str(v)

        G = nx.DiGraph()
        seen_ids: set[str] = set()
        id_map: dict[str, str] = {}
        for n in G_raw.nodes():
            attrs = dict(G_raw.nodes[n])
            safe = {k: _safe_val(v) for k, v in attrs.items()}
            safe["label"] = str(n)[:300]  # human-readable label
            node_id = _safe_id(n, seen_ids)
            id_map[n] = node_id
            G.add_node(node_id, **safe)
        # id_map: original -> safe for edges
        for u, v in G_raw.edges():
            data = dict(G_raw.edges[u, v])
            safe_data = {k: _safe_val(val) for k, val in data.items()}
            u_safe = id_map.get(u, _safe_id(u))
            v_safe = id_map.get(v, _safe_id(v))
            if u_safe in G and v_safe in G:
                G.add_edge(u_safe, v_safe, **safe_data)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        nx.write_graphml(G, path, encoding="utf-8")

    def add_lineage_dataset(self, node: DatasetNode) -> None:
        self.lineage_datasets[node.name] = node
        self.lineage_digraph.add_node(node.name, kind="dataset", **node.model_dump())

    def add_lineage_transformation(self, node: TransformationNode) -> None:
        self.lineage_transformations[node.id] = node
        self.lineage_digraph.add_node(node.id, kind="transformation", **node.model_dump())
        for s in node.source_datasets:
            self.lineage_digraph.add_edge(s, node.id, edge_type=EdgeType.CONSUMES)
        for t in node.target_datasets:
            self.lineage_digraph.add_edge(node.id, t, edge_type=EdgeType.PRODUCES)

    def lineage_sources(self) -> list[str]:
        """Nodes with in-degree 0 (entry points)."""
        return [n for n in self.lineage_digraph.nodes() if self.lineage_digraph.in_degree(n) == 0]

    def lineage_sinks(self) -> list[str]:
        """Nodes with out-degree 0 (exit points)."""
        return [n for n in self.lineage_digraph.nodes() if self.lineage_digraph.out_degree(n) == 0]

    def blast_radius(self, node: str, direction: str = "downstream") -> list[str]:
        """All nodes reachable from this node (downstream) or that can reach it (upstream)."""
        if node not in self.lineage_digraph:
            return []
        if direction == "downstream":
            return list(nx.descendants(self.lineage_digraph, node))
        return list(nx.ancestors(self.lineage_digraph, node))

    def to_module_graph_model(
        self,
        pagerank: dict[str, float],
        scc: list[list[str]],
        high_velocity: list[str],
        entry_points: Optional[list[str]] = None,
        dead_code_candidates: Optional[list[str]] = None,
        hub_modules: Optional[list[str]] = None,
        metadata: Optional[AnalysisMetadata] = None,
        network_metrics: Optional[NetworkMetrics] = None,
    ) -> ModuleGraph:
        nodes = list(self.module_nodes.values())
        edges: list[GraphEdge] = []
        for u, v, data in self.module_digraph.edges(data=True):
            edges.append(
                GraphEdge(source=u, target=v, edge_type=EdgeType.IMPORTS, weight=data.get("weight"))
            )
        return ModuleGraph(
            metadata=metadata,
            nodes=nodes,
            edges=edges,
            pagerank=pagerank,
            strongly_connected_components=[[n for n in comp] for comp in scc],
            high_velocity_files=high_velocity,
            entry_points=entry_points or [],
            dead_code_candidates=dead_code_candidates or [],
            hub_modules=hub_modules or [],
            network_metrics=network_metrics,
        )

    def to_lineage_graph_model(
        self,
        configs: Optional[list[ConfigNode]] = None,
        critical_path: Optional[list[str]] = None,
        metadata: Optional[AnalysisMetadata] = None,
        column_lineage: Optional[list[ColumnLineageEdge]] = None,
        network_metrics: Optional[NetworkMetrics] = None,
    ) -> LineageGraph:
        edges: list[GraphEdge] = []
        for u, v, data in self.lineage_digraph.edges(data=True):
            et = data.get("edge_type", EdgeType.CONSUMES)
            edges.append(GraphEdge(source=u, target=v, edge_type=et))
        return LineageGraph(
            metadata=metadata,
            datasets=list(self.lineage_datasets.values()),
            transformations=list(self.lineage_transformations.values()),
            configs=configs or self.lineage_configs,
            edges=edges,
            sources=self.lineage_sources(),
            sinks=self.lineage_sinks(),
            critical_path=critical_path or [],
            column_lineage=column_lineage or [],
            network_metrics=network_metrics,
        )

    def write_module_graph_json(
        self,
        path: str | Path,
        high_velocity: Optional[list[str]] = None,
        entry_points: Optional[list[str]] = None,
        dead_code_candidates: Optional[list[str]] = None,
        hub_modules: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        network_metrics: Optional[NetworkMetrics] = None,
    ) -> None:
        pagerank = self.compute_module_pagerank()
        scc = self.compute_strongly_connected_components()
        scc_filtered = [c for c in scc if len(c) > 1]  # only non-trivial
        net_metrics = network_metrics or self.compute_module_network_metrics()

        meta = None
        if metadata:
            meta = AnalysisMetadata(**metadata)

        model = self.to_module_graph_model(
            pagerank,
            scc_filtered,
            high_velocity or [],
            entry_points=entry_points,
            dead_code_candidates=dead_code_candidates,
            hub_modules=hub_modules,
            metadata=meta,
            network_metrics=net_metrics,
        )
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(model.model_dump(), f, indent=2, default=str)

    def write_lineage_graph_json(
        self,
        path: str | Path,
        configs: Optional[list[ConfigNode]] = None,
        critical_path: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        column_lineage: Optional[list[ColumnLineageEdge]] = None,
        network_metrics: Optional[NetworkMetrics] = None,
    ) -> None:
        meta = None
        if metadata:
            meta = AnalysisMetadata(**metadata)
        net_metrics = network_metrics or self.compute_lineage_network_metrics()

        model = self.to_lineage_graph_model(
            configs=configs,
            critical_path=critical_path,
            metadata=meta,
            column_lineage=column_lineage,
            network_metrics=net_metrics,
        )
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(model.model_dump(), f, indent=2, default=str)
