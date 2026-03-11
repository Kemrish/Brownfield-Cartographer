"""NetworkX-based knowledge graph with serialization to .cartography/."""

import json
from pathlib import Path
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
        )

    def to_lineage_graph_model(
        self,
        configs: Optional[list[ConfigNode]] = None,
        critical_path: Optional[list[str]] = None,
        metadata: Optional[AnalysisMetadata] = None,
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
        )

    def write_module_graph_json(
        self,
        path: str | Path,
        high_velocity: Optional[list[str]] = None,
        entry_points: Optional[list[str]] = None,
        dead_code_candidates: Optional[list[str]] = None,
        hub_modules: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        pagerank = self.compute_module_pagerank()
        scc = self.compute_strongly_connected_components()
        scc_filtered = [c for c in scc if len(c) > 1]  # only non-trivial

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
    ) -> None:
        meta = None
        if metadata:
            meta = AnalysisMetadata(**metadata)

        model = self.to_lineage_graph_model(
            configs=configs,
            critical_path=critical_path,
            metadata=meta,
        )
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(model.model_dump(), f, indent=2, default=str)
