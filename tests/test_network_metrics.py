"""Tests for network metrics computation."""

import pytest
import networkx as nx

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode


def test_compute_module_network_metrics():
    kg = KnowledgeGraph()
    kg.add_module_node(ModuleNode(path="a.py", language="python"))
    kg.add_module_node(ModuleNode(path="b.py", language="python"))
    kg.add_module_node(ModuleNode(path="c.py", language="python"))
    kg.add_import_edge("a.py", "b.py")
    kg.add_import_edge("b.py", "c.py")
    kg.add_import_edge("a.py", "c.py")
    m = kg.compute_module_network_metrics(top_n=5)
    assert m.module_node_count == 3
    assert m.module_edge_count == 3
    assert len(m.module_pagerank_top) <= 5
    assert len(m.module_betweenness_top) <= 5


def test_compute_lineage_network_metrics():
    kg = KnowledgeGraph()
    from src.models.schemas import DatasetNode, TransformationNode
    kg.add_lineage_dataset(DatasetNode(name="raw", storage_type="table"))
    kg.add_lineage_dataset(DatasetNode(name="stg", storage_type="table"))
    kg.add_lineage_dataset(DatasetNode(name="mart", storage_type="table"))
    kg.add_lineage_transformation(
        TransformationNode(id="t1", source_datasets=["raw"], target_datasets=["stg"], transformation_type="sql", source_file="x.sql")
    )
    kg.add_lineage_transformation(
        TransformationNode(id="t2", source_datasets=["stg"], target_datasets=["mart"], transformation_type="sql", source_file="y.sql")
    )
    m = kg.compute_lineage_network_metrics(top_n=5)
    assert m.lineage_node_count == 5  # 3 datasets + 2 transformations
    assert m.lineage_edge_count == 4
    assert len(m.lineage_betweenness_top) <= 5
