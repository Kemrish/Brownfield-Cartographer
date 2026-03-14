"""Tests for Semanticist agent."""

import pytest
from pathlib import Path

from src.agents.semanticist import Semanticist, _infer_domain, _infer_purpose
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode


def test_infer_purpose_dbt_staging():
    node = ModuleNode(path="models/staging/stg_customers.sql", language="sql")
    assert _infer_purpose("models/staging/stg_customers.sql", node) == "dbt staging model"


def test_infer_purpose_dbt_marts():
    node = ModuleNode(path="models/marts/orders.sql", language="sql")
    assert _infer_purpose("models/marts/orders.sql", node) == "dbt mart model"


def test_infer_purpose_yaml_config():
    node = ModuleNode(path="dbt_project.yml", language="yaml", yaml_root_keys=["name", "version"])
    p = _infer_purpose("dbt_project.yml", node)
    assert "dbt project" in (p or "")


def test_infer_domain_ingestion():
    node = ModuleNode(path="staging/raw_orders.sql", language="sql", public_functions=[], classes=[])
    assert _infer_domain("staging/raw_orders.sql", node) == "ingestion"


def test_infer_domain_analytics():
    node = ModuleNode(path="models/marts/customers.sql", language="sql", public_functions=[], classes=[])
    assert _infer_domain("models/marts/customers.sql", node) == "analytics"


def test_semanticist_enrich_module():
    sem = Semanticist()
    node = ModuleNode(path="models/staging/stg_orders.sql", language="sql")
    enriched = sem.enrich_module_node("models/staging/stg_orders.sql", node)
    assert enriched.purpose_statement is not None
    assert enriched.domain_cluster is not None


def test_semanticist_run():
    kg = KnowledgeGraph()
    kg.add_module_node(ModuleNode(path="a.py", language="python", public_functions=["main"]))
    kg.add_module_node(ModuleNode(path="models/marts/x.sql", language="sql"))
    sem = Semanticist()
    sem.run(kg)
    assert kg.module_nodes["a.py"].purpose_statement is not None
    assert kg.module_nodes["models/marts/x.sql"].domain_cluster == "analytics"
