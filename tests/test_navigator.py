"""Tests for Navigator query agent."""

import json
import tempfile
from pathlib import Path

import pytest

from src.agents.navigator import Navigator


@pytest.fixture
def sample_cartography_dir():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d)
        (path / "module_graph.json").write_text(
            json.dumps({
                "nodes": [],
                "hub_modules": ["src/main.py"],
                "entry_points": ["src/cli.py"],
            }),
            encoding="utf-8",
        )
        (path / "lineage_graph.json").write_text(
            json.dumps({
                "sources": ["ecom.raw_orders", "ecom.raw_customers"],
                "sinks": ["orders", "customers"],
                "critical_path": ["ecom.raw_orders", "stg_orders", "orders"],
                "edges": [
                    {"source": "ecom.raw_orders", "target": "sql:x"},
                    {"source": "sql:x", "target": "stg_orders"},
                    {"source": "stg_orders", "target": "sql:y"},
                    {"source": "sql:y", "target": "orders"},
                ],
                "column_lineage": [],
            }),
            encoding="utf-8",
        )
        yield path


def test_navigator_sources(sample_cartography_dir):
    nav = Navigator(sample_cartography_dir)
    assert set(nav.sources()) == {"ecom.raw_orders", "ecom.raw_customers"}


def test_navigator_sinks(sample_cartography_dir):
    nav = Navigator(sample_cartography_dir)
    assert set(nav.sinks()) == {"orders", "customers"}


def test_navigator_critical_path(sample_cartography_dir):
    nav = Navigator(sample_cartography_dir)
    assert nav.critical_path() == ["ecom.raw_orders", "stg_orders", "orders"]


def test_navigator_blast_radius_downstream(sample_cartography_dir):
    nav = Navigator(sample_cartography_dir)
    radius = nav.blast_radius_nodes("stg_orders", direction="downstream")
    assert "orders" in radius


def test_navigator_query_sources(sample_cartography_dir):
    nav = Navigator(sample_cartography_dir)
    answer = nav.query("what are the sources?")
    assert "raw_orders" in answer or "sources" in answer.lower()


def test_navigator_query_sinks(sample_cartography_dir):
    nav = Navigator(sample_cartography_dir)
    answer = nav.query("sinks")
    assert "orders" in answer or "customers" in answer
