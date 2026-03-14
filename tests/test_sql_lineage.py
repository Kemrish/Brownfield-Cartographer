"""Tests for SQL lineage analyzer."""

import pytest
from pathlib import Path

from src.analyzers.sql_lineage import (
    SQLLineageAnalyzer,
    extract_dbt_refs,
    extract_dbt_sources,
    strip_jinja_for_sqlglot,
)


def test_extract_dbt_refs():
    sql = "SELECT * FROM {{ ref('stg_customers') }} JOIN {{ ref('orders') }}"
    assert extract_dbt_refs(sql) == ["stg_customers", "orders"]


def test_extract_dbt_sources():
    sql = "SELECT * FROM {{ source('ecom', 'raw_orders') }}"
    assert extract_dbt_sources(sql) == [("ecom", "raw_orders")]


def test_strip_jinja_preserves_refs():
    sql = "SELECT * FROM {{ ref('x') }}"
    out = strip_jinja_for_sqlglot(sql)
    assert "__dbt_ref__x" in out
    assert "ref(" not in out


def test_extract_dependencies_select():
    analyzer = SQLLineageAnalyzer()
    sql = "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
    sources, targets, refs, srcs = analyzer.extract_dependencies(sql, "")
    assert "t1" in sources or "t2" in sources
    assert targets == []


def test_extract_dependencies_with_dbt_ref():
    analyzer = SQLLineageAnalyzer()
    sql = "SELECT * FROM {{ ref('stg_orders') }}"
    sources, targets, refs, srcs = analyzer.extract_dependencies(sql, "")
    assert "stg_orders" in sources
    assert "stg_orders" in refs


def test_analyze_file_returns_four_tuple():
    analyzer = SQLLineageAnalyzer()
    sql = "SELECT id, name FROM {{ ref('customers') }}"
    nodes_ds, nodes_trans, edges, col_lineage = analyzer.analyze_file(Path("x.sql"), sql, model_name_hint="my_model")
    assert isinstance(nodes_ds, list)
    assert isinstance(nodes_trans, list)
    assert isinstance(edges, list)
    assert isinstance(col_lineage, list)
