"""SQL and dbt table dependency extraction using sqlglot."""

import re
from pathlib import Path
from typing import Optional

import sqlglot
from sqlglot import exp

from src.models.schemas import DatasetNode, TransformationNode, GraphEdge, EdgeType


# Dialects to try (order matters for ambiguous syntax)
DIALECTS = ["duckdb", "snowflake", "bigquery", "postgres", "spark"]

# Regex patterns for dbt Jinja extraction
DBT_REF_PATTERN = re.compile(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", re.IGNORECASE)
DBT_SOURCE_PATTERN = re.compile(r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", re.IGNORECASE)
DBT_CONFIG_PATTERN = re.compile(r"\{\{\s*config\s*\([^)]*\)\s*\}\}", re.IGNORECASE)
DBT_VAR_PATTERN = re.compile(r"\{\{\s*var\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", re.IGNORECASE)


def extract_dbt_refs(source: str) -> list[str]:
    """Extract all ref('model_name') calls from dbt SQL/Jinja."""
    return DBT_REF_PATTERN.findall(source)


def extract_dbt_sources(source: str) -> list[tuple[str, str]]:
    """Extract all source('source_name', 'table_name') calls. Returns list of (source, table)."""
    return DBT_SOURCE_PATTERN.findall(source)


def is_dbt_macro_file(path: str, source: str) -> bool:
    """Check if file is a dbt macro (not a model)."""
    path_lower = path.lower()
    if "/macros/" in path_lower or "\\macros\\" in path_lower:
        return True
    if "{%- macro" in source or "{% macro" in source:
        return True
    return False


def strip_jinja_for_sqlglot(source: str) -> str:
    """Replace Jinja template blocks with placeholder table names so sqlglot can parse."""
    result = source
    for match in DBT_REF_PATTERN.finditer(source):
        model_name = match.group(1)
        result = result.replace(match.group(0), f"__dbt_ref__{model_name}")
    for match in DBT_SOURCE_PATTERN.finditer(source):
        src_name, tbl_name = match.group(1), match.group(2)
        result = result.replace(match.group(0), f"__dbt_source__{src_name}__{tbl_name}")
    result = DBT_CONFIG_PATTERN.sub("", result)
    result = re.sub(r"\{\{[^}]*\}\}", "1", result)  # replace other Jinja with literal
    result = re.sub(r"\{%[^%]*%\}", "", result)     # remove Jinja blocks
    result = re.sub(r"\{#[^#]*#\}", "", result)     # remove Jinja comments
    return result


class SQLLineageAnalyzer:
    """Extract table dependencies from SQL files using sqlglot, with dbt Jinja support."""

    def __init__(self, dialect: str = "postgres") -> None:
        self.dialect = dialect

    def parse_sql(self, source: str, path: str = "") -> Optional[sqlglot.Expression]:
        """Parse SQL string; return root expression or None."""
        clean = strip_jinja_for_sqlglot(source)
        try:
            parsed = sqlglot.parse(clean, dialect=self.dialect)
            if parsed and len(parsed) > 0:
                return parsed[0]
        except Exception:
            pass
        return None

    def extract_table_references(self, expression: sqlglot.Expression) -> list[str]:
        """Collect all table/identifier references from SELECT/FROM/JOIN/CTE."""
        tables: list[str] = []
        for table in expression.find_all(exp.Table):
            name = table.sql(dialect=self.dialect)
            if name:
                tables.append(name)
        for alias in expression.find_all(exp.TableAlias):
            if alias.this:
                name = alias.this.sql(dialect=self.dialect)
                if name:
                    tables.append(name)
        return list(dict.fromkeys(tables))

    def extract_cte_names(self, expression: sqlglot.Expression) -> list[str]:
        """Get CTE names from WITH clause."""
        ctes: list[str] = []
        for cte in expression.find_all(exp.CTE):
            if cte.alias:
                ctes.append(cte.alias)
        return ctes

    def extract_dependencies(self, source: str, path: str = "") -> tuple[list[str], list[str], list[str], list[str]]:
        """
        Return (sources, targets, dbt_refs, dbt_sources_full) for lineage.
        - sources: table names from SQL parsing
        - targets: output table names
        - dbt_refs: model names from ref() calls
        - dbt_sources_full: "source.table" names from source() calls
        """
        # First extract dbt-specific refs and sources from raw source (before Jinja strip)
        dbt_refs = extract_dbt_refs(source)
        dbt_sources_raw = extract_dbt_sources(source)
        dbt_sources_full = [f"{s[0]}.{s[1]}" for s in dbt_sources_raw]

        root = self.parse_sql(source, path)
        sources: list[str] = []
        targets: list[str] = []

        if root is None:
            # Even if sqlglot fails, we have dbt refs/sources
            return list(dict.fromkeys(dbt_refs + dbt_sources_full)), targets, dbt_refs, dbt_sources_full

        # INSERT INTO / CREATE TABLE AS / OVERWRITE → target
        if isinstance(root, exp.Insert):
            if root.this:
                targets.append(root.this.sql(dialect=self.dialect))
            for t in root.find_all(exp.Table):
                if t not in (root.this,):
                    tname = t.sql(dialect=self.dialect)
                    if not tname.startswith("__dbt_"):
                        sources.append(tname)
            combined_sources = list(dict.fromkeys(sources + dbt_refs + dbt_sources_full))
            return combined_sources, list(dict.fromkeys(targets)), dbt_refs, dbt_sources_full

        if isinstance(root, exp.Create):
            if root.this:
                targets.append(root.this.sql(dialect=self.dialect))
            for t in root.find_all(exp.Table):
                if t != getattr(root, "this", None):
                    tname = t.sql(dialect=self.dialect)
                    if not tname.startswith("__dbt_"):
                        sources.append(tname)
            combined_sources = list(dict.fromkeys(sources + dbt_refs + dbt_sources_full))
            return combined_sources, list(dict.fromkeys(targets)), dbt_refs, dbt_sources_full

        # SELECT: all referenced tables are "sources"
        all_tables = self.extract_table_references(root)
        ctes = self.extract_cte_names(root)
        for t in all_tables:
            if t not in ctes and not t.startswith("__dbt_"):
                sources.append(t)

        combined_sources = list(dict.fromkeys(sources + dbt_refs + dbt_sources_full))
        return combined_sources, list(dict.fromkeys(targets)), dbt_refs, dbt_sources_full

    def analyze_file(
        self,
        path: str | Path,
        source: str,
        model_name_hint: Optional[str] = None,
    ) -> tuple[list[DatasetNode], list[TransformationNode], list[GraphEdge]]:
        """
        Analyze one SQL file and return datasets, one transformation node, and edges.
        model_name_hint: e.g. dbt model name (filename without .sql) to use as output.
        """
        path_str = str(Path(path).as_posix())
        is_macro = is_dbt_macro_file(path_str, source)

        # Skip macros from lineage (they're not transformations)
        if is_macro:
            return [], [], []

        sources, targets, dbt_refs, dbt_sources_full = self.extract_dependencies(source, path_str)

        # For dbt models without explicit target, use filename as target
        if model_name_hint and not targets:
            targets = [model_name_hint]

        trans_id = f"sql:{path_str}"
        nodes_ds: list[DatasetNode] = []
        nodes_trans: list[TransformationNode] = []
        edges: list[GraphEdge] = []

        # Create dataset nodes for sources
        for s in sources:
            is_dbt_source = "." in s and s in dbt_sources_full
            parts = s.split(".") if is_dbt_source else [None, s]
            nodes_ds.append(
                DatasetNode(
                    name=s,
                    storage_type="table",
                    source_file=path_str,
                    is_external_source=is_dbt_source,
                    dbt_source_name=parts[0] if is_dbt_source else None,
                    dbt_ref_name=s if s in dbt_refs else None,
                )
            )
            edges.append(
                GraphEdge(source=s, target=trans_id, edge_type=EdgeType.CONSUMES, metadata={"source_file": path_str})
            )

        # Create dataset nodes for targets
        for t in targets:
            nodes_ds.append(
                DatasetNode(
                    name=t,
                    storage_type="table",
                    source_file=path_str,
                )
            )
            edges.append(
                GraphEdge(source=trans_id, target=t, edge_type=EdgeType.PRODUCES, metadata={"source_file": path_str})
            )

        nodes_trans.append(
            TransformationNode(
                id=trans_id,
                source_datasets=sources,
                target_datasets=targets,
                transformation_type="dbt" if (dbt_refs or dbt_sources_full) else "sql",
                source_file=path_str,
                sql_query_if_applicable=source[:2000],
                dbt_refs=dbt_refs,
                dbt_sources=dbt_sources_full,
                is_macro=is_macro,
            )
        )
        return nodes_ds, nodes_trans, edges
