"""Pydantic schemas for knowledge graph nodes, edges, and graph types."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# --- Analysis Metadata ---


class AnalysisMetadata(BaseModel):
    """Metadata about the analysis run."""

    repo_path: str
    analyzed_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    total_files: int = 0
    total_modules: int = 0
    total_datasets: int = 0
    total_transformations: int = 0
    languages_detected: list[str] = Field(default_factory=list)
    analysis_duration_seconds: float = 0.0
    cartographer_version: str = "0.1.0"


# --- Node Types ---


class ModuleNode(BaseModel):
    """A source file or module in the codebase."""

    path: str
    language: str
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None
    complexity_score: Optional[float] = None
    cyclomatic_complexity: Optional[int] = None
    lines_of_code: Optional[int] = None
    comment_ratio: Optional[float] = None
    change_velocity_30d: Optional[int] = None
    is_dead_code_candidate: bool = False
    is_entry_point: bool = False
    last_modified: Optional[str] = None
    imports: list[str] = Field(default_factory=list)
    public_functions: list[str] = Field(default_factory=list)
    classes: list[str] = Field(default_factory=list)
    decorators: list[str] = Field(default_factory=list)
    # SQL-specific fields (populated when language=sql)
    sql_statement_type: Optional[str] = None  # SELECT, INSERT, CREATE, etc.
    sql_tables_referenced: list[str] = Field(default_factory=list)
    sql_tables_written: list[str] = Field(default_factory=list)
    sql_ctes: list[str] = Field(default_factory=list)
    sql_joins: list[dict[str, str]] = Field(default_factory=list)
    sql_has_aggregation: bool = False
    sql_has_window_function: bool = False
    sql_subquery_count: int = 0
    # YAML-specific fields (populated when language=yaml)
    yaml_root_keys: list[str] = Field(default_factory=list)
    yaml_key_paths: list[str] = Field(default_factory=list)
    yaml_depth: int = 0
    yaml_list_keys: list[str] = Field(default_factory=list)


class DatasetNode(BaseModel):
    """A dataset, table, file, or stream in the data pipeline."""

    name: str
    storage_type: str  # table | file | stream | api
    schema_snapshot: Optional[dict[str, Any]] = None
    columns: list[str] = Field(default_factory=list)
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False
    is_external_source: bool = False
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    dbt_source_name: Optional[str] = None  # e.g., 'ecom' from source('ecom', 'raw_customers')
    dbt_ref_name: Optional[str] = None     # e.g., 'stg_customers' from ref('stg_customers')


class FunctionNode(BaseModel):
    """A function or method in the codebase."""

    qualified_name: str
    parent_module: str
    signature: Optional[str] = None
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = False


class TransformationNode(BaseModel):
    """A transformation step in the data lineage (produces/consumes datasets)."""

    id: str  # unique id for graph node
    source_datasets: list[str] = Field(default_factory=list)
    target_datasets: list[str] = Field(default_factory=list)
    transformation_type: str  # e.g. sql, python, dbt, airflow
    source_file: str
    line_range: Optional[tuple[int, int]] = None
    sql_query_if_applicable: Optional[str] = None
    dbt_refs: list[str] = Field(default_factory=list)      # ref() calls found
    dbt_sources: list[str] = Field(default_factory=list)   # source() calls found
    is_macro: bool = False


class ConfigNode(BaseModel):
    """A configuration file that configures pipelines or modules."""

    path: str
    config_type: str  # dbt_project | airflow_dag | schema_yml | sources_yml
    configures: list[str] = Field(default_factory=list)  # paths it configures
    variables: dict[str, Any] = Field(default_factory=dict)
    source_file: str


# --- Edge Types ---


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class GraphEdge(BaseModel):
    """An edge in the knowledge graph."""

    source: str
    target: str
    edge_type: EdgeType
    weight: Optional[float] = None
    metadata: Optional[dict[str, Any]] = None


# --- Graph container types (for serialization) ---


class ColumnLineageEdge(BaseModel):
    """Column-level lineage: source column -> target column via transformation."""

    source_dataset: str
    source_column: str
    target_dataset: str
    target_column: str
    transformation_id: str
    source_file: Optional[str] = None


class NetworkMetrics(BaseModel):
    """Network analysis metrics for module and lineage graphs."""

    # Module graph
    module_node_count: int = 0
    module_edge_count: int = 0
    module_pagerank_top: list[tuple[str, float]] = Field(default_factory=list)
    module_betweenness_top: list[tuple[str, float]] = Field(default_factory=list)
    module_communities: list[list[str]] = Field(default_factory=list)  # Louvain-style clusters
    module_degree_stats: dict[str, float] = Field(default_factory=dict)  # min, max, mean in/out degree

    # Lineage graph
    lineage_node_count: int = 0
    lineage_edge_count: int = 0
    lineage_betweenness_top: list[tuple[str, float]] = Field(default_factory=list)
    lineage_communities: list[list[str]] = Field(default_factory=list)
    lineage_degree_stats: dict[str, float] = Field(default_factory=dict)


class ModuleGraph(BaseModel):
    """Serializable module import graph."""

    metadata: Optional[AnalysisMetadata] = None
    nodes: list[ModuleNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)
    pagerank: dict[str, float] = Field(default_factory=dict)
    strongly_connected_components: list[list[str]] = Field(default_factory=list)
    high_velocity_files: list[str] = Field(default_factory=list)
    entry_points: list[str] = Field(default_factory=list)
    dead_code_candidates: list[str] = Field(default_factory=list)
    hub_modules: list[str] = Field(default_factory=list)  # top PageRank modules
    network_metrics: Optional[NetworkMetrics] = None


class LineageGraph(BaseModel):
    """Serializable data lineage graph."""

    metadata: Optional[AnalysisMetadata] = None
    datasets: list[DatasetNode] = Field(default_factory=list)
    transformations: list[TransformationNode] = Field(default_factory=list)
    configs: list[ConfigNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)  # in-degree 0 (data sources)
    sinks: list[str] = Field(default_factory=list)    # out-degree 0 (final outputs)
    critical_path: list[str] = Field(default_factory=list)  # longest path in DAG
    column_lineage: list[ColumnLineageEdge] = Field(default_factory=list)
    network_metrics: Optional[NetworkMetrics] = None
