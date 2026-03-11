"""Parse Airflow DAG definitions and dbt schema.yml for pipeline topology."""

from pathlib import Path
from typing import Any

import yaml

from src.models.schemas import DatasetNode, TransformationNode, ConfigNode, GraphEdge, EdgeType


class DAGConfigParser:
    """Extract pipeline topology from Airflow DAG files and dbt YAML config."""

    def __init__(self) -> None:
        pass

    def parse_yaml_file(self, path: str | Path, source: str) -> Any:
        """Safe YAML parse."""
        try:
            return yaml.safe_load(source)
        except Exception:
            return None

    def parse_dbt_schema_yml(self, path: str | Path, source: str) -> tuple[list[DatasetNode], list[GraphEdge], list[ConfigNode]]:
        """
        Parse dbt schema.yml (models, sources). Returns dataset nodes, edges, and config nodes.
        """
        path_str = str(Path(path).as_posix())
        data = self.parse_yaml_file(path, source)
        if not data or not isinstance(data, dict):
            return [], [], []

        nodes: list[DatasetNode] = []
        edges: list[GraphEdge] = []
        configs: list[ConfigNode] = []

        # dbt sources - extract source name AND tables within
        for source_block in data.get("sources", []):
            if not isinstance(source_block, dict):
                continue
            source_name = source_block.get("name")
            if not source_name:
                continue

            # Add the source itself
            nodes.append(
                DatasetNode(
                    name=source_name,
                    storage_type="table",
                    source_file=path_str,
                    is_source_of_truth=True,
                    is_external_source=True,
                    dbt_source_name=source_name,
                )
            )

            # Add individual tables within the source
            for table in source_block.get("tables", []):
                if isinstance(table, dict) and table.get("name"):
                    tbl_name = table["name"]
                    full_name = f"{source_name}.{tbl_name}"
                    columns = []
                    for col in table.get("columns", []):
                        if isinstance(col, dict) and col.get("name"):
                            columns.append(col["name"])
                    nodes.append(
                        DatasetNode(
                            name=full_name,
                            storage_type="table",
                            source_file=path_str,
                            is_source_of_truth=True,
                            is_external_source=True,
                            dbt_source_name=source_name,
                            columns=columns,
                        )
                    )

        # dbt models - list of { name: "model_name", columns: [...], ... }
        for model_block in data.get("models", []):
            if isinstance(model_block, dict) and model_block.get("name"):
                model_name = model_block["name"]
                columns = []
                for col in model_block.get("columns", []):
                    if isinstance(col, dict) and col.get("name"):
                        columns.append(col["name"])
                nodes.append(
                    DatasetNode(
                        name=model_name,
                        storage_type="table",
                        source_file=path_str,
                        columns=columns,
                        dbt_ref_name=model_name,
                    )
                )
            elif isinstance(model_block, list):
                for m in model_block:
                    if isinstance(m, dict) and m.get("name"):
                        nodes.append(
                            DatasetNode(name=m["name"], storage_type="table", source_file=path_str, dbt_ref_name=m["name"])
                        )

        # Create a config node for this schema file
        configures = [n.name for n in nodes]
        if configures:
            configs.append(
                ConfigNode(
                    path=path_str,
                    config_type="schema_yml" if "schema" in path_str.lower() else "sources_yml",
                    configures=configures,
                    source_file=path_str,
                )
            )

        return nodes, edges, configs

    def parse_dbt_project_yml(self, path: str | Path, source: str) -> ConfigNode | None:
        """Parse dbt_project.yml to extract project config."""
        path_str = str(Path(path).as_posix())
        data = self.parse_yaml_file(path, source)
        if not data or not isinstance(data, dict):
            return None

        variables = {}
        if "vars" in data:
            variables = data["vars"] if isinstance(data["vars"], dict) else {}

        return ConfigNode(
            path=path_str,
            config_type="dbt_project",
            configures=[],
            variables=variables,
            source_file=path_str,
        )

    def parse_airflow_dag_python(self, path: str | Path, source: str) -> tuple[list[TransformationNode], list[GraphEdge]]:
        """
        Heuristic parse of Airflow DAG Python file: look for task definitions and dependencies.
        Does not execute the file; uses simple string/AST patterns.
        """
        import re
        path_str = str(Path(path).as_posix())
        transformations: list[TransformationNode] = []
        edges: list[GraphEdge] = []

        # Extract task_id from operator definitions
        task_id_pattern = re.compile(r'task_id\s*=\s*["\']([^"\']+)["\']')
        task_ids = task_id_pattern.findall(source)

        # Extract dependencies from >> and << operators
        dep_pattern = re.compile(r'(\w+)\s*>>\s*(\w+)')
        deps = dep_pattern.findall(source)

        task_set = set(task_ids)
        for tid in task_ids[:50]:  # cap
            trans_id = f"airflow:{path_str}:{tid}"
            transformations.append(
                TransformationNode(
                    id=trans_id,
                    source_datasets=[],
                    target_datasets=[],
                    transformation_type="airflow",
                    source_file=path_str,
                )
            )

        # Create edges for task dependencies
        for upstream, downstream in deps:
            if upstream in task_set or downstream in task_set:
                edges.append(
                    GraphEdge(
                        source=f"airflow:{path_str}:{upstream}",
                        target=f"airflow:{path_str}:{downstream}",
                        edge_type=EdgeType.CALLS,
                        metadata={"source_file": path_str},
                    )
                )

        return transformations, edges

    def analyze_dbt_project(self, project_root: str | Path) -> tuple[list[DatasetNode], list[GraphEdge], list[ConfigNode]]:
        """Scan dbt project for schema.yml, sources.yml, and dbt_project.yml."""
        root = Path(project_root)
        nodes: list[DatasetNode] = []
        edges: list[GraphEdge] = []
        configs: list[ConfigNode] = []

        for yml in root.rglob("*.yml"):
            try:
                rel = yml.relative_to(root)
                if ".git" in rel.parts or ".venv" in rel.parts:
                    continue
                text = yml.read_text(encoding="utf-8", errors="ignore")

                if yml.name == "dbt_project.yml":
                    cfg = self.parse_dbt_project_yml(yml, text)
                    if cfg:
                        configs.append(cfg)
                elif "schema" in yml.name.lower() or "source" in yml.name.lower() or yml.name.startswith("_"):
                    n, e, c = self.parse_dbt_schema_yml(yml, text)
                    nodes.extend(n)
                    edges.extend(e)
                    configs.extend(c)
            except Exception:
                pass
        return nodes, edges, configs
