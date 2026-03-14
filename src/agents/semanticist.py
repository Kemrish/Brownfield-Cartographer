"""Semanticist agent: infer purpose statements and domain clusters.
Supports LLM-powered analysis via OpenRouter (OPENROUTER_API_KEY in .env) or OpenAI (OPENAI_API_KEY); otherwise heuristics.
"""

import os
import re
from pathlib import Path
from typing import Optional

import httpx

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode

# Domain set for LLM (consistent vocabulary)
DOMAIN_OPTIONS = "ingestion, analytics, pipeline, configuration, data, testing, api, auth, shared, entrypoint, uncategorized"


# Domain keywords: path/filename/identifier hints -> domain label
DOMAIN_PATTERNS = [
    (r"\b(staging|stg_|raw_|ingest)\b", "ingestion"),
    (r"\b(mart|marts|dwh|warehouse|fact|dim)\b", "analytics"),
    (r"\b(api|rest|client|request)\b", "api"),
    (r"\b(test|spec|fixture|mock)\b", "testing"),
    (r"\b(config|settings|env)\b", "configuration"),
    (r"\b(model|train|predict|ml)\b", "ml"),
    (r"\b(etl|pipeline|dag|workflow)\b", "pipeline"),
    (r"\b(auth|login|user|session)\b", "auth"),
    (r"\b(db|schema|migration|sql)\b", "data"),
    (r"\b(util|helper|common|shared)\b", "shared"),
    (r"\b(cli|main|entry)\b", "entrypoint"),
]

# Purpose templates by language and path hints
PURPOSE_HINTS = [
    (r"dbt_project\.yml", "dbt project configuration"),
    (r"schema\.yml|sources\.yml|__sources\.yml", "dbt schema/source definitions"),
    (r"models/staging/", "dbt staging model"),
    (r"models/marts/", "dbt mart model"),
    (r"models/intermediate/", "dbt intermediate model"),
    (r"macros/", "dbt macro"),
    (r"seeds/", "dbt seed data"),
    (r"snapshots/", "dbt snapshot"),
    (r"tests/", "dbt test"),
    (r"cli\.py|__main__\.py", "CLI entry point"),
    (r"orchestrator|runner|pipeline\.py", "pipeline orchestration"),
    (r"config\.py|settings\.py", "application configuration"),
    (r"conftest\.py", "pytest configuration"),
    (r"__init__\.py", "package initializer"),
]


def _infer_domain(path: str, node: ModuleNode) -> Optional[str]:
    """Infer domain cluster from path and module content."""
    path_lower = path.lower()
    combined = f"{path_lower} {' '.join(node.public_functions or [])} {' '.join(node.classes or [])}"

    for pattern, domain in DOMAIN_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return domain
    return None


def _infer_purpose(path: str, node: ModuleNode) -> Optional[str]:
    """Infer purpose statement from path and structure."""
    path_posix = path.replace("\\", "/")

    for pattern, purpose in PURPOSE_HINTS:
        if re.search(pattern, path_posix, re.IGNORECASE):
            return purpose

    # Language-specific heuristics
    if node.language == "sql":
        if node.sql_statement_type:
            return f"SQL {node.sql_statement_type} transformation"
        return "SQL transformation"
    if node.language == "yaml":
        if node.yaml_root_keys:
            keys = ", ".join(node.yaml_root_keys[:3])
            return f"YAML config: {keys}"
        return "YAML configuration"
    if node.language == "python":
        if node.classes:
            return f"Python module defining: {', '.join(node.classes[:2])}"
        if node.public_functions:
            return f"Python module: {', '.join(node.public_functions[:3])}"
        if node.imports:
            return "Python module (imports only)"
    return None


def _llm_infer_purpose_and_domain(
    path: str,
    node: ModuleNode,
    api_key: str,
    base_url: Optional[str] = None,
    model: str = "gpt-4o-mini",
) -> tuple[Optional[str], Optional[str]]:
    """Call LLM to infer purpose statement and domain cluster. Returns (purpose, domain) or (None, None) on failure."""
    context = f"path={path}, language={node.language}"
    if node.public_functions:
        context += f", functions={node.public_functions[:5]}"
    if node.classes:
        context += f", classes={node.classes[:3]}"
    if node.sql_statement_type:
        context += f", sql_type={node.sql_statement_type}"
    if node.yaml_root_keys:
        context += f", yaml_keys={node.yaml_root_keys[:5]}"

    prompt = f"""For this codebase module, return exactly two short lines:
1. purpose: one concise sentence describing what this file does (e.g. "dbt staging model for customer data").
2. domain: exactly one word from this list: {DOMAIN_OPTIONS}

Context: {context}

Format your response as:
purpose: <sentence>
domain: <one word>"""

    url = (base_url or "https://api.openai.com/v1").rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 150,
        "temperature": 0.2,
    }
    try:
        with httpx.Client(timeout=15.0) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
            content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    except Exception:
        return None, None

    purpose, domain = None, None
    for line in content.strip().split("\n"):
        line = line.strip()
        if line.lower().startswith("purpose:"):
            purpose = line.split(":", 1)[-1].strip()
        elif line.lower().startswith("domain:"):
            domain = line.split(":", 1)[-1].strip().lower()
    if domain and domain not in {s.strip() for s in DOMAIN_OPTIONS.split(",")}:
        domain = "uncategorized"
    return purpose or None, domain or None


# OpenRouter: same API shape as OpenAI, different base URL and model ID
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_DEFAULT_MODEL = "openai/gpt-4o-mini"


def _resolve_llm_config() -> tuple[str, Optional[str], str]:
    """Resolve API key, base URL, and model from env. Prefer OpenRouter if OPENROUTER_API_KEY is set."""
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if openrouter_key:
        return openrouter_key, OPENROUTER_BASE_URL, OPENROUTER_DEFAULT_MODEL
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if openai_key:
        return openai_key, None, "gpt-4o-mini"
    return "", None, "gpt-4o-mini"


class Semanticist:
    """Infers semantic meaning: LLM via OpenRouter (OPENROUTER_API_KEY) or OpenAI (OPENAI_API_KEY), else heuristic."""

    def __init__(
        self,
        repo_root: Optional[Path] = None,
        use_llm: bool = True,
        api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        llm_model: Optional[str] = None,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root else None
        env_key, env_base, env_model = _resolve_llm_config()
        self.api_key = api_key or env_key
        self.llm_base_url = llm_base_url if llm_base_url is not None else env_base
        self.llm_model = llm_model or env_model
        self.use_llm = use_llm and bool(self.api_key)
        self._llm_calls = 0

    def enrich_module_node(self, path: str, node: ModuleNode) -> ModuleNode:
        """Add purpose_statement and domain_cluster (LLM if enabled, else heuristics)."""
        purpose = node.purpose_statement
        domain = node.domain_cluster
        if self.use_llm and self.api_key:
            llm_purpose, llm_domain = _llm_infer_purpose_and_domain(
                path, node, self.api_key, self.llm_base_url, self.llm_model
            )
            if llm_purpose is not None:
                purpose = llm_purpose
                self._llm_calls += 1
            if llm_domain is not None:
                domain = llm_domain
        if purpose is None:
            purpose = _infer_purpose(path, node)
        if domain is None:
            domain = _infer_domain(path, node)
        node.purpose_statement = purpose
        node.domain_cluster = domain
        return node

    def run(self, graph: KnowledgeGraph) -> KnowledgeGraph:
        """Enrich all module nodes in the graph with purpose and domain."""
        for path, node in list(graph.module_nodes.items()):
            graph.module_nodes[path] = self.enrich_module_node(path, node)
        return graph

    def get_domain_summary(self, graph: KnowledgeGraph) -> dict[str, list[str]]:
        """Return domain -> list of module paths."""
        summary: dict[str, list[str]] = {}
        for path, node in graph.module_nodes.items():
            domain = node.domain_cluster or "uncategorized"
            summary.setdefault(domain, []).append(path)
        return summary

    def get_stats(self) -> dict:
        """Return stats including whether LLM was used and how many calls."""
        return {"llm_enabled": self.use_llm, "llm_calls": self._llm_calls}
