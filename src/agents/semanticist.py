"""Semanticist agent: infer purpose statements and domain clusters.
- LLM-powered analysis (OpenRouter/OpenAI) with cost-aware model tiering and ContextWindowBudget (token budget).
- Documentation-drift detection: compare code-grounded purpose to module docstrings.
- Day-One question answering: purpose/domain feed Navigator and onboarding_brief.
"""

import os
import re
from pathlib import Path
from typing import Optional

import httpx

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode


def _extract_module_docstring(repo_root: Optional[Path], path: str) -> Optional[str]:
    """Extract first module-level docstring (Python) or None."""
    if not repo_root or not path or not path.endswith(".py"):
        return None
    try:
        full = repo_root / path.replace("\\", "/")
        if not full.exists():
            return None
        text = full.read_text(encoding="utf-8", errors="replace").strip()
        # First ''' or """ block at start of file
        for sep in ('"""', "'''"):
            if text.startswith(sep):
                end = text.find(sep, len(sep))
                if end != -1:
                    return text[len(sep):end].strip()[:300]
        return None
    except Exception:
        return None


def _detect_drift(purpose: Optional[str], docstring: Optional[str]) -> bool:
    """True if purpose and docstring both exist but have no significant overlap (documentation drift)."""
    if not purpose or not docstring:
        return False
    p = purpose.lower()
    d = docstring.lower()
    words_p = set(re.findall(r"\w+", p))
    words_d = set(re.findall(r"\w+", d))
    overlap = len(words_p & words_d) / max(len(words_p), 1)
    return overlap < 0.2

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
) -> tuple[Optional[str], Optional[str], int]:
    """Call LLM to infer purpose and domain. Returns (purpose, domain, tokens_used) or (None, None, 0) on failure."""
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
    total_tokens = 0
    try:
        with httpx.Client(timeout=15.0) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
            content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
            usage = data.get("usage", {})
            total_tokens = (usage.get("input_tokens") or usage.get("prompt_tokens") or 0) + (
                usage.get("output_tokens") or usage.get("completion_tokens") or 0
            )
    except Exception:
        return None, None, 0

    purpose, domain = None, None
    for line in content.strip().split("\n"):
        line = line.strip()
        if line.lower().startswith("purpose:"):
            purpose = line.split(":", 1)[-1].strip()
        elif line.lower().startswith("domain:"):
            domain = line.split(":", 1)[-1].strip().lower()
    if domain and domain not in {s.strip() for s in DOMAIN_OPTIONS.split(",")}:
        domain = "uncategorized"
    if total_tokens == 0 and (purpose or domain):
        total_tokens = (len(prompt) + len(content)) // 4
    return purpose or None, domain or None, total_tokens


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


def _parse_token_budget() -> int:
    """Token budget from env (CARTOGRAPHER_TOKEN_BUDGET). Default 100_000."""
    try:
        return max(0, int(os.environ.get("CARTOGRAPHER_TOKEN_BUDGET", "100000")))
    except Exception:
        return 100_000


class Semanticist:
    """Infers semantic meaning: LLM (cost-aware tiering + token budget) or heuristic; doc-drift detection vs docstrings."""

    def __init__(
        self,
        repo_root: Optional[Path] = None,
        use_llm: bool = True,
        api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        llm_model: Optional[str] = None,
        token_budget: Optional[int] = None,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root else None
        env_key, env_base, env_model = _resolve_llm_config()
        self.api_key = api_key or env_key
        self.llm_base_url = llm_base_url if llm_base_url is not None else env_base
        self.llm_model = llm_model or env_model
        self.use_llm = use_llm and bool(self.api_key)
        self._llm_calls = 0
        self._tokens_used = 0
        self.token_budget = token_budget if token_budget is not None else _parse_token_budget()
        self.ContextWindowBudget = self.token_budget  # alias for spec
        self._doc_drift_paths: list[str] = []

    def enrich_module_node(self, path: str, node: ModuleNode) -> ModuleNode:
        """Add purpose_statement and domain_cluster (LLM if enabled and budget allows, else heuristics). Doc-drift vs docstring."""
        purpose = node.purpose_statement
        domain = node.domain_cluster
        if self.use_llm and self.api_key and self._tokens_used < self.token_budget:
            llm_purpose, llm_domain, tokens = _llm_infer_purpose_and_domain(
                path, node, self.api_key, self.llm_base_url, self.llm_model
            )
            self._tokens_used += tokens
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
        docstring = _extract_module_docstring(self.repo_root, path)
        if docstring:
            node.docstring_snippet = docstring[:200]
            if _detect_drift(purpose, docstring):
                node.doc_drift_detected = True
                self._doc_drift_paths.append(path)
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
        """Return stats: LLM usage, token count, doc-drift paths."""
        return {
            "llm_enabled": self.use_llm,
            "llm_calls": self._llm_calls,
            "tokens_used": self._tokens_used,
            "token_budget": self.token_budget,
            "doc_drift_detected": self._doc_drift_paths,
        }

    def get_doc_drift_report(self, graph: KnowledgeGraph) -> list[dict]:
        """Day-One audit: list modules where code-grounded purpose diverges from docstring."""
        return [
            {"path": p, "purpose": graph.module_nodes.get(p).purpose_statement if p in graph.module_nodes else None}
            for p in self._doc_drift_paths
        ]
