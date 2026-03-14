"""Semanticist agent: infer purpose statements and domain clusters.
- LLM-powered analysis (OpenRouter/OpenAI) with cost-aware model tiering and ContextWindowBudget (token budget).
- Embedding-based domain clustering (optional): refine domains by similarity to domain prototypes.
- Documentation-drift detection: compare code-grounded purpose to module docstrings.
- Day-One Q&A synthesizer: explicit Q&A with file/line citations from the graphs.
"""

import os
import re
from pathlib import Path
from typing import Any, Optional

import httpx

from src.embeddings import cosine_similarity, get_embedding
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

# Prototype descriptions for embedding-based domain clustering (one short string per domain)
DOMAIN_PROTOTYPES = [
    "ingestion: raw data, staging, ETL input, source tables",
    "analytics: data mart, fact and dimension tables, reporting, aggregations",
    "pipeline: workflow, DAG, orchestration, task runner",
    "configuration: config files, settings, environment, YAML config",
    "data: database, schema, SQL, migrations",
    "testing: tests, fixtures, mocks, specs",
    "api: REST API, client, HTTP requests",
    "auth: authentication, login, session, user",
    "shared: utilities, helpers, common code",
    "entrypoint: CLI, main, application entry",
    "uncategorized: other or unknown",
]


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
OPENROUTER_EXPENSIVE_MODEL = "openai/gpt-4o"

# Model tiering: cheap = fast/cheap, expensive = higher quality, auto = cheap until budget low then heuristics
def _llm_tier() -> str:
    return os.environ.get("CARTOGRAPHER_LLM_TIER", "auto").strip().lower() or "auto"


def _resolve_llm_config() -> tuple[str, Optional[str], str]:
    """Resolve API key, base URL, and model from env. Model tiering: cheap vs expensive driven by CARTOGRAPHER_LLM_TIER."""
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    key = openrouter_key or openai_key
    base = OPENROUTER_BASE_URL if openrouter_key else (None if openai_key else OPENROUTER_BASE_URL)
    model = OPENROUTER_DEFAULT_MODEL if openrouter_key else "gpt-4o-mini"
    tier = _llm_tier()
    if tier == "expensive":
        model = OPENROUTER_EXPENSIVE_MODEL if openrouter_key else "gpt-4o"
    elif tier == "cheap":
        model = OPENROUTER_DEFAULT_MODEL if openrouter_key else "gpt-4o-mini"
    return key or "", base, model


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

    def _use_llm_for_node(self) -> bool:
        """Model tiering: auto = use LLM until budget drops below 20%, then heuristics."""
        if not self.use_llm or not self.api_key:
            return False
        tier = _llm_tier()
        if tier == "cheap" or tier == "expensive":
            return self._tokens_used < self.token_budget
        if tier == "auto":
            return self._tokens_used < self.token_budget and (self._tokens_used < 0.8 * self.token_budget)
        return self._tokens_used < self.token_budget

    def enrich_module_node(self, path: str, node: ModuleNode) -> ModuleNode:
        """Add purpose_statement and domain_cluster (LLM if enabled and budget allows, else heuristics). Doc-drift vs docstring."""
        purpose = node.purpose_statement
        domain = node.domain_cluster
        if self._use_llm_for_node():
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

    def _embedding_domain_refinement(self, graph: KnowledgeGraph) -> None:
        """Optional: refine domain_cluster using embedding similarity to domain prototypes. Set CARTOGRAPHER_EMBEDDING_CLUSTER=1."""
        if not os.environ.get("CARTOGRAPHER_EMBEDDING_CLUSTER", "").strip() in ("1", "true", "yes") or not self.api_key:
            return
        domain_names = [p.split(":")[0].strip() for p in DOMAIN_PROTOTYPES]
        prototype_embeddings: list[Optional[list[float]]] = []
        for p in DOMAIN_PROTOTYPES:
            emb = get_embedding(p, self.api_key, self.llm_base_url)
            prototype_embeddings.append(emb)
        if any(e is None for e in prototype_embeddings):
            return
        for path, node in list(graph.module_nodes.items()):
            text = f"{path} {node.purpose_statement or ''}"
            emb = get_embedding(text, self.api_key, self.llm_base_url)
            if emb is None:
                continue
            best_i = 0
            best_sim = -1.0
            for i, pe in enumerate(prototype_embeddings):
                if pe is None:
                    continue
                sim = cosine_similarity(emb, pe)
                if sim > best_sim:
                    best_sim = sim
                    best_i = i
            if best_i < len(domain_names):
                node.domain_cluster = domain_names[best_i]
                graph.module_nodes[path] = node
        return

    def run(self, graph: KnowledgeGraph) -> KnowledgeGraph:
        """Enrich all module nodes in the graph with purpose and domain; optional embedding-based domain refinement."""
        for path, node in list(graph.module_nodes.items()):
            graph.module_nodes[path] = self.enrich_module_node(path, node)
        self._embedding_domain_refinement(graph)
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


def day_one_qa_synthesis(module_graph: dict, lineage_graph: dict) -> list[dict[str, Any]]:
    """Explicit Day-One Q&A synthesizer: answers the five Day-One questions with file/line citations from the graphs."""
    citations_for = []
    sources = [s for s in lineage_graph.get("sources", []) if not s.startswith("sql:")]
    sinks = [s for s in lineage_graph.get("sinks", []) if not s.startswith("sql:")]
    trans = lineage_graph.get("transformations", [])
    critical = lineage_graph.get("critical_path", [])
    nodes = module_graph.get("nodes", [])

    def cite(file: str, line_range: Optional[tuple[int, int]] = None) -> dict:
        d: dict = {"file": file, "source": "static"}
        if line_range:
            d["line_range"] = list(line_range)
        return d

    # Q1: Primary data ingestion path
    lines1 = ["Data enters via: " + ", ".join(sources[:10]) + "."]
    for t in trans[:5]:
        if t.get("source_file"):
            citations_for.append(cite(t["source_file"], tuple(t["line_range"]) if isinstance(t.get("line_range"), list) and len(t.get("line_range", [])) >= 2 else None))
    q1 = {"question": "What is the primary data ingestion path?", "answer": "\n".join(lines1), "citations": citations_for[:10]}

    # Q2: Critical output datasets
    lines2 = ["Critical outputs: " + ", ".join(sinks[:10]) + "."]
    q2 = {"question": "What are the 3-5 most critical output datasets?", "answer": "\n".join(lines2), "citations": [cite("lineage_graph.json")]}

    # Q3: Blast radius (critical path)
    path_show = [n for n in critical if not n.startswith("sql:")][:8]
    lines3 = ["Critical path: " + " -> ".join(path_show) + ". Failure of any node blocks downstream."]
    c3 = []
    for tid in critical:
        t = next((x for x in trans if x.get("id") == tid), None)
        if t and t.get("source_file"):
            c3.append(cite(t["source_file"], tuple(t["line_range"]) if isinstance(t.get("line_range"), list) and len(t.get("line_range", [])) >= 2 else None))
    q3 = {"question": "What is the blast radius if the most critical module fails?", "answer": "\n".join(lines3), "citations": c3[:15]}

    # Q4: Business logic concentration (hub modules + high-LOC)
    hub = module_graph.get("hub_modules", [])[:5]
    by_loc = sorted(nodes, key=lambda n: -(n.get("lines_of_code") or 0))[:5]
    lines4 = ["Hub modules (high impact): " + ", ".join(hub) + ". High-LOC: " + ", ".join(n.get("path", "") for n in by_loc) + "."]
    c4 = [cite(n.get("path", "")) for n in by_loc if n.get("path")]
    q4 = {"question": "Where is business logic concentrated?", "answer": "\n".join(lines4), "citations": c4[:10]}

    # Q5: High-velocity files
    vel = module_graph.get("high_velocity_files", [])[:5]
    lines5 = ["High-velocity (recent changes): " + ", ".join(vel) + "."] if vel else ["No git velocity data (run with repo clone)."]
    q5 = {"question": "What has changed most frequently?", "answer": "\n".join(lines5), "citations": [cite("module_graph.json")]}

    return [q1, q2, q3, q4, q5]
