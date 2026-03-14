"""Navigator agent: LangGraph-style agent with 4 tools (find_implementation, trace_lineage, blast_radius, explain_module)
against the knowledge graph; embedding-based semantic search in find_implementation; line ranges and explicit static-vs-LLM
tagging in all responses; multi-step tool chaining for compound queries."""

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from src.embeddings import cosine_similarity, get_embedding
from src.agents.semanticist import day_one_qa_synthesis


def _load_cartography(cartography_dir: Path) -> tuple[Optional[dict], Optional[dict]]:
    module_path = cartography_dir / "module_graph.json"
    lineage_path = cartography_dir / "lineage_graph.json"
    module_graph = None
    lineage_graph = None
    if module_path.exists():
        try:
            with open(module_path, "r", encoding="utf-8") as f:
                module_graph = json.load(f)
        except Exception:
            pass
    if lineage_path.exists():
        try:
            with open(lineage_path, "r", encoding="utf-8") as f:
                lineage_graph = json.load(f)
        except Exception:
            pass
    return module_graph, lineage_graph


@dataclass
class Citation:
    """A file/line citation for a fact (static from artifacts or LLM)."""
    file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
    source: str = "static"  # "static" | "llm"
    artifact: Optional[str] = None  # e.g. "lineage_graph.json", "module_graph.json"


@dataclass
class ToolResult:
    """Structured result from a Navigator tool: answer text + citations + explicit static-vs-LLM attribution."""
    answer: str
    citations: list[Citation] = field(default_factory=list)
    attribution: str = "Source: static (lineage_graph.json, module_graph.json). No LLM used for this response."
    used_llm: bool = False  # True when embedding search or other LLM was used


class Navigator:
    """Query agent: four named tools against the knowledge graph, agent loop for NL, citations and attribution."""

    def __init__(self, cartography_dir: str | Path) -> None:
        self.cartography_dir = Path(cartography_dir)
        self._module_graph: Optional[dict] = None
        self._lineage_graph: Optional[dict] = None
        self._load()

    def _load(self) -> None:
        self._module_graph, self._lineage_graph = _load_cartography(self.cartography_dir)

    # ---------- Four named tools (against the knowledge graph) ----------

    def tool_ingestion_and_outputs(self) -> ToolResult:
        """Tool 1: Data sources (entry points) and data sinks (outputs). Day-One Q1/Q2."""
        if not self._lineage_graph:
            return ToolResult("No lineage graph loaded.", citations=[], attribution="Source: static. No LLM used for this response.")
        sources = [s for s in self._lineage_graph.get("sources", []) if not s.startswith("sql:")]
        sinks = [s for s in self._lineage_graph.get("sinks", []) if not s.startswith("sql:")]
        citations: list[Citation] = [
            Citation(file=None, line_range=None, source="static", artifact="lineage_graph.json"),
        ]
        for t in self._lineage_graph.get("transformations", [])[:5]:
            sf = t.get("source_file")
            if sf:
                lr = t.get("line_range")
                citations.append(Citation(file=sf, line_range=tuple(lr) if isinstance(lr, list) and len(lr) >= 2 else None, source="static", artifact="lineage_graph.json"))
        lines = ["**Data sources (ingestion):**"]
        for s in sources[:20]:
            lines.append(f"  - {s}")
        lines.append("")
        lines.append("**Data sinks (outputs):**")
        for s in sinks[:20]:
            lines.append(f"  - {s}")
        return ToolResult(
            answer="\n".join(lines),
            citations=citations[:10],
            attribution="Source: static (lineage_graph.json). No LLM used for this response.",
        )

    def tool_critical_path(self) -> ToolResult:
        """Tool 2: Longest dependency chain (critical path). Day-One Q3 blast-radius context."""
        if not self._lineage_graph:
            return ToolResult("No lineage graph loaded.", citations=[], attribution="Source: static. No LLM used for this response.")
        path = self._lineage_graph.get("critical_path", [])
        path_show = [n for n in path if not n.startswith("sql:")][:12]
        citations: list[Citation] = [Citation(artifact="lineage_graph.json", source="static")]
        trans_by_id = {t.get("id"): t for t in self._lineage_graph.get("transformations", [])}
        for n in path:
            t = trans_by_id.get(n)
            if t and t.get("source_file"):
                lr = t.get("line_range")
                citations.append(Citation(file=t["source_file"], line_range=tuple(lr) if isinstance(lr, list) and len(lr) >= 2 else None, source="static", artifact="lineage_graph.json"))
        answer = "Critical path (longest dependency chain):\n  " + " -> ".join(path_show)
        if len(path) > 12:
            answer += f"\n  ... and {len(path) - 12} more nodes"
        return ToolResult(answer=answer, citations=citations[:15], attribution="Source: static (lineage_graph.json). No LLM used for this response.")

    def tool_blast_radius(self, node: str, direction: str = "downstream") -> ToolResult:
        """Tool 3: Upstream/downstream impact of a node (blast radius)."""
        if not self._lineage_graph:
            return ToolResult("No lineage graph loaded.", citations=[], attribution="Source: static. No LLM used for this response.")
        edges = self._lineage_graph.get("edges", [])
        out_edges: dict[str, list[str]] = {}
        in_edges: dict[str, list[str]] = {}
        for e in edges:
            s, t = e.get("source"), e.get("target")
            if s and t:
                out_edges.setdefault(s, []).append(t)
                in_edges.setdefault(t, []).append(s)
        if direction == "downstream":
            visited = set()
            stack = [node]
            while stack:
                n = stack.pop()
                if n in visited:
                    continue
                visited.add(n)
                for out in out_edges.get(n, []):
                    stack.append(out)
            visited.discard(node)
            affected = list(visited)
        else:
            visited = set()
            stack = [node]
            while stack:
                n = stack.pop()
                if n in visited:
                    continue
                visited.add(n)
                for inc in in_edges.get(n, []):
                    stack.append(inc)
            visited.discard(node)
            affected = list(visited)
        label = "Downstream" if direction == "downstream" else "Upstream"
        answer = f"{label} blast radius of '{node}': {len(affected)} nodes\n" + "\n".join(f"  - {x}" for x in affected[:30])
        if len(affected) > 30:
            answer += f"\n  ... and {len(affected) - 30} more"
        trans_by_id = {t.get("id"): t for t in self._lineage_graph.get("transformations", [])}
        citations = [Citation(artifact="lineage_graph.json", source="static")]
        if node in trans_by_id:
            t = trans_by_id[node]
            if t.get("source_file"):
                citations.append(Citation(file=t["source_file"], line_range=tuple(t["line_range"]) if isinstance(t.get("line_range"), list) and len(t.get("line_range", [])) >= 2 else None, source="static", artifact="lineage_graph.json"))
        return ToolResult(answer=answer, citations=citations, attribution="Source: static (lineage_graph.json). No LLM used for this response.")

    def tool_column_lineage_and_hub_modules(self, dataset: Optional[str] = None) -> ToolResult:
        """Tool 4: Column lineage for a dataset, or hub modules (high PageRank)."""
        citations: list[Citation] = [Citation(artifact="lineage_graph.json", source="static"), Citation(artifact="module_graph.json", source="static")]
        if dataset and self._lineage_graph:
            edges = self._lineage_graph.get("column_lineage", [])
            relevant = [e for e in edges if e.get("source_dataset") == dataset or e.get("target_dataset") == dataset]
            if relevant:
                lines = [f"Column lineage for '{dataset}':"]
                for e in relevant[:25]:
                    lines.append(f"  {e.get('source_dataset')}.{e.get('source_column')} -> {e.get('target_dataset')}.{e.get('target_column')}")
                return ToolResult("\n".join(lines), citations=citations, attribution="Source: static (lineage_graph.json). No LLM used for this response.")
        if self._module_graph:
            hubs = self._module_graph.get("hub_modules", [])[:15]
            if hubs:
                return ToolResult("Hub modules (high PageRank):\n" + "\n".join(f"  - {h}" for h in hubs), citations=citations, attribution="Source: static (module_graph.json). No LLM used for this response.")
        return ToolResult("Specify a dataset for column lineage (e.g. 'column lineage for customers') or ask for 'hub modules'.", citations=citations, attribution="Source: static. No LLM used for this response.")

    # ---------- Four named tools (spec: find_implementation, trace_lineage, blast_radius, explain_module) ----------

    def _embedding_search(self, query: str, top_k: int = 5) -> tuple[list[tuple[str, float]], bool]:
        """Semantic search over modules (path+purpose). Returns [(path, score), ...] and used_llm."""
        api_key = os.environ.get("OPENROUTER_API_KEY", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key or not self._module_graph:
            return [], False
        q_emb = get_embedding(query)
        if q_emb is None:
            return [], False
        nodes = self._module_graph.get("nodes", [])
        scored = []
        for n in nodes:
            path = n.get("path", "")
            purpose = n.get("purpose_statement") or ""
            text = f"{path} {purpose}"
            m_emb = get_embedding(text)
            if m_emb is None:
                continue
            sim = cosine_similarity(q_emb, m_emb)
            scored.append((path, sim))
        scored.sort(key=lambda x: -x[1])
        return scored[:top_k], True

    def find_implementation(self, module_or_dataset: str) -> ToolResult:
        """Find where a module or dataset is implemented: embedding-based semantic search when API key set, else hub/explain."""
        exact = self._module_graph and module_or_dataset in [n.get("path") for n in self._module_graph.get("nodes", [])]
        if exact:
            return self.explain_module(module_or_dataset)
        # Embedding-based semantic search when available
        scored, used_llm = self._embedding_search(module_or_dataset, top_k=5)
        if scored and used_llm:
            lines = [f"Top matches for '{module_or_dataset}' (embedding similarity):"]
            citations = []
            for path, score in scored:
                r = self.explain_module(path)
                lines.append(f"  - {path} (score={score:.2f}): {r.answer.split(chr(10))[0]}")
                citations.extend(r.citations)
            att = "Source: LLM (embedding-based semantic search). Citations from module_graph.json (static)."
            return ToolResult(answer="\n".join(lines), citations=citations[:10], attribution=att, used_llm=True)
        return self.tool_column_lineage_and_hub_modules(dataset=module_or_dataset)

    def trace_lineage(self) -> ToolResult:
        """Trace data lineage: sources, sinks, and critical path."""
        ing = self.tool_ingestion_and_outputs()
        crit = self.tool_critical_path()
        combined = ing.answer + "\n\n" + crit.answer
        return ToolResult(answer=combined, citations=ing.citations + crit.citations[:5], attribution="Source: static (lineage_graph.json). No LLM used for this response.")

    def explain_module(self, module_path: str) -> ToolResult:
        """Explain a module: purpose statement and domain from the knowledge graph."""
        if not self._module_graph:
            return ToolResult("No module graph loaded.", citations=[], attribution="Source: static. No LLM used for this response.")
        for n in self._module_graph.get("nodes", []):
            if n.get("path") == module_path:
                purpose = n.get("purpose_statement") or "(none)"
                domain = n.get("domain_cluster") or "uncategorized"
                loc = n.get("lines_of_code") or 0
                lang = n.get("language", "")
                answer = f"**{module_path}** ({lang}, {loc} LOC)\n- Purpose: {purpose}\n- Domain: {domain}"
                return ToolResult(answer, citations=[Citation(artifact="module_graph.json", source="static")], attribution="Source: static (module_graph.json). No LLM used for this response.")
        return ToolResult(f"Module not found: {module_path}", citations=[], attribution="Source: static. No LLM used for this response.")

    # ---------- Convenience accessors (used by tools / query) ----------

    def sources(self) -> list[str]:
        if not self._lineage_graph:
            return []
        return [s for s in self._lineage_graph.get("sources", []) if not s.startswith("sql:")]

    def sinks(self) -> list[str]:
        if not self._lineage_graph:
            return []
        return [s for s in self._lineage_graph.get("sinks", []) if not s.startswith("sql:")]

    def critical_path(self) -> list[str]:
        if not self._lineage_graph:
            return []
        return self._lineage_graph.get("critical_path", [])

    def blast_radius(self, node: str, direction: str = "downstream") -> ToolResult:
        """Blast radius (tool): upstream/downstream impact of a node."""
        return self.tool_blast_radius(node, direction=direction)

    def blast_radius_nodes(self, node: str, direction: str = "downstream") -> list[str]:
        if not self._lineage_graph:
            return []
        edges = self._lineage_graph.get("edges", [])
        out_edges: dict[str, list[str]] = {}
        in_edges: dict[str, list[str]] = {}
        for e in edges:
            s, t = e.get("source"), e.get("target")
            if s and t:
                out_edges.setdefault(s, []).append(t)
                in_edges.setdefault(t, []).append(s)
        if direction == "downstream":
            visited = set()
            stack = [node]
            while stack:
                n = stack.pop()
                if n in visited:
                    continue
                visited.add(n)
                for out in out_edges.get(n, []):
                    stack.append(out)
            visited.discard(node)
            return list(visited)
        else:
            visited = set()
            stack = [node]
            while stack:
                n = stack.pop()
                if n in visited:
                    continue
                visited.add(n)
                for inc in in_edges.get(n, []):
                    stack.append(inc)
            visited.discard(node)
            return list(visited)

    def column_lineage_for_dataset(self, dataset: str) -> list[dict]:
        if not self._lineage_graph:
            return []
        edges = self._lineage_graph.get("column_lineage", [])
        return [e for e in edges if e.get("source_dataset") == dataset or e.get("target_dataset") == dataset]

    def hub_modules(self) -> list[str]:
        if not self._module_graph:
            return []
        return self._module_graph.get("hub_modules", [])

    def entry_points(self) -> list[str]:
        if not self._module_graph:
            return []
        return self._module_graph.get("entry_points", [])

    # ---------- Agent loop: route natural-language query to tools; multi-step chaining; Day-One Q&A synthesis ----------

    def _run_single_query(self, q: str) -> Optional[ToolResult]:
        """Run one intent and return ToolResult or None."""
        if re.search(r"\bcritical\s*path\b|\blongest\s*path\b|\bchain\b", q):
            return self.tool_critical_path()
        if re.search(r"\bblast\s*radius\b|\bimpact\b|\bdownstream\b|\bupstream\b", q):
            match = re.search(r"(?:of|for)\s+([^\s?]+)", q, re.IGNORECASE)
            node = match.group(1).strip() if match else None
            if node:
                direction = "downstream" if "upstream" not in q else "upstream"
                return self.tool_blast_radius(node, direction=direction)
            return ToolResult("Specify a node: e.g. 'blast radius of stg_orders'", attribution="Source: static. No LLM used for this response.")
        if re.search(r"\bcolumn\s*lineage\b|\bcolumns?\b", q):
            match = re.search(r"(?:for|of)\s+([^\s?]+)", q, re.IGNORECASE)
            dataset = match.group(1).strip() if match else None
            r = self.tool_column_lineage_and_hub_modules(dataset=dataset)
            if dataset and "Specify a dataset" in r.answer:
                return ToolResult(f"No column lineage found for '{dataset}'.", citations=r.citations, attribution=r.attribution)
            return r
        if re.search(r"\bhub\b|\bpagerank\b|\bimportant\s*modules\b", q):
            return self.tool_column_lineage_and_hub_modules(dataset=None)
        if re.search(r"\bsources?\b|\bsinks?\b|\bentry\b|\bingest|ingestion|outputs?\b|\bexit\b", q):
            return self.tool_ingestion_and_outputs()
        if re.search(r"\b(where|find|implementation|implemented)\b", q):
            match = re.search(r"(?:where|find|for)\s+(?:is|does)\s+([^\s?]+)|implementation\s+of\s+([^\s?]+)|([a-zA-Z0-9_.]+)", q, re.IGNORECASE)
            target = (match.group(1) or match.group(2) or match.group(3) or q).strip() if match else q.strip()
            if target and len(target) > 1:
                return self.find_implementation(target)
        return None

    def query(self, question: str) -> str:
        """Route natural-language question; support multi-step tool chaining and Day-One Q&A with file/line citations and explicit static-vs-LLM tagging."""
        q = question.strip()
        q_lower = q.lower()
        if not q:
            return "Ask a Day-One question: 'sources', 'sinks', 'critical path', 'blast radius of <node>', 'hub modules', 'column lineage for <dataset>'. All responses include Source: static or Source: LLM."

        # Day-One Q&A synthesizer: explicit Q&A with citations when question matches
        day_one_keywords = ["ingestion path", "primary data", "critical output", "blast radius", "business logic", "changed most", "what is the primary", "what are the.*critical", "where is business"]
        if any(re.search(k, q_lower) for k in day_one_keywords) and self._module_graph and self._lineage_graph:
            try:
                qas = day_one_qa_synthesis(self._module_graph, self._lineage_graph)
                for qa in qas:
                    if qa["question"].lower() in q_lower or any(w in q_lower for w in qa["question"].lower().split()[:3]):
                        cite_lines = []
                        for c in qa.get("citations", [])[:10]:
                            f = c.get("file", "")
                            lr = c.get("line_range")
                            if lr:
                                cite_lines.append(f"  - {f} lines {lr[0]}-{lr[1]}")
                            else:
                                cite_lines.append(f"  - {f}")
                        out = f"**{qa['question']}**\n\n{qa['answer']}"
                        if cite_lines:
                            out += "\n\n---\n**Citations (file/line):**\n" + "\n".join(cite_lines)
                        out += "\n\n**Attribution:** Source: static (day_one_qa_synthesis from module_graph.json, lineage_graph.json). No LLM used for this response."
                        return out
            except Exception:
                pass

        # Multi-step tool chaining: split on " and " and run each part
        parts = [p.strip() for p in re.split(r"\s+and\s+", q_lower) if p.strip()]
        if len(parts) >= 2:
            results = []
            all_citations = []
            for i, part in enumerate(parts):
                res = self._run_single_query(part)
                if res:
                    results.append(f"## Part {i+1}\n{res.answer}")
                    all_citations.extend(res.citations[:5])
            if results:
                out = "\n\n".join(results)
                cite_lines = []
                seen = set()
                for c in all_citations[:12]:
                    key = (c.file or "", c.line_range)
                    if key in seen:
                        continue
                    seen.add(key)
                    if c.file:
                        lr = f" lines {c.line_range[0]}-{c.line_range[1]}" if c.line_range else ""
                        cite_lines.append(f"  - {c.file}{lr} ({c.artifact})")
                    elif c.artifact:
                        cite_lines.append(f"  - {c.artifact}")
                if cite_lines:
                    out += "\n\n---\n**Citations (file/line):**\n" + "\n".join(cite_lines)
                out += "\n\n**Attribution:** Source: static (lineage_graph.json, module_graph.json). No LLM used for this response."
                return out
        # Single intent
        result = self._run_single_query(q_lower)
        if result is None:
            result = self.tool_ingestion_and_outputs()

        out = result.answer
        if result.citations:
            cite_lines = []
            for c in result.citations[:8]:
                if c.file:
                    lr = f" lines {c.line_range[0]}-{c.line_range[1]}" if c.line_range else ""
                    cite_lines.append(f"  - {c.file}{lr} ({c.artifact})")
                elif c.artifact:
                    cite_lines.append(f"  - {c.artifact}")
            if cite_lines:
                out += "\n\n---\n**Citations (file/line):**\n" + "\n".join(cite_lines)
        out += "\n\n**Attribution:** " + result.attribution
        return out
