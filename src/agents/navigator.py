"""Navigator agent: LangGraph-style agent with 4 tools (find_implementation, trace_lineage, blast_radius, explain_module)
against the knowledge graph; agent loop for NL routing; file/line citations and static-vs-LLM attribution."""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


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
    """Structured result from a Navigator tool: answer text + citations + attribution."""
    answer: str
    citations: list[Citation] = field(default_factory=list)
    attribution: str = "static (from .cartography/lineage_graph.json and module_graph.json)"


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
            return ToolResult("No lineage graph loaded.", citations=[], attribution="static (no artifacts)")
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
            attribution="static (from .cartography/lineage_graph.json)",
        )

    def tool_critical_path(self) -> ToolResult:
        """Tool 2: Longest dependency chain (critical path). Day-One Q3 blast-radius context."""
        if not self._lineage_graph:
            return ToolResult("No lineage graph loaded.", citations=[], attribution="static (no artifacts)")
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
        return ToolResult(answer=answer, citations=citations[:15], attribution="static (from .cartography/lineage_graph.json)")

    def tool_blast_radius(self, node: str, direction: str = "downstream") -> ToolResult:
        """Tool 3: Upstream/downstream impact of a node (blast radius)."""
        if not self._lineage_graph:
            return ToolResult("No lineage graph loaded.", citations=[], attribution="static (no artifacts)")
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
        return ToolResult(answer=answer, citations=citations, attribution="static (from .cartography/lineage_graph.json)")

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
                return ToolResult("\n".join(lines), citations=citations, attribution="static (from .cartography/lineage_graph.json)")
        if self._module_graph:
            hubs = self._module_graph.get("hub_modules", [])[:15]
            if hubs:
                return ToolResult("Hub modules (high PageRank):\n" + "\n".join(f"  - {h}" for h in hubs), citations=citations, attribution="static (from .cartography/module_graph.json)")
        return ToolResult("Specify a dataset for column lineage (e.g. 'column lineage for customers') or ask for 'hub modules'.", citations=citations, attribution="static")

    # ---------- Four named tools (spec: find_implementation, trace_lineage, blast_radius, explain_module) ----------

    def find_implementation(self, module_or_dataset: str) -> ToolResult:
        """Find where a module or dataset is implemented: hub modules, entry points, or column lineage."""
        if self._module_graph and module_or_dataset in [n.get("path") for n in self._module_graph.get("nodes", [])]:
            return self.explain_module(module_or_dataset)
        return self.tool_column_lineage_and_hub_modules(dataset=module_or_dataset)

    def trace_lineage(self) -> ToolResult:
        """Trace data lineage: sources, sinks, and critical path."""
        ing = self.tool_ingestion_and_outputs()
        crit = self.tool_critical_path()
        combined = ing.answer + "\n\n" + crit.answer
        return ToolResult(answer=combined, citations=ing.citations + crit.citations[:5], attribution=ing.attribution)

    def explain_module(self, module_path: str) -> ToolResult:
        """Explain a module: purpose statement and domain from the knowledge graph."""
        if not self._module_graph:
            return ToolResult("No module graph loaded.", citations=[], attribution="static (no artifacts)")
        for n in self._module_graph.get("nodes", []):
            if n.get("path") == module_path:
                purpose = n.get("purpose_statement") or "(none)"
                domain = n.get("domain_cluster") or "uncategorized"
                loc = n.get("lines_of_code") or 0
                lang = n.get("language", "")
                answer = f"**{module_path}** ({lang}, {loc} LOC)\n- Purpose: {purpose}\n- Domain: {domain}"
                return ToolResult(answer, citations=[Citation(artifact="module_graph.json", source="static")], attribution="static (from .cartography/module_graph.json)")
        return ToolResult(f"Module not found: {module_path}", citations=[], attribution="static")

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

    # ---------- Agent loop: route natural-language query to tools, return answer with citations and attribution ----------

    def query(self, question: str) -> str:
        """Route natural-language question to the four named tools; return answer with file/line citations and static-vs-LLM attribution."""
        q = question.lower().strip()
        if not q:
            return "Ask a Day-One question: 'sources', 'sinks', 'critical path', 'blast radius of <node>', 'hub modules', 'column lineage for <dataset>'. Attribution: static (from cartography artifacts)."

        result: Optional[ToolResult] = None

        if re.search(r"\bcritical\s*path\b|\blongest\s*path\b|\bchain\b", q):
            result = self.tool_critical_path()
        elif re.search(r"\bblast\s*radius\b|\bimpact\b|\bdownstream\b|\bupstream\b", q):
            match = re.search(r"(?:of|for)\s+([^\s?]+)", q, re.IGNORECASE)
            node = match.group(1).strip() if match else None
            if node:
                direction = "downstream" if "upstream" not in q else "upstream"
                result = self.tool_blast_radius(node, direction=direction)
            else:
                result = ToolResult("Specify a node: e.g. 'blast radius of stg_orders'", attribution="static")
        elif re.search(r"\bcolumn\s*lineage\b|\bcolumns?\b", q):
            match = re.search(r"(?:for|of)\s+([^\s?]+)", q, re.IGNORECASE)
            dataset = match.group(1).strip() if match else None
            result = self.tool_column_lineage_and_hub_modules(dataset=dataset)
            if dataset and "Specify a dataset" in result.answer:
                result = ToolResult(f"No column lineage found for '{dataset}'.", citations=result.citations, attribution=result.attribution)
        elif re.search(r"\bhub\b|\bpagerank\b|\bimportant\s*modules\b", q):
            result = self.tool_column_lineage_and_hub_modules(dataset=None)
        elif re.search(r"\bsources?\b|\bsinks?\b|\bentry\b|\bingest|ingestion|outputs?\b|\bexit\b", q):
            result = self.tool_ingestion_and_outputs()

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
