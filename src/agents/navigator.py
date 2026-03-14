"""Navigator agent: query the cartography artifacts (sources, sinks, blast radius, critical path)."""

import json
import re
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


class Navigator:
    """Query agent: answer questions over the cartography artifacts."""

    def __init__(self, cartography_dir: str | Path) -> None:
        self.cartography_dir = Path(cartography_dir)
        self._module_graph: Optional[dict] = None
        self._lineage_graph: Optional[dict] = None
        self._load()

    def _load(self) -> None:
        self._module_graph, self._lineage_graph = _load_cartography(self.cartography_dir)

    def sources(self) -> list[str]:
        """Return data source names (entry points)."""
        if not self._lineage_graph:
            return []
        return [s for s in self._lineage_graph.get("sources", []) if not s.startswith("sql:")]

    def sinks(self) -> list[str]:
        """Return data sink names (outputs)."""
        if not self._lineage_graph:
            return []
        return [s for s in self._lineage_graph.get("sinks", []) if not s.startswith("sql:")]

    def critical_path(self) -> list[str]:
        """Return the longest dependency chain."""
        if not self._lineage_graph:
            return []
        return self._lineage_graph.get("critical_path", [])

    def blast_radius(self, node: str, direction: str = "downstream") -> list[str]:
        """Return nodes reachable from node (downstream) or that reach it (upstream)."""
        if not self._lineage_graph:
            return []
        edges = self._lineage_graph.get("edges", [])
        # Build adjacency
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
        """Return column lineage edges where dataset is source or target."""
        if not self._lineage_graph:
            return []
        edges = self._lineage_graph.get("column_lineage", [])
        return [
            e for e in edges
            if e.get("source_dataset") == dataset or e.get("target_dataset") == dataset
        ]

    def hub_modules(self) -> list[str]:
        """Return high PageRank module paths."""
        if not self._module_graph:
            return []
        return self._module_graph.get("hub_modules", [])

    def entry_points(self) -> list[str]:
        """Return entry point module paths."""
        if not self._module_graph:
            return []
        return self._module_graph.get("entry_points", [])

    def network_metrics(self) -> dict[str, Any]:
        """Return network metrics from lineage graph if present."""
        if not self._lineage_graph:
            return {}
        nm = self._lineage_graph.get("network_metrics") or {}
        return dict(nm)

    def query(self, question: str) -> str:
        """Answer a natural-language question about the codebase (keyword-based)."""
        q = question.lower().strip()
        if not q:
            return "Ask a question about the codebase (e.g. 'sources', 'sinks', 'critical path', 'blast radius of X')."

        if re.search(r"\bsources?\b|\bentry\b|\bingest", q):
            s = self.sources()
            return "Data sources (entry points):\n" + "\n".join(f"  - {x}" for x in s[:20]) if s else "No sources found."

        if re.search(r"\bsinks?\b|\boutputs?\b|\bexit", q):
            s = self.sinks()
            return "Data sinks (outputs):\n" + "\n".join(f"  - {x}" for x in s[:20]) if s else "No sinks found."

        if re.search(r"\bcritical\s*path\b|\blongest\s*path\b|\bchain\b", q):
            path = self.critical_path()
            path_show = [n for n in path if not n.startswith("sql:")][:10]
            return "Critical path:\n  " + " -> ".join(path_show) if path_show else "No critical path."

        if re.search(r"\bblast\s*radius\b|\bimpact\b|\bdownstream\b|\bupstream\b", q):
            # Try to extract node name
            match = re.search(r"(?:of|for)\s+([^\s?]+)", q, re.IGNORECASE)
            node = match.group(1).strip() if match else None
            if not node:
                return "Specify a node: e.g. 'blast radius of stg_orders'"
            direction = "downstream" if "upstream" not in q else "upstream"
            radius = self.blast_radius(node, direction=direction)
            label = "Downstream" if direction == "downstream" else "Upstream"
            return f"{label} of '{node}': {len(radius)} nodes\n" + "\n".join(f"  - {x}" for x in radius[:25])

        if re.search(r"\bhub\b|\bpagerank\b|\bimportant\s*modules\b", q):
            hubs = self.hub_modules()
            return "Hub modules (high impact):\n" + "\n".join(f"  - {x}" for x in hubs[:15]) if hubs else "No hub modules."

        if re.search(r"\bcolumn\s*lineage\b|\bcolumns?\b", q):
            match = re.search(r"(?:for|of)\s+([^\s?]+)", q, re.IGNORECASE)
            dataset = match.group(1).strip() if match else None
            if dataset:
                edges = self.column_lineage_for_dataset(dataset)
                if not edges:
                    return f"No column lineage found for '{dataset}'."
                lines = [f"  {e.get('source_dataset')}.{e.get('source_column')} -> {e.get('target_dataset')}.{e.get('target_column')}" for e in edges[:20]]
                return f"Column lineage for '{dataset}':\n" + "\n".join(lines)
            return "Specify a dataset: e.g. 'column lineage for customers'"

        return (
            "Try: 'sources', 'sinks', 'critical path', 'blast radius of <node>', "
            "'hub modules', 'column lineage for <dataset>'."
        )
