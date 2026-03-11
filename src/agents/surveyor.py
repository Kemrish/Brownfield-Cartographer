"""Surveyor agent: static structure, module graph, PageRank, git velocity, dead code candidates."""

import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode


def _run_git_log_follow(repo_path: Path, file_path: Path, days: int = 30) -> int:
    """Return number of commits touching file in the last `days` days."""
    if not (repo_path / ".git").exists():
        return 0
    try:
        result = subprocess.run(
            ["git", "log", "--follow", "--since", f"{days} days ago", "--oneline", "--", str(file_path)],
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            timeout=5,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
        if result.returncode == 0 and result.stdout:
            lines = result.stdout.strip().split("\n")
            return len([l for l in lines if l.strip()])
    except Exception:
        pass
    return 0


def _get_file_last_modified(path: Path) -> Optional[str]:
    """Get file's last modification time as ISO string."""
    try:
        mtime = path.stat().st_mtime
        return datetime.fromtimestamp(mtime).isoformat()
    except Exception:
        return None


class Surveyor:
    """Static structure analyst: builds module graph, PageRank, git velocity, dead code candidates."""

    SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", "target", "dbt_packages", "dist", "build", ".tox", ".mypy_cache"}

    def __init__(self, repo_root: str | Path) -> None:
        self.repo_root = Path(repo_root)
        self.analyzer = TreeSitterAnalyzer()
        self.graph = KnowledgeGraph()
        self._stats = {"files_analyzed": 0, "languages": set(), "total_loc": 0}
        self._high_velocity: list[str] = []
        self._dead_code_candidates: list[str] = []
        self._entry_points: list[str] = []
        self._hub_modules: list[str] = []

    def discover_files(self, extensions: Optional[set[str]] = None) -> list[Path]:
        """List analyzable source files under repo root."""
        if extensions is None:
            extensions = {".py", ".sql", ".yml", ".yaml", ".js", ".ts", ".jsx", ".tsx"}
        out: list[Path] = []
        try:
            for p in self.repo_root.rglob("*"):
                if p.is_file() and p.suffix.lower() in extensions:
                    try:
                        rel = p.relative_to(self.repo_root)
                        if any(part.startswith(".") for part in rel.parts):
                            continue
                        if any(part in self.SKIP_DIRS for part in rel.parts):
                            continue
                        out.append(p)
                    except ValueError:
                        pass
        except Exception:
            pass
        return out

    def analyze_module(self, path: Path) -> Optional[ModuleNode]:
        """Analyze one file and return ModuleNode."""
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return None

        path_posix = path.relative_to(self.repo_root).as_posix()
        node = self.analyzer.analyze_module(path, source)
        if node is None:
            return None

        node.path = path_posix
        node.last_modified = _get_file_last_modified(path)

        # Track stats
        self._stats["files_analyzed"] += 1
        self._stats["languages"].add(node.language)
        self._stats["total_loc"] += node.lines_of_code or 0

        return node

    def resolve_import_to_path(self, imp: str, from_file: str) -> Optional[str]:
        """Resolve Python import to a repo-relative path (simplified)."""
        from_path = Path(from_file)
        if imp.startswith("."):
            # relative
            base = from_path.parent
            parts = imp.lstrip(".").split(".")
            for _ in parts:
                base = base.parent if base != self.repo_root else base
            candidate = (self.repo_root / base / from_path.stem).with_suffix(".py")
            try:
                rel = candidate.relative_to(self.repo_root)
                return rel.as_posix()
            except ValueError:
                return None
        # top-level: look for imp.replace(".", "/) or imp.replace(".", "/) + __init__.py
        candidate = imp.replace(".", "/") + ".py"
        candidates = [
            self.repo_root / candidate,
            self.repo_root / candidate.replace(".py", "/__init__.py"),
        ]
        for c in candidates:
            if c.exists():
                try:
                    return c.relative_to(self.repo_root).as_posix()
                except ValueError:
                    pass
        return None

    def extract_git_velocity(self, path: Path, days: int = 30) -> int:
        """Commits touching this file in the last `days` days."""
        return _run_git_log_follow(self.repo_root, path, days)

    def run(self) -> KnowledgeGraph:
        """Build module graph: analyze all files, add nodes and import edges, set velocity."""
        files = self.discover_files()
        path_to_node: dict[str, ModuleNode] = {}

        # Phase 1: Analyze all files
        for f in files:
            node = self.analyze_module(f)
            if node is None:
                continue
            full_path = self.repo_root / node.path
            velocity = self.extract_git_velocity(full_path, days=30)
            node.change_velocity_30d = velocity
            path_to_node[node.path] = node
            self.graph.add_module_node(node)

            # Track entry points
            if node.is_entry_point:
                self._entry_points.append(node.path)

        # Phase 2: Build import edges (Python only)
        for path, node in path_to_node.items():
            for imp in node.imports:
                target = self.resolve_import_to_path(imp, path)
                if target and target in path_to_node:
                    self.graph.add_import_edge(path, target)

        # Phase 3: Compute PageRank and find hub modules
        pagerank = self.graph.compute_module_pagerank()
        sorted_by_pr = sorted(pagerank.items(), key=lambda x: -x[1])
        n_hubs = max(1, len(sorted_by_pr) // 10)  # top 10%
        self._hub_modules = [p for p, _ in sorted_by_pr[:n_hubs] if pagerank.get(p, 0) > 0]

        # Phase 4: High-velocity files (top 20% by commit count)
        velocities = [(p, n.change_velocity_30d or 0) for p, n in path_to_node.items()]
        velocities.sort(key=lambda x: -x[1])
        n_high = max(1, len(velocities) // 5)
        self._high_velocity = [p for p, v in velocities[:n_high] if v > 0]

        # Phase 5: Dead code detection (only for Python files with no incoming edges)
        all_referenced = set()
        for _, node in path_to_node.items():
            for imp in node.imports:
                t = self.resolve_import_to_path(imp, node.path)
                if t:
                    all_referenced.add(t)

        # Only Python files can be "dead code candidates" based on import analysis
        for path, node in path_to_node.items():
            if node.language != "python":
                continue  # SQL/YAML files are not "dead code" - they're used differently
            if path in all_referenced:
                continue
            if node.is_entry_point:
                continue  # entry points are used
            if path.endswith("__init__.py"):
                continue  # package markers are not dead code
            self.graph.module_nodes[path].is_dead_code_candidate = True
            self._dead_code_candidates.append(path)

        return self.graph

    def get_stats(self) -> dict:
        """Return analysis statistics."""
        return {
            "files_analyzed": self._stats["files_analyzed"],
            "languages": list(self._stats["languages"]),
            "total_loc": self._stats["total_loc"],
            "entry_points": len(self._entry_points),
            "hub_modules": len(self._hub_modules),
            "dead_code_candidates": len(self._dead_code_candidates),
            "high_velocity_files": len(self._high_velocity),
        }

    def write_module_graph(self, out_dir: str | Path, metadata: Optional[dict] = None) -> str:
        """Write .cartography/module_graph.json. Returns path to file."""
        out_path = Path(out_dir) / ".cartography" / "module_graph.json"
        self.graph.write_module_graph_json(
            out_path,
            high_velocity=self._high_velocity,
            entry_points=self._entry_points,
            dead_code_candidates=self._dead_code_candidates,
            hub_modules=self._hub_modules,
            metadata=metadata,
        )
        return str(out_path)
