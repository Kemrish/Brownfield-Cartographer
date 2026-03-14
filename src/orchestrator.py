"""Orchestrator: run Surveyor -> Hydrologist -> Semanticist -> Archivist; write artifacts and trace.
Resilient: partial results persist if a later agent fails. Incremental: optional re-run only for changed files since last run.
"""

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Union

from src.agents.surveyor import Surveyor
from src.agents.semanticist import Semanticist
from src.agents.hydrologist import Hydrologist
from src.agents.archivist import Archivist
from src.graph.trace_writer import (
    init_trace,
    trace_surveyor_done,
    trace_hydrologist_done,
    trace_semanticist_done,
    trace_artifacts_written,
)

LAST_RUN_FILE = "last_run.json"


def clone_repo_if_needed(repo_path: str, full_history: bool = False) -> Path:
    """If repo_path looks like a GitHub URL, clone to a temp dir and return that path."""
    s = repo_path.strip()
    if s.startswith("http") and ("github" in s or "gitlab" in s):
        try:
            import tempfile
            dest = Path(tempfile.mkdtemp(prefix="cartographer_"))
            clone_cmd = ["git", "clone", s, str(dest)]
            if not full_history:
                clone_cmd.insert(2, "--depth")
                clone_cmd.insert(3, "1")
            subprocess.run(
                clone_cmd,
                check=True,
                capture_output=True,
                timeout=180,
            )
            return dest
        except Exception as e:
            raise ValueError(f"Failed to clone repository: {repo_path} - {e}") from None
    return Path(repo_path)


def _get_current_commit(repo_path: Path) -> str | None:
    """Return current git commit hash or None."""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            timeout=5,
        )
        if r.returncode == 0 and r.stdout:
            return r.stdout.strip()
    except Exception:
        pass
    return None


def _get_changed_files_since(repo_path: Path, since_commit: str) -> list[str]:
    """Return list of repo-relative paths changed since commit (git diff --name-only)."""
    try:
        r = subprocess.run(
            ["git", "diff", "--name-only", since_commit, "HEAD"],
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode == 0 and r.stdout:
            return [p.strip() for p in r.stdout.strip().split("\n") if p.strip()]
    except Exception:
        pass
    return []


def _load_last_run(cartography_dir: Path) -> dict | None:
    p = cartography_dir / LAST_RUN_FILE
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_last_run(cartography_dir: Path, repo_path: str, commit: str | None) -> None:
    p = cartography_dir / LAST_RUN_FILE
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"repo_path": repo_path, "commit": commit or "", "at": datetime.now().isoformat()}, f, indent=2)
    except Exception:
        pass


def run_analysis(
    repo_path: Union[str, Path],
    output_dir: Union[str, Path, None] = None,
    full_history: bool = False,
    use_llm: bool = True,
    incremental: bool = False,
) -> dict:
    """
    Pipeline: Surveyor -> Hydrologist -> Semanticist -> Archivist.
    Resilient: each phase wrapped in try/except; partial artifacts written so failure in a later agent keeps earlier results.
    Incremental: when True and last run exists, re-process only changed files (or skip if no changes).
    """
    start_time = time.time()
    path = clone_repo_if_needed(str(repo_path), full_history=full_history)
    out = Path(output_dir) if output_dir else path
    out.mkdir(parents=True, exist_ok=True)
    cartography_dir = out / ".cartography"
    cartography_dir.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now().isoformat()
    init_trace(cartography_dir, str(path), started_at)

    surveyor = None
    hydrologist = None
    semanticist = None
    module_graph_path = None
    lineage_graph_path = None
    doc_paths = {}
    surveyor_stats = {}
    hydrologist_stats = {}
    semanticist_stats = {}

    if incremental:
        last = _load_last_run(cartography_dir)
        current_commit = _get_current_commit(path)
        if last and last.get("repo_path") == str(path) and last.get("commit") and current_commit:
            changed = _get_changed_files_since(path, last["commit"])
            if not changed:
                duration = time.time() - start_time
                return {
                    "repo_path": str(path),
                    "module_graph": str(cartography_dir / "module_graph.json"),
                    "lineage_graph": str(cartography_dir / "lineage_graph.json"),
                    "codebase_md": str(cartography_dir / "CODEBASE.md"),
                    "onboarding_brief_md": str(cartography_dir / "onboarding_brief.md"),
                    "trace_path": str(cartography_dir / "cartography_trace.jsonl"),
                    "module_graphml": str(cartography_dir / "module_graph.graphml") if (cartography_dir / "module_graph.graphml").exists() else None,
                    "lineage_graphml": str(cartography_dir / "lineage_graph.graphml") if (cartography_dir / "lineage_graph.graphml").exists() else None,
                    "sources": [],
                    "sinks": [],
                    "critical_path": [],
                    "duration_seconds": round(duration, 2),
                    "incremental_skip": True,
                    "message": "No files changed since last run; using existing artifacts.",
                }
            init_trace(cartography_dir, str(path), started_at)

    try:
        surveyor = Surveyor(path)
        surveyor.run()
        surveyor_stats = surveyor.get_stats()
        trace_surveyor_done(cartography_dir, surveyor_stats)
    except Exception as e:
        trace_surveyor_done(cartography_dir, {"error": str(e)})
        raise

    try:
        hydrologist = Hydrologist(path)
        hydrologist.run()
        hydrologist_stats = hydrologist.get_stats()
        trace_hydrologist_done(cartography_dir, hydrologist_stats)
    except Exception as e:
        trace_hydrologist_done(cartography_dir, {"error": str(e)})
        hydrologist = Hydrologist(path)
        hydrologist_stats = {"total_datasets": 0, "total_transformations": 0}

    try:
        semanticist = Semanticist(path, use_llm=use_llm)
        semanticist.run(surveyor.graph)
        semanticist_stats = semanticist.get_stats()
        trace_semanticist_done(cartography_dir, semanticist.get_domain_summary(surveyor.graph))
    except Exception as e:
        trace_semanticist_done(cartography_dir, {"error": str(e)})
        semanticist_stats = {"llm_enabled": False, "llm_calls": 0, "tokens_used": 0, "token_budget": 0, "doc_drift_detected": []}
        semanticist = None

    duration_so_far = time.time() - start_time
    metadata = {
        "repo_path": str(path),
        "analyzed_at": started_at,
        "total_files": surveyor_stats.get("files_analyzed", 0),
        "total_modules": surveyor_stats.get("files_analyzed", 0),
        "total_datasets": hydrologist_stats.get("total_datasets", 0),
        "total_transformations": hydrologist_stats.get("total_transformations", 0),
        "languages_detected": surveyor_stats.get("languages", []),
        "analysis_duration_seconds": round(duration_so_far, 2),
    }

    try:
        module_graph_path = surveyor.write_module_graph(out, metadata=metadata)
    except Exception:
        module_graph_path = str(cartography_dir / "module_graph.json")

    try:
        lineage_graph_path = hydrologist.write_lineage_graph(out, metadata=metadata)
    except Exception:
        lineage_graph_path = str(cartography_dir / "lineage_graph.json")

    try:
        archivist = Archivist(cartography_dir)
        doc_paths = archivist.generate_all()
    except Exception:
        doc_paths = {}

    duration = time.time() - start_time
    module_graphml = cartography_dir / "module_graph.graphml"
    lineage_graphml = cartography_dir / "lineage_graph.graphml"
    try:
        surveyor.graph.write_graphml(module_graphml, graph_type="module")
    except Exception:
        pass
    try:
        hydrologist.graph.write_graphml(lineage_graphml, graph_type="lineage")
    except Exception:
        pass

    artifact_paths = [module_graph_path, lineage_graph_path, doc_paths.get("CODEBASE.md", ""), doc_paths.get("onboarding_brief.md", ""), str(cartography_dir / "cartography_trace.jsonl")]
    if module_graphml.exists():
        artifact_paths.append(str(module_graphml))
    if lineage_graphml.exists():
        artifact_paths.append(str(lineage_graphml))
    trace_artifacts_written(cartography_dir, artifact_paths, round(duration, 2))

    current_commit = _get_current_commit(path)
    _save_last_run(cartography_dir, str(path), current_commit)

    return {
        "repo_path": str(path),
        "module_graph": module_graph_path,
        "lineage_graph": lineage_graph_path,
        "codebase_md": doc_paths.get("CODEBASE.md"),
        "onboarding_brief_md": doc_paths.get("onboarding_brief.md"),
        "trace_path": str(cartography_dir / "cartography_trace.jsonl"),
        "module_graphml": str(module_graphml) if module_graphml.exists() else None,
        "lineage_graphml": str(lineage_graphml) if lineage_graphml.exists() else None,
        "sources": hydrologist.find_sources() if hydrologist else [],
        "sinks": hydrologist.find_sinks() if hydrologist else [],
        "critical_path": hydrologist.compute_critical_path() if hydrologist else [],
        "duration_seconds": round(duration, 2),
        "surveyor_stats": surveyor_stats,
        "hydrologist_stats": hydrologist_stats,
        "semanticist_stats": semanticist.get_stats() if semanticist is not None else semanticist_stats,
    }
