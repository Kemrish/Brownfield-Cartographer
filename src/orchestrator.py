"""Orchestrator: run Surveyor -> Semanticist -> Hydrologist, then Archivist; write artifacts and trace."""

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


def clone_repo_if_needed(repo_path: str, full_history: bool = False) -> Path:
    """If repo_path looks like a GitHub URL, clone to a temp dir and return that path."""
    s = repo_path.strip()
    if s.startswith("http") and ("github" in s or "gitlab" in s):
        try:
            import tempfile
            import subprocess
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


def run_analysis(
    repo_path: Union[str, Path],
    output_dir: Union[str, Path, None] = None,
    full_history: bool = False,
    use_llm: bool = True,
) -> dict:
    """
    Pipeline: Surveyor (structure) -> Semanticist (purpose/domain, optional LLM) -> Hydrologist (lineage) -> Archivist (artifacts).
    Write to output_dir or repo_path/.cartography. Returns paths and stats.
    """
    start_time = time.time()

    path = clone_repo_if_needed(str(repo_path), full_history=full_history)
    out = Path(output_dir) if output_dir else path
    out.mkdir(parents=True, exist_ok=True)
    cartography_dir = out / ".cartography"
    cartography_dir.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now().isoformat()
    init_trace(cartography_dir, str(path), started_at)

    # Phase 1: Surveyor (static structure)
    surveyor = Surveyor(path)
    surveyor.run()
    surveyor_stats = surveyor.get_stats()
    trace_surveyor_done(cartography_dir, surveyor_stats)

    # Phase 2: Semanticist (enrich module nodes with purpose and domain; LLM if OPENAI_API_KEY set)
    semanticist = Semanticist(path, use_llm=use_llm)
    semanticist.run(surveyor.graph)
    domain_summary = semanticist.get_domain_summary(surveyor.graph)
    trace_semanticist_done(cartography_dir, domain_summary)

    # Re-write module graph with enriched nodes (purpose_statement, domain_cluster)
    # Surveyor's graph was mutated by Semanticist; we need to write after enrichment
    # and pass the same metadata we'll build below. So we build metadata after Hydrologist.
    # Phase 3: Hydrologist (data lineage)
    hydrologist = Hydrologist(path)
    hydrologist.run()
    hydrologist_stats = hydrologist.get_stats()
    trace_hydrologist_done(cartography_dir, hydrologist_stats)

    duration = time.time() - start_time
    metadata = {
        "repo_path": str(path),
        "analyzed_at": started_at,
        "total_files": surveyor_stats["files_analyzed"],
        "total_modules": surveyor_stats["files_analyzed"],
        "total_datasets": hydrologist_stats["total_datasets"],
        "total_transformations": hydrologist_stats["total_transformations"],
        "languages_detected": surveyor_stats["languages"],
        "analysis_duration_seconds": round(duration, 2),
    }

    # Write graph artifacts (Surveyor graph now has enriched nodes)
    module_graph_path = surveyor.write_module_graph(out, metadata=metadata)
    lineage_graph_path = hydrologist.write_lineage_graph(out, metadata=metadata)

    # Phase 4: Archivist (CODEBASE.md, onboarding_brief.md)
    archivist = Archivist(cartography_dir)
    doc_paths = archivist.generate_all()

    # Optional: export GraphML for visualization
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

    artifact_paths = [
        module_graph_path,
        lineage_graph_path,
        doc_paths.get("CODEBASE.md", ""),
        doc_paths.get("onboarding_brief.md", ""),
        str(cartography_dir / "cartography_trace.jsonl"),
    ]
    if module_graphml.exists():
        artifact_paths.append(str(module_graphml))
    if lineage_graphml.exists():
        artifact_paths.append(str(lineage_graphml))
    trace_artifacts_written(cartography_dir, artifact_paths, round(duration, 2))

    return {
        "repo_path": str(path),
        "module_graph": module_graph_path,
        "lineage_graph": lineage_graph_path,
        "codebase_md": doc_paths.get("CODEBASE.md"),
        "onboarding_brief_md": doc_paths.get("onboarding_brief.md"),
        "trace_path": str(cartography_dir / "cartography_trace.jsonl"),
        "module_graphml": str(module_graphml) if module_graphml.exists() else None,
        "lineage_graphml": str(lineage_graphml) if lineage_graphml.exists() else None,
        "sources": hydrologist.find_sources(),
        "sinks": hydrologist.find_sinks(),
        "critical_path": hydrologist.compute_critical_path(),
        "duration_seconds": round(duration, 2),
        "surveyor_stats": surveyor_stats,
        "hydrologist_stats": hydrologist_stats,
        "semanticist_stats": semanticist.get_stats(),
    }
