"""Orchestrator: run Surveyor then Hydrologist and write artifacts to .cartography/."""

import time
from datetime import datetime
from pathlib import Path
from typing import Union

from src.agents.surveyor import Surveyor
from src.agents.hydrologist import Hydrologist


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
) -> dict:
    """
    Run Surveyor then Hydrologist on the repo. Write to output_dir or repo_path/.cartography.
    Returns paths to generated artifacts and analysis stats.
    """
    start_time = time.time()

    path = clone_repo_if_needed(str(repo_path), full_history=full_history)
    out = Path(output_dir) if output_dir else path
    out.mkdir(parents=True, exist_ok=True)
    cartography_dir = out / ".cartography"
    cartography_dir.mkdir(parents=True, exist_ok=True)

    # Phase 1: Surveyor (static structure)
    surveyor = Surveyor(path)
    surveyor.run()
    surveyor_stats = surveyor.get_stats()

    # Phase 2: Hydrologist (data lineage)
    hydrologist = Hydrologist(path)
    hydrologist.run()
    hydrologist_stats = hydrologist.get_stats()

    # Compute duration
    duration = time.time() - start_time

    # Build metadata
    metadata = {
        "repo_path": str(path),
        "analyzed_at": datetime.now().isoformat(),
        "total_files": surveyor_stats["files_analyzed"],
        "total_modules": surveyor_stats["files_analyzed"],
        "total_datasets": hydrologist_stats["total_datasets"],
        "total_transformations": hydrologist_stats["total_transformations"],
        "languages_detected": surveyor_stats["languages"],
        "analysis_duration_seconds": round(duration, 2),
    }

    # Write artifacts with metadata
    module_graph_path = surveyor.write_module_graph(out, metadata=metadata)
    lineage_graph_path = hydrologist.write_lineage_graph(out, metadata=metadata)

    return {
        "repo_path": str(path),
        "module_graph": module_graph_path,
        "lineage_graph": lineage_graph_path,
        "sources": hydrologist.find_sources(),
        "sinks": hydrologist.find_sinks(),
        "critical_path": hydrologist.compute_critical_path(),
        "duration_seconds": round(duration, 2),
        "surveyor_stats": surveyor_stats,
        "hydrologist_stats": hydrologist_stats,
    }
