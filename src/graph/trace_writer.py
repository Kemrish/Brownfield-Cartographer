"""Write cartography_trace.jsonl — audit log of analysis steps and findings."""

import json
from pathlib import Path
from typing import Any


def write_trace_event(path: Path, event: dict[str, Any]) -> None:
    """Append one JSON line to cartography_trace.jsonl."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, default=str) + "\n")


def init_trace(cartography_dir: Path, repo_path: str, started_at: str) -> None:
    """Write trace start event."""
    write_trace_event(
        cartography_dir / "cartography_trace.jsonl",
        {
            "event": "analysis_started",
            "repo_path": repo_path,
            "started_at": started_at,
        },
    )


def trace_surveyor_done(cartography_dir: Path, stats: dict) -> None:
    """Log Surveyor completion."""
    write_trace_event(
        cartography_dir / "cartography_trace.jsonl",
        {"event": "surveyor_done", "stats": stats},
    )


def trace_hydrologist_done(cartography_dir: Path, stats: dict) -> None:
    """Log Hydrologist completion."""
    write_trace_event(
        cartography_dir / "cartography_trace.jsonl",
        {"event": "hydrologist_done", "stats": stats},
    )


def trace_semanticist_done(cartography_dir: Path, domain_summary: dict) -> None:
    """Log Semanticist completion."""
    write_trace_event(
        cartography_dir / "cartography_trace.jsonl",
        {"event": "semanticist_done", "domain_summary": domain_summary},
    )


def trace_artifacts_written(
    cartography_dir: Path,
    paths: list[str],
    duration_seconds: float,
) -> None:
    """Log artifacts written."""
    write_trace_event(
        cartography_dir / "cartography_trace.jsonl",
        {
            "event": "artifacts_written",
            "paths": paths,
            "duration_seconds": duration_seconds,
        },
    )


def trace_archivist_artifact(
    cartography_dir: Path,
    artifact_name: str,
    path: str,
    evidence_sources: list[str],
    confidence: str = "high",
) -> None:
    """Log a single Archivist artifact write with timestamp, evidence sources, and confidence for auditability."""
    from datetime import datetime
    write_trace_event(
        cartography_dir / "cartography_trace.jsonl",
        {
            "event": "archivist_artifact_written",
            "artifact": artifact_name,
            "path": path,
            "timestamp": datetime.now().isoformat(),
            "evidence_sources": evidence_sources,
            "confidence": confidence,
        },
    )
