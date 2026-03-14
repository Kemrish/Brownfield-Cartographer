"""CLI entry point: analyze a repo, query cartography, export graphs."""

from pathlib import Path

import click
from dotenv import load_dotenv

# Load .env from project root (cwd when running from repo) so OPENROUTER_API_KEY / OPENAI_API_KEY are set
load_dotenv()
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.orchestrator import run_analysis
from src.agents.navigator import Navigator

console = Console()


@click.group()
def main() -> None:
    """Brownfield Cartographer — codebase intelligence for rapid FDE onboarding."""
    pass


@main.command("analyze")
@click.argument("repo_path", type=click.STRING, required=True)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Write .cartography here (default: inside repo_path).",
)
@click.option(
    "--full-history",
    "-f",
    is_flag=True,
    default=False,
    help="Clone full git history (slower, but enables git velocity analysis).",
)
@click.option(
    "--no-llm",
    is_flag=True,
    default=False,
    help="Disable LLM-powered Semanticist; use heuristics only (no OPENAI_API_KEY needed).",
)
def analyze(repo_path: str, output_dir: Path | None, full_history: bool, no_llm: bool) -> None:
    """Analyze a repository (local path or GitHub URL) and write module_graph.json and lineage_graph.json to .cartography/."""
    console.print(Panel.fit(f"[bold blue]Brownfield Cartographer[/bold blue]\nAnalyzing: {repo_path}"))

    try:
        result = run_analysis(
            repo_path, output_dir=output_dir, full_history=full_history, use_llm=not no_llm
        )

        # Summary panel
        surveyor = result.get("surveyor_stats", {})
        hydro = result.get("hydrologist_stats", {})
        semanticist = result.get("semanticist_stats", {})
        _llm_line = "LLM: " + str(semanticist.get("llm_calls", 0)) + " calls" if semanticist.get("llm_enabled") else "heuristics only"

        summary = f"""[green]Analysis complete in {result['duration_seconds']}s[/green]

[bold]Files:[/bold] {surveyor.get('files_analyzed', 0)} analyzed ({surveyor.get('total_loc', 0)} LOC)
[bold]Languages:[/bold] {', '.join(surveyor.get('languages', [])) or 'N/A'}
[bold]Datasets:[/bold] {hydro.get('total_datasets', 0)}
[bold]Transformations:[/bold] {hydro.get('total_transformations', 0)}
[bold]dbt refs found:[/bold] {hydro.get('dbt_refs_found', 0)}
[bold]Entry points:[/bold] {surveyor.get('entry_points', 0)}
[bold]Hub modules:[/bold] {surveyor.get('hub_modules', 0)}
[bold]Semanticist:[/bold] {_llm_line}"""

        # Add SQL-specific stats if present
        sql_stats = surveyor.get('sql', {})
        if sql_stats:
            summary += f"""

[bold cyan]SQL Analysis:[/bold cyan]
  Files: {sql_stats.get('files', 0)} | Tables: {sql_stats.get('tables_referenced', 0)} referenced, {sql_stats.get('tables_written', 0)} written
  CTEs: {sql_stats.get('ctes_defined', 0)} | Aggregations: {sql_stats.get('with_aggregation', 0)} | Window funcs: {sql_stats.get('with_window_functions', 0)}"""

        # Add YAML-specific stats if present
        yaml_stats = surveyor.get('yaml', {})
        if yaml_stats:
            summary += f"""

[bold yellow]YAML Analysis:[/bold yellow]
  Files: {yaml_stats.get('files', 0)} | Keys: {yaml_stats.get('total_keys', 0)} | Max depth: {yaml_stats.get('max_depth', 0)}"""

        console.print(Panel(summary, title="Analysis Summary"))

        console.print(f"\n[dim]Artifacts written to:[/dim]")
        console.print(f"  Module graph: {result['module_graph']}")
        console.print(f"  Lineage graph: {result['lineage_graph']}")
        if result.get("codebase_md"):
            console.print(f"  CODEBASE.md: {result['codebase_md']}")
        if result.get("onboarding_brief_md"):
            console.print(f"  onboarding_brief.md: {result['onboarding_brief_md']}")
        if result.get("trace_path"):
            console.print(f"  Trace: {result['trace_path']}")
        if result.get("module_graphml"):
            console.print(f"  Module GraphML: {result['module_graphml']}")
        if result.get("lineage_graphml"):
            console.print(f"  Lineage GraphML: {result['lineage_graphml']}")

        if output_dir is None and ("github" in repo_path or "gitlab" in repo_path):
            console.print("\n[yellow]Tip:[/yellow] Repo was cloned to a temp dir. Use [bold]-o ./artifacts[/bold] to save outputs permanently.")

        # Lineage sources table
        sources = result.get("sources", [])
        if sources:
            table = Table(title="Data Sources (entry points)", show_lines=False)
            table.add_column("Source", style="cyan")
            for s in sources[:10]:
                if not s.startswith("sql:"):  # Don't show transformation IDs
                    table.add_row(s)
            if len([s for s in sources if not s.startswith("sql:")]) > 0:
                console.print(table)

        # Lineage sinks table
        sinks = result.get("sinks", [])
        if sinks:
            table = Table(title="Data Sinks (outputs)", show_lines=False)
            table.add_column("Sink", style="green")
            for s in sinks[:10]:
                if not s.startswith("sql:"):
                    table.add_row(s)
            if len([s for s in sinks if not s.startswith("sql:")]) > 0:
                console.print(table)

        # Critical path
        crit = result.get("critical_path", [])
        if crit and len(crit) > 1:
            console.print(f"\n[bold]Critical path ({len(crit)} nodes):[/bold]")
            console.print("  " + " -> ".join(crit[:7]))
            if len(crit) > 7:
                console.print(f"  ... and {len(crit) - 7} more")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)


@main.command("query")
@click.argument("cartography_dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.argument("question", type=click.STRING, required=False, default="")
def query(cartography_dir: Path, question: str) -> None:
    """Query cartography artifacts (sources, sinks, critical path, blast radius, hub modules)."""
    cartography_dir = cartography_dir / ".cartography" if (cartography_dir / ".cartography").exists() else cartography_dir
    nav = Navigator(cartography_dir)
    if question:
        answer = nav.query(question)
        console.print(Panel(answer, title="Answer"))
    else:
        console.print("[bold]Example questions:[/bold]")
        console.print("  sources")
        console.print("  sinks")
        console.print("  critical path")
        console.print("  blast radius of stg_orders")
        console.print("  hub modules")
        console.print("  column lineage for customers")
        console.print("\nRun: [bold]cartographer query <path> '<question>'[/bold]")


@main.command("export-graphml")
@click.argument("repo_or_cartography", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--output-dir", "-o", type=click.Path(path_type=Path), default=None, help="Directory for .graphml files")
def export_graphml(repo_or_cartography: Path, output_dir: Path | None) -> None:
    """Export module and lineage graphs to GraphML (for Gephi, Cytoscape). Expects .cartography/ to exist."""
    cartography = repo_or_cartography / ".cartography"
    if not cartography.exists():
        cartography = repo_or_cartography
    if not (cartography / "module_graph.json").exists():
        console.print("[red]No module_graph.json found. Run 'cartographer analyze' first.[/red]")
        raise SystemExit(1)
    out = output_dir or cartography
    module_path = out / "module_graph.graphml"
    lineage_path = out / "lineage_graph.graphml"
    # We need to re-load graphs and write GraphML; Navigator doesn't expose graph. Use a small helper.
    import json
    import networkx as nx
    with open(cartography / "module_graph.json", "r", encoding="utf-8") as f:
        mg = json.load(f)
    # Build minimal DiGraph from module graph edges
    G = nx.DiGraph()
    for n in mg.get("nodes", []):
        G.add_node(n.get("path", ""), **{k: v for k, v in n.items() if isinstance(v, (str, int, float, bool)) and k != "path"})
    for e in mg.get("edges", []):
        G.add_edge(e.get("source", ""), e.get("target", ""))
    out.mkdir(parents=True, exist_ok=True)
    nx.write_graphml(G, module_path, encoding="utf-8")
    console.print(f"[green]Module graph:[/green] {module_path}")
    with open(cartography / "lineage_graph.json", "r", encoding="utf-8") as f:
        lg = json.load(f)
    H = nx.DiGraph()
    for d in lg.get("datasets", []):
        H.add_node(d.get("name", ""), kind="dataset")
    for t in lg.get("transformations", []):
        H.add_node(t.get("id", ""), kind="transformation")
    for e in lg.get("edges", []):
        H.add_edge(e.get("source", ""), e.get("target", ""))
    nx.write_graphml(H, lineage_path, encoding="utf-8")
    console.print(f"[green]Lineage graph:[/green] {lineage_path}")


if __name__ == "__main__":
    main()
