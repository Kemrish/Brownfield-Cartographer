"""CLI entry point: analyze a repo (local path or GitHub URL)."""

from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.orchestrator import run_analysis

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
def analyze(repo_path: str, output_dir: Path | None, full_history: bool) -> None:
    """Analyze a repository (local path or GitHub URL) and write module_graph.json and lineage_graph.json to .cartography/."""
    console.print(Panel.fit(f"[bold blue]Brownfield Cartographer[/bold blue]\nAnalyzing: {repo_path}"))

    try:
        result = run_analysis(repo_path, output_dir=output_dir, full_history=full_history)

        # Summary panel
        surveyor = result.get("surveyor_stats", {})
        hydro = result.get("hydrologist_stats", {})

        summary = f"""[green]Analysis complete in {result['duration_seconds']}s[/green]

[bold]Files:[/bold] {surveyor.get('files_analyzed', 0)} analyzed ({surveyor.get('total_loc', 0)} LOC)
[bold]Languages:[/bold] {', '.join(surveyor.get('languages', [])) or 'N/A'}
[bold]Datasets:[/bold] {hydro.get('total_datasets', 0)}
[bold]Transformations:[/bold] {hydro.get('total_transformations', 0)}
[bold]dbt refs found:[/bold] {hydro.get('dbt_refs_found', 0)}
[bold]Entry points:[/bold] {surveyor.get('entry_points', 0)}
[bold]Hub modules:[/bold] {surveyor.get('hub_modules', 0)}"""

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


if __name__ == "__main__":
    main()
