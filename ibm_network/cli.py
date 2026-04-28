"""CLI entry point: `ibm-net convert <graph_name> --out <dir>`."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from ibm_network.codegen import synthesize
from ibm_network.config.settings import get_settings
from ibm_network.ir.neo4j_loader import Neo4jGraphLoader


@click.group()
def cli() -> None:
    """IBM Network — Ab Initio → PySpark convertor."""


@cli.command("convert")
@click.argument("graph_name", type=str)
@click.option("--out", "out_dir", type=click.Path(path_type=Path), default=None)
@click.option(
    "--no-llm-fallback",
    is_flag=True,
    default=False,
    help="Disable LLM fallback for unknown components — fail fast instead.",
)
@click.option(
    "--polish/--no-polish",
    default=None,
    help="Run an LLM polish pass on the generated script (overrides settings).",
)
def convert_cmd(
    graph_name: str,
    out_dir: Path | None,
    no_llm_fallback: bool,
    polish: bool | None,
) -> None:
    """Convert a graph stored in Neo4j into a PySpark `.py` file."""
    settings = get_settings()
    out_path = out_dir or Path(settings.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    with Neo4jGraphLoader(settings) as loader:
        graph = loader.load_graph(graph_name)

    if not graph.components:
        click.echo(f"error: graph {graph_name!r} has no components in Neo4j", err=True)
        sys.exit(2)

    enable_polish = settings.enable_llm_polish if polish is None else polish

    result = synthesize(
        graph,
        enable_llm_fallback=not no_llm_fallback,
        enable_llm_polish=enable_polish,
    )

    target = out_path / f"{graph_name}.py"
    target.write_text(result.code)

    click.echo(f"wrote {target}")
    if result.notes:
        click.echo("notes:")
        for n in result.notes:
            click.echo(f"  - {n}")


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
