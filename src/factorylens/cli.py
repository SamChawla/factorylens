"""FactoryLens command-line interface (Rich/Typer).

Commands so far:
  - ``run``   : generate the demo dataset, run the pipeline, print OEE per line,
                and export spans (to SigNoz if configured, else the console).
  - ``check`` : send a single hello-world span to SigNoz and flush it — the
                Day-1 auth test. Prove one span lands before trusting the rest.

The Q&A ``ask`` command lands in piece 4. User-facing output goes through Rich;
diagnostic logs go through structlog to stderr (quiet by default, ``--verbose``
to see per-stage logs).
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from factorylens.config import get_settings
from factorylens.generator import DEMO_SCENARIO, generate
from factorylens.logging import configure_logging
from factorylens.oee import OEEResult
from factorylens.pipeline import run_pipeline
from factorylens.telemetry import setup_telemetry

app = typer.Typer(
    help="FactoryLens — manufacturing pipeline health & data-quality observability for SigNoz.",
    add_completion=False,
    no_args_is_help=True,
)
console = Console()


def _init_logging(verbose: bool) -> None:
    configure_logging(level=logging.INFO if verbose else logging.WARNING)


def _oee_color(oee: float) -> str:
    if oee >= 0.75:
        return "green"
    if oee >= 0.65:
        return "yellow"
    return "red"


def _oee_table(results: dict[str, OEEResult]) -> Table:
    table = Table(title="OEE per line", title_style="bold")
    table.add_column("Line")
    for col in ("Availability", "Performance", "Quality", "OEE"):
        table.add_column(col, justify="right")
    for line_id in sorted(results):
        r = results[line_id]
        color = _oee_color(r.oee)
        table.add_row(
            line_id,
            f"{r.availability:.2f}",
            f"{r.performance:.2f}",
            f"{r.quality:.2f}",
            f"[{color}]{r.oee:.2f}[/{color}]",
        )
    return table


@app.command()
def run(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-stage logs."),
) -> None:
    """Generate the demo dataset, run the pipeline, and export the spans."""
    _init_logging(verbose)
    settings = get_settings()
    telemetry = setup_telemetry(settings)
    try:
        raw = generate(DEMO_SCENARIO)
        results = run_pipeline(raw, telemetry)
        telemetry.force_flush()
    finally:
        telemetry.shutdown()

    console.print(_oee_table(results))
    if telemetry.exporting_to_signoz:
        console.print(
            f"[green]Exported {len(results) * 5} spans to SigNoz[/green] "
            f"(service '{settings.otel_service_name}' at {settings.signoz_otlp_endpoint})."
        )
    else:
        console.print(
            "[yellow]SigNoz not configured[/yellow] - spans went to the console. "
            "Run [bold]factorylens check[/bold] after setting SIGNOZ_INGESTION_KEY in .env."
        )


@app.command()
def check(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show diagnostic logs."),
) -> None:
    """Send one hello-world span to SigNoz — the Day-1 auth test."""
    _init_logging(verbose)
    settings = get_settings()

    if not settings.signoz_configured:
        console.print(
            Panel(
                "SIGNOZ_INGESTION_KEY is not set, so no span can reach SigNoz.\n"
                "Copy .env.example to .env and fill in the ingestion key and\n"
                "region endpoint, then run this again.",
                title="[yellow]SigNoz not configured[/yellow]",
                border_style="yellow",
            )
        )
        raise typer.Exit(code=1)

    telemetry = setup_telemetry(settings)
    try:
        tracer = telemetry.tracer()
        with tracer.start_as_current_span("factorylens_healthcheck") as span:
            span.set_attribute("check", "hello_signoz")
            span.set_attribute("service.name", settings.otel_service_name)
        flushed = telemetry.force_flush()
    finally:
        telemetry.shutdown()

    console.print(
        Panel(
            f"Sent [bold]factorylens_healthcheck[/bold] span to\n"
            f"{settings.signoz_otlp_endpoint}\n"
            f"flush acknowledged: {flushed}\n\n"
            f"Open SigNoz -> Traces and filter service = "
            f"[bold]{settings.otel_service_name}[/bold] to confirm it landed.",
            title="[green]Test span sent[/green]",
            border_style="green",
        )
    )


if __name__ == "__main__":
    app()
