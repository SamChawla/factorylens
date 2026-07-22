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
import time

import typer
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from factorylens import agent
from factorylens.config import get_settings
from factorylens.exceptions import LLMProviderError
from factorylens.generator import DEMO_SCENARIO, degrading_scenario, generate
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
    runs: int = typer.Option(
        1, "--runs", "-n", min=1,
        help="Number of pipeline runs. >1 seeds a trend with line_3 degrading.",
    ),
    interval: float = typer.Option(
        30.0, "--interval", "-i", min=0.0,
        help="Seconds between runs. Spread runs out so trend panels show a curve.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-stage logs."),
) -> None:
    """Generate the demo dataset, run the pipeline, and export the spans.

    A single run uses the fixed demo scenario. Multiple runs walk the degrading
    scenario, so the SigNoz trend panels show data quality falling on line_3 and
    its OEE falling with it.
    """
    _init_logging(verbose)
    settings = get_settings()
    telemetry = setup_telemetry(settings)
    first: dict[str, OEEResult] = {}
    results: dict[str, OEEResult] = {}
    try:
        for i in range(runs):
            scenario = DEMO_SCENARIO if runs == 1 else degrading_scenario(i, runs)
            results = run_pipeline(generate(scenario), telemetry)
            telemetry.force_flush()
            if i == 0:
                first = results
            if runs > 1:
                console.print(
                    f"run {i + 1}/{runs}  "
                    + "  ".join(
                        f"{lid} OEE [{_oee_color(results[lid].oee)}]{results[lid].oee:.2f}"
                        f"[/{_oee_color(results[lid].oee)}]"
                        for lid in sorted(results)
                    )
                )
            if interval > 0 and i < runs - 1:
                time.sleep(interval)
    finally:
        telemetry.shutdown()

    console.print(_oee_table(results))
    if runs > 1:
        drift = results["line_3"].oee - first["line_3"].oee
        console.print(
            f"line_3 OEE moved [bold]{first['line_3'].oee:.2f} -> "
            f"{results['line_3'].oee:.2f}[/bold] ({drift:+.2f}) across {runs} runs."
        )
    if telemetry.exporting_to_signoz:
        console.print(
            f"[green]Exported {runs * len(results) * 5} spans to SigNoz[/green] "
            f"(service '{settings.otel_service_name}' at {settings.signoz_otlp_endpoint})."
        )
    else:
        console.print(
            "[yellow]SigNoz not configured[/yellow] - spans went to the console. "
            "Run [bold]factorylens check[/bold] after setting SIGNOZ_INGESTION_KEY in .env."
        )


@app.command()
def ask(
    question: str = typer.Argument(..., help="e.g. \"why is line_3's OEE low?\""),
    runs: int = typer.Option(
        1, "--runs", "-n", min=1,
        help="Pipeline runs to gather before answering. >1 lets the agent see a trend.",
    ),
    show_context: bool = typer.Option(
        False, "--show-context", help="Print the telemetry handed to the model."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-stage logs."),
) -> None:
    """Ask a question about the pipeline; answered from its OTel spans."""
    _init_logging(verbose)
    settings = get_settings()
    capture = InMemorySpanExporter()
    telemetry = setup_telemetry(settings, capture=capture)
    try:
        source = agent.LocalPipelineSource(telemetry=telemetry, capture=capture)
        with console.status("Running pipeline and collecting telemetry..."):
            snapshot = source.snapshot(runs=runs)
        if show_context:
            console.print(
                Panel(agent.format_context(snapshot), title="Telemetry given to the model",
                      border_style="blue")
            )
        with console.status("Asking..."):
            try:
                reply = agent.answer(
                    question, snapshot, settings=settings, tracer=telemetry.tracer()
                )
            except LLMProviderError as e:
                console.print(
                    Panel(str(e), title="[red]No answer[/red]", border_style="red")
                )
                raise typer.Exit(code=1) from e
        telemetry.force_flush()
    finally:
        telemetry.shutdown()

    console.print(Panel(reply, title=f"[green]{question}[/green]", border_style="green"))


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
