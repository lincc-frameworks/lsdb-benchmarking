import typer


app = typer.Typer(help="lbench CLI — run benchmarks and dashboards")

@app.command()
def dash(port: int = 8050):
    """Run the lbench dashboard."""
    from lbench.dashboard.app import run_dashboard
    run_dashboard(port=port)
