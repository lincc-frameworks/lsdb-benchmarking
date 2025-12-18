import typer
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

from lbench.cli.env import CURRENT_DIR_ENV_VAR, get_lbench_root_dir

app = typer.Typer(help="lbench CLI — run benchmarks and dashboards")

@app.command()
def run(tests: list[str] = typer.Argument(None)):
    """Run benchmarks"""
    root = get_lbench_root_dir()

    root = Path(root).expanduser().resolve()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = root / run_id
    run_dir.mkdir(parents=True)

    env = dict(os.environ)
    env[CURRENT_DIR_ENV_VAR] = str(run_dir)

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "--benchmark-only",
        f"--benchmark-json={run_dir / 'pytest-benchmark.json'}",
        *tests,  # forward arguments to pytest
    ]
    print(f"[lbench] running benchmarks in {run_dir}")
    subprocess.run(cmd, env=env, check=True)


@app.command()
def dash(port: int = 8050):
    """Run the lbench dashboard."""
    from lbench.dashboard.app import run_dashboard
    run_dashboard(port=port)
