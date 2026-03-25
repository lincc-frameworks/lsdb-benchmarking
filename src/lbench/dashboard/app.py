import json
import os
import re
from pathlib import Path
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

from flask import send_from_directory, Response
from tuna.main import read, render
from tuna import __file__ as tuna_file

from lbench.cli.env import get_lbench_root_dir
from lbench.dashboard.metrics import BenchmarkCollection

TUNA_WEB_DIR = Path(tuna_file).parent / "web"

# Root directory where benchmark runs are stored
ROOT_DIR = get_lbench_root_dir()


# --- Load and cache runs ---
def load_run_json(run_dir):
    json_file = run_dir / "pytest-benchmark.json"
    if not json_file.exists() or os.stat(json_file).st_size == 0:
        return None
    try:
        with open(json_file, "r") as f:
            data = json.load(f)
        if "benchmarks" not in data:
            return None
        return data
    except json.JSONDecodeError:
        return None


def load_all_runs(root_dir):
    runs = {}
    for p in root_dir.iterdir():
        if p.is_dir():
            data = load_run_json(p)
            if data:
                runs[p.name] = data
    return dict(sorted(runs.items(), reverse=True))


# Global run data (needs to be defined before importing pages)
RUN_DATA = load_all_runs(ROOT_DIR)

# Initialize metrics collection
BENCHMARK_COLLECTION = BenchmarkCollection(RUN_DATA)


# --- Dash app ---
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    use_pages=True,
)
app.title = "lbench Dashboard"


def create_navbar():
    return dbc.NavbarSimple(
        brand="lbench Dashboard",
        brand_href="/",
        color="primary",
        dark=True,
        children=[
            dbc.NavItem(dcc.Link("Runs", href="/", className="nav-link")),
            dbc.NavItem(dcc.Link("Trends", href="/trends", className="nav-link")),
        ],
        sticky="top",
    )


app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        create_navbar(),
        dash.page_container,
    ]
)


def run_dashboard(port=8050):
    app.run(debug=True, port=port)


# Flask server
server = app.server


@server.route("/file/<run_name>/<path:filename>")
def serve_file(run_name, filename):
    run_dir = ROOT_DIR / run_name
    # Make sure to sanitize filename for safety in production
    return send_from_directory(run_dir, filename)


@server.route("/tuna_web/<path:filename>")
def tuna_static(filename):
    return send_from_directory(TUNA_WEB_DIR, filename)


@server.route("/flamegraph/<run_name>/<path:filename>")
def serve_flamegraph(run_name, filename):
    run_dir = ROOT_DIR / run_name
    prof_file = run_dir / filename

    if not prof_file.exists():
        return "File not found", 404

    # Read Tuna data and render HTML
    data = read(str(prof_file))
    html_content = render(data, prof_file.name)

    html_content = re.sub(
        r'src="static/(.*?)"', r'src="/tuna_web/static/\1"', html_content
    )
    html_content = re.sub(
        r'href="static/(.*?)"', r'href="/tuna_web/static/\1"', html_content
    )

    return Response(html_content, mimetype="text/html")
