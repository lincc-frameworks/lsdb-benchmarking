import json
import os
from pathlib import Path
import dash
from dash import html, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd

from flask import send_from_directory

from lbench.cli.env import ROOT_DIR_ENV_VAR

# Root directory where benchmark runs are stored
ROOT_DIR = Path(os.environ.get(ROOT_DIR_ENV_VAR))

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

RUN_DATA = load_all_runs(ROOT_DIR)

# --- Helper to create a table for a benchmark ---
def benchmark_to_table(bm, run_name):
    stats = bm.get("stats", {})
    df = pd.DataFrame({
        "min": [stats.get("min")],
        "max": [stats.get("max")],
        "mean": [stats.get("mean")],
        "stddev": [stats.get("stddev")],
        "total_time": [stats.get("total")],
        "rounds": [stats.get("rounds")],
        "iterations": [stats.get("iterations")]
    })

    dask_table = None
    total_time_table = None
    dask_report_button = None

    if "extra_info" in bm and bm["extra_info"]:
        extra = bm["extra_info"]

        n_tasks = extra.get("n_tasks")
        keys = [k[0] for k in extra.get("keys", [])]

        times = [
            k["stop"] - k["start"]
            for s in extra.get("startstops", [])
            for k in s
        ]

        total_dask_time = sum(times)

        total_time_by_key = {}
        for k, t in zip(keys, times):
            total_time_by_key[k] = total_time_by_key.get(k, 0) + t

        dask_table = pd.DataFrame({
            "n_tasks": [n_tasks],
            "total_dask_time": [total_dask_time],
        })

        sorted_key_times = sorted(
            total_time_by_key.items(),
            key=lambda x: x[1],
            reverse=True
        )

        total_time_table = pd.DataFrame({
            "task_key": [k for k, _ in sorted_key_times],
            "total_time": [t for _, t in sorted_key_times],
        })

        # --- Dask report button ---
        report_path = extra.get("performance_report")
        if report_path:
            report_name = Path(report_path).name
            dask_report_button = html.A(
                "Open Dask Performance Report",
                href=f"/dask_report/{run_name}/{report_name}",
                target="_blank",
                className="btn btn-outline-primary mt-2",
                role="button",
            )

    card_children = [
        dbc.CardHeader(bm["fullname"]),
        dbc.CardBody(
            dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True)
        ),
    ]

    if dask_table is not None:
        card_children.append(
            dbc.CardBody([
                html.H5("Dask Metrics", className="card-title"),
                dbc.Table.from_dataframe(dask_table, striped=True, bordered=True, hover=True),
            ])
        )

        card_children.append(
            dbc.CardBody([
                html.H5("Dask Task Times", className="card-title"),
                dbc.Table.from_dataframe(total_time_table, striped=True, bordered=True, hover=True),
            ])
        )

        if dask_report_button is not None:
            card_children.append(
                dbc.CardBody(
                    dask_report_button,
                    className="text-end"
                )
            )

    return dbc.Card(card_children, className="mb-3")


def benchmarks_to_tables(run_name, run_data):
    return [
        benchmark_to_table(bm, run_name)
        for bm in run_data.get("benchmarks", [])
    ]


# --- Dash app ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "lbench Dashboard"

# Layout with sidebar as a clickable list
def create_sidebar(active_run=None):
    return dbc.ListGroup(
        [
            dbc.ListGroupItem(
                r,
                id={"type": "run-item", "index": i},
                action=True,
                active=(r == active_run)
            )
            for i, r in enumerate(RUN_DATA.keys())
        ],
        id="run-list"
    )

app.layout = dbc.Container([
    dbc.Row([
        # Sidebar
        dbc.Col([
            html.H4("Benchmark Runs"),
            html.Div(id="sidebar-container", children=create_sidebar())
        ], width=3, style={"borderRight": "1px solid #ccc", "height": "100vh", "overflowY": "auto"}),

        # Main content
        dbc.Col([
            html.Div(id="benchmark-tables-container")
        ], width=9, style={"padding": "20px", "overflowY": "auto", "height": "100vh"})
    ])
], fluid=True)

# --- Callback to update benchmark tables and highlight selected run ---
@app.callback(
    Output("benchmark-tables-container", "children"),
    Output("sidebar-container", "children"),
    Input({"type": "run-item", "index": dash.ALL}, "n_clicks"),
)
def update_benchmarks_and_sidebar(n_clicks_list):
    if not any(n_clicks_list):
        # No run clicked yet
        return html.Div("Select a run from the sidebar"), create_sidebar()

    # Determine which item was clicked
    clicked_idx = n_clicks_list.index(max(n_clicks_list))
    run_name = list(RUN_DATA.keys())[clicked_idx]
    run_data = RUN_DATA[run_name]

    # Update sidebar to highlight selected run
    sidebar = create_sidebar(active_run=run_name)
    tables = benchmarks_to_tables(run_name, run_data)

    return tables, sidebar

def run_dashboard(port=8050):
    app.run(debug=True, port=port)

# Assuming 'app' is your dash.Dash instance
server = app.server  # Flask server

@server.route("/dask_report/<run_name>/<path:filename>")
def serve_dask_report(run_name, filename):
    run_dir = ROOT_DIR / run_name
    # Make sure to sanitize filename for safety in production
    return send_from_directory(run_dir, filename)

