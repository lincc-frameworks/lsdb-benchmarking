import json
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

import dash
from dash import html, Input, Output, dcc
import dash_bootstrap_components as dbc
import pandas as pd

from flask import send_from_directory, Response
from tuna.main import read, render
from tuna import __file__ as tuna_file

from lbench.cli.env import get_lbench_root_dir

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


# --- Helper to create a table for a benchmark ---
def format_duration(seconds, digits=3):
    """
    Format a duration in seconds using the most appropriate unit.
    Returns (value_str, unit).
    """
    if seconds is None:
        return "-", ""

    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return str(seconds), ""

    if seconds >= 1:
        return f"{seconds:.{digits}f}", "s"
    elif seconds >= 1e-3:
        return f"{seconds * 1e3:.{digits}f}", "ms"
    elif seconds >= 1e-6:
        return f"{seconds * 1e6:.{digits}f}", "µs"
    else:
        return f"{seconds * 1e9:.{digits}f}", "ns"


def benchmark_to_table(bm, run_name):
    stats = bm.get("stats", {})
    # Round values and add units to headings
    min_v, min_u = format_duration(stats.get("min"))
    max_v, max_u = format_duration(stats.get("max"))
    mean_v, mean_u = format_duration(stats.get("mean"))
    std_v, std_u = format_duration(stats.get("stddev"))

    df = pd.DataFrame(
        {
            f"min ({min_u})": [min_v],
            f"max ({max_u})": [max_v],
            f"mean ({mean_u})": [mean_v],
            f"stddev ({std_u})": [std_v],
            "rounds": [stats.get("rounds")],
            "iterations": [stats.get("iterations")],
        }
    )

    dask_table = None
    total_time_table = None
    dask_report_button = None
    flamegraph_button = None

    if "extra_info" in bm and bm["extra_info"]:
        extra = bm["extra_info"]

        if "dask" in extra:
            dask_stats = extra["dask"]
            n_tasks = dask_stats.get("n_tasks")
            keys = [k[0] for k in dask_stats.get("keys", [])]

            times = [
                sum([k["stop"] - k["start"] for k in s])
                for s in dask_stats.get("startstops", [])
            ]

            total_dask_time = sum(times)

            total_time_by_key = {}
            for k, t in zip(keys, times):
                total_time_by_key[k] = total_time_by_key.get(k, 0) + t

            total_time_fmt, total_time_u = format_duration(total_dask_time)

            dask_table = pd.DataFrame({
                "n_tasks": [n_tasks],
                f"total dask time ({total_time_u})": [total_time_fmt],
            })

            sorted_key_times = sorted(
                total_time_by_key.items(),
                key=lambda x: x[1],
                reverse=True
            )

            formatted_times = [format_duration(t) for _, t in sorted_key_times]

            total_time_table = pd.DataFrame({
                "task_key": [k for k, _ in sorted_key_times],
                "total time": [f"{v} {u}" for v, u in formatted_times],
            })

            # --- Dask report button ---
            report_path = dask_stats.get("performance_report")
            if report_path:
                report_name = Path(report_path).name
                dask_report_button = html.A(
                    "Open Dask Performance Report",
                    href=f"/file/{run_name}/{report_name}",
                    target="_blank",
                    className="btn btn-outline-primary mt-2",
                    role="button",
                )

        if "cprofile_path" in extra:
            profile_path = extra["cprofile_path"]
            profile_name = Path(profile_path).name
            flamegraph_button = html.A(
                "Open Flamegraph",
                href=f"/flamegraph/{run_name}/{profile_name}",
                target="_blank",
                className="btn btn-outline-secondary mt-2",
                role="button",
            )

    card_children = [
        dbc.CardHeader(bm["fullname"]),
        dbc.CardBody(
            dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True)
        ),
    ]

    buttons = []

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
        buttons.append(dask_report_button)
    if flamegraph_button is not None:
        buttons.append(flamegraph_button)

    if len(buttons) > 0:
        card_children.append(
            dbc.CardBody(
                html.Div(
                    buttons,
                    className="btn-group mt-2",
                ),
                className="text-end",
            )
        )

    return dbc.Card(card_children, className="mb-3")


def benchmarks_to_tables(run_name, run_data):
    return [
        benchmark_to_table(bm, run_name)
        for bm in run_data.get("benchmarks", [])
    ]


# --- Dash app ---
# Create app with relative URL paths to work with Jupyter proxy
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY]
)
app.title = "lbench Dashboard"


# Layout with sidebar as a clickable list
def create_sidebar(run_data, active_run=None):
    return dbc.ListGroup(
        [
            dbc.ListGroupItem(
                r,
                id={"type": "run-item", "index": i},
                action=True,
                active=(r == active_run)
            )
            for i, r in enumerate(run_data.keys())
        ],
        id="run-list"
    )


def initial_layout():
    run_data = load_all_runs(ROOT_DIR)
    return dbc.Container([
        dcc.Store(id="run-data-store", data=run_data),
        dbc.Row([
            # Sidebar
            dbc.Col([
                html.H4("Benchmark Runs"),
                html.Div(id="sidebar-container", children=create_sidebar(run_data))
            ], width=3, style={"borderRight": "1px solid #ccc", "height": "100vh", "overflowY": "auto",
                               "paddingTop": "20px"}),

            # Main content
            dbc.Col([
                html.Div(id="benchmark-tables-container")
            ], width=9, style={"padding": "20px", "overflowY": "auto", "height": "100vh"})
        ])
    ], fluid=True)


app.layout = initial_layout  # assign the function, not the result, so it's rerun on each page reload


# --- Callback to update benchmark tables and highlight selected run ---
@app.callback(
    Output("benchmark-tables-container", "children"),
    Output("sidebar-container", "children"),
    Input({"type": "run-item", "index": dash.ALL}, "n_clicks"),
    Input("run-data-store", "data"),
)
def update_benchmarks_and_sidebar(n_clicks_list, run_data):
    ctx = dash.ctx
    triggered = ctx.triggered_id  # This is the dict {"type": "run-item", "index": i} or None
    if not run_data or not isinstance(run_data, dict):
        return html.Div("No run data found"), create_sidebar({})
    if not triggered or triggered == "run-data-store":
        # No click yet
        return html.Div("Select a run from the sidebar"), create_sidebar(run_data)
    # Use triggered index directly
    if isinstance(triggered, dict) and "index" in triggered:
        idx = triggered["index"]
        run_name = list(run_data.keys())[idx]
        sidebar = create_sidebar(run_data, active_run=run_name)
        tables = benchmarks_to_tables(run_name, run_data[run_name])
        return tables, sidebar
    return html.Div("Select a run from the sidebar"), create_sidebar(run_data)


def _create_app_with_prefix(prefix='/'):
    """Create a new Dash app instance with the specified URL prefix."""
    new_app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.FLATLY],
        requests_pathname_prefix=prefix,
        routes_pathname_prefix=prefix
    )
    new_app.title = "lbench Dashboard"

    # Copy layout and callbacks from the original app
    new_app.layout = app.layout
    new_app.callback_map = app.callback_map
    new_app._callback_list = app._callback_list

    return new_app


def run_dashboard(port=8050, jupyter_mode='external', height=800, jupyter_server_url=None):
    """
    Run the dashboard.

    Parameters
    ----------
    port : int
        Port to run the server on (default: 8050)
    jupyter_mode : str
        Display mode. Options:
        - 'external': Opens in a new browser tab (default, recommended for JupyterHub)
        - 'inline': Embeds directly in Jupyter notebook (may fail with strict CSP)
        - 'jupyterlab': Opens in JupyterLab tab
    height : int
        Height of inline display in pixels (default: 800)
    jupyter_server_url : str, optional
        Base URL for Jupyter server (e.g., 'https://usdf-rsp.slac.stanford.edu/nb/user/smcgui').
        If None, will attempt to auto-detect from environment variables.

    Notes
    -----
    For JupyterHub with strict Content Security Policy (CSP), 'external' mode is
    recommended as it opens the dashboard in a new tab instead of embedding inline.
    """
    # Auto-detect Jupyter server URL
    if jupyter_server_url is None:
        try:
            import os
            jupyter_server = os.environ.get('JUPYTERHUB_SERVICE_PREFIX')
            if jupyter_server:
                # Try to get the full base URL
                jupyter_server_url = os.environ.get('JUPYTERHUB_BASE_URL', '')
                if not jupyter_server_url.startswith('http'):
                    # Can't auto-detect full URL, skip
                    jupyter_server_url = None
        except Exception:
            pass

    # For Jupyter environments, pass the server URL directly
    if jupyter_server_url:
        jupyter_server_url = jupyter_server_url.rstrip('/')
        url = f'{jupyter_server_url}/proxy/{port}/'
        url_prefix = urlsplit(url).path

        # Display the URL explicitly for external mode
        if jupyter_mode == 'external':
            from IPython.display import display, HTML
            print(f"Dashboard starting on port {port}...")
            print(f"Access at: {url}")
            display(
                HTML(f'<a href="{url}" target="_blank">Click here to open the dashboard in a new tab</a>'))

        # Don't set any prefix - the Jupyter proxy handles routing transparently
        # Just run the app normally on localhost

        # Get prefix by url path and proxy

        # app = _create_app_with_prefix(prefix=url_prefix)

        app = _create_app_with_prefix(prefix=url_prefix)

        app.run(
            debug=False,
            host='127.0.0.1',
            jupyter_mode=jupyter_mode,
            port=port,
            use_reloader=False,
            dev_tools_props_check=False
        )
    else:
        # For command line or local environments
        app.run(debug=True, port=port, jupyter_mode=jupyter_mode, height=height)


# Assuming 'app' is your dash.Dash instance
server = app.server  # Flask server


# Middleware to handle CSP for Jupyter iframe embedding
@server.after_request
def add_security_headers(response):
    # Allow embedding in Jupyter notebooks
    # Remove restrictive CSP that blocks iframe embedding
    if 'Content-Security-Policy' in response.headers:
        csp = response.headers['Content-Security-Policy']
        # Allow frame-ancestors for Jupyter environments
        if 'frame-ancestors' not in csp:
            response.headers['Content-Security-Policy'] = csp + "; frame-ancestors *"
    return response


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

    html_content = re.sub(r'src="static/(.*?)"', r'src="/tuna_web/static/\1"', html_content)
    html_content = re.sub(r'href="static/(.*?)"', r'href="/tuna_web/static/\1"', html_content)

    return Response(html_content, mimetype="text/html")
