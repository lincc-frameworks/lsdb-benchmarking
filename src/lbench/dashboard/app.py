import dash
from lbench.dashboard.layout import layout
import dash_bootstrap_components as dbc

import re
from pathlib import Path

from flask import send_from_directory, Response
from tuna.main import render, read
from werkzeug.routing import BaseConverter
from werkzeug.security import safe_join

from tuna import __file__ as tuna_file

from lbench.dashboard.context import ROOT_DIR

TUNA_WEB_DIR = Path(tuna_file).parent / "web"

app = dash.Dash(
    __name__,
    health_endpoint="health",
    external_stylesheets=[
        dbc.themes.FLATLY,
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css",
    ],
)
app.title = "lbench Dashboard"
app.layout = layout


def run_dashboard(port=8050, **kwargs):
    app.run(debug=True, port=port, **kwargs)


# WSGI entry point for production servers, e.g. `gunicorn lbench.dashboard.app:server`
server = app.server


class DaskReportConverter(BaseConverter):
    """URL converter matching the Dask performance reports written by lbench.runner"""

    regex = r"dask_performance_report_[0-9a-f-]+\.html"


server.url_map.converters["dask_report"] = DaskReportConverter


@server.route("/file/<run_name>/<dask_report:filename>")
def serve_dask_reports(run_name, filename):
    response = send_from_directory(ROOT_DIR, f"{run_name}/{filename}")
    # Run the scripts in an isolated origin
    response.headers["Content-Security-Policy"] = "sandbox allow-scripts"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@server.route("/tuna_web/<path:filename>")
def tuna_static(filename):
    return send_from_directory(TUNA_WEB_DIR, filename)


@server.route("/flamegraph/<run_name>/<path:filename>")
def serve_flamegraph(run_name, filename):
    # safe_join returns None for paths that escape ROOT_DIR
    prof_path = safe_join(str(ROOT_DIR), run_name, filename)
    if prof_path is None or not Path(prof_path).is_file():
        return "File not found", 404
    prof_file = Path(prof_path)

    # Read Tuna data and render HTML
    data = read(str(prof_file))
    html_content = render(data, prof_file.name)

    html_content = re.sub(r'src="static/(.*?)"', r'src="/tuna_web/static/\1"', html_content)
    html_content = re.sub(r'href="static/(.*?)"', r'href="/tuna_web/static/\1"', html_content)

    return Response(html_content, mimetype="text/html")
