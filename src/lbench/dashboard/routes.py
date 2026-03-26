import re
from pathlib import Path

from flask import send_from_directory, Response
from tuna.main import render, read

from lbench.dashboard.app import app
from tuna import __file__ as tuna_file

from lbench.dashboard.context import ROOT_DIR

TUNA_WEB_DIR = Path(tuna_file).parent / "web"

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