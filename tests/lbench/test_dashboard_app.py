import cProfile
import json

import pytest
from werkzeug.exceptions import Forbidden

from lbench.dashboard import app as dashboard_app
from lbench.dashboard.layouts import sidebar

REPORT = "dask_performance_report_0f4c1a5e-8f0b-4d55-9a8e-3c2b7e6d1f90.html"


def _write_prof(path):
    profiler = cProfile.Profile()
    profiler.enable()
    sum(range(1000))
    profiler.disable()
    profiler.dump_stats(path)


@pytest.fixture
def root_dir(tmp_path, monkeypatch):
    """Results root with a single run, and a sibling directory that must not be reachable."""
    root = tmp_path / "results"
    run_dir = root / "run1"
    run_dir.mkdir(parents=True)
    (run_dir / "pytest-benchmark.json").write_text(json.dumps({"benchmarks": []}))
    _write_prof(run_dir / "cprofile_test.prof")
    (run_dir / REPORT).write_text("<html>report</html>")

    (tmp_path / REPORT).write_text("<html>secret</html>")
    outside = tmp_path / "outside"
    outside.mkdir()
    _write_prof(outside / "secret.prof")

    monkeypatch.setattr(dashboard_app, "ROOT_DIR", root)
    return root


@pytest.fixture
def client(root_dir):
    return dashboard_app.server.test_client()


def test_server_has_layout(client):
    """The WSGI server is usable as-is, without calling run_dashboard()"""
    response = client.get("/_dash-layout")
    assert response.status_code == 200
    assert "run-data-store" in response.get_data(as_text=True)


def test_serve_file(client):
    response = client.get(f"/file/run1/{REPORT}")
    assert response.status_code == 200
    assert response.mimetype == "text/html"
    assert response.headers["Content-Security-Policy"] == "sandbox allow-scripts"
    assert response.headers["X-Content-Type-Options"] == "nosniff"


@pytest.mark.parametrize("filename", ["pytest-benchmark.json", "cprofile_test.prof"])
def test_serve_file_only_serves_dask_reports(client, root_dir, filename):
    # Other files don't match the route, so Dash's catch-all answers with the dashboard page
    response = client.get(f"/file/run1/{filename}")
    assert response.data != (root_dir / "run1" / filename).read_bytes()


def test_serve_flamegraph(client):
    response = client.get("/flamegraph/run1/cprofile_test.prof")
    assert response.status_code == 200
    assert response.mimetype == "text/html"


@pytest.mark.parametrize("filename", ["missing.prof", "pytest-benchmark.json/x.prof"])
def test_serve_flamegraph_missing(client, filename):
    assert client.get(f"/flamegraph/run1/{filename}").status_code == 404


@pytest.mark.parametrize(
    "url",
    [
        f"/file/../{REPORT}",
        f"/file/%2E%2E/{REPORT}",
        f"/file/run1/../../{REPORT}",
        "/flamegraph/../outside/secret.prof",
        "/flamegraph/%2E%2E/outside/secret.prof",
        "/flamegraph/run1/../../outside/secret.prof",
    ],
)
def test_paths_outside_root_dir_are_rejected(client, url):
    # A route either refuses the path (404), or none matches and Dash serves the dashboard page
    assert b"secret" not in client.get(url).data


def test_rename_is_disabled():
    assert sidebar.create_sidebar({"run1": {}})[{"type": "run-edit-btn", "index": 0}].hidden

    with pytest.raises(Forbidden):
        sidebar.handle_rename([], None, 1, "../outside", "pulled_in", {})
