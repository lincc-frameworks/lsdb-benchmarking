import functools
import json
import os
import shutil

from lbench.cli.env import get_lbench_root_dir
from lbench.dashboard.metrics import MetricRegistry
from lbench.dashboard.metrics.benchmark_collection import BenchmarkCollection
from lbench.dashboard.metrics.groups import stats_group, execution_group, dask_group, profiling_group

# Registry for available metrics — constant, built once at startup
registry = MetricRegistry()
for group in [stats_group, execution_group, dask_group, profiling_group]:
    registry.register_group(group)

# Root directory where benchmark runs are stored — constant
ROOT_DIR = get_lbench_root_dir()


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
    return dict(sorted(runs.items(), key=lambda kv: kv[1].get("datetime", ""), reverse=True))


# Runs are kept on the server between callbacks. The browser only gets their names, so callbacks don't
# have to send every run's data back to the server with each request.
@functools.lru_cache(maxsize=1)
def _load_runs(root_dir, _files):
    return load_all_runs(root_dir)


def get_runs():
    """All runs in ROOT_DIR, newest first, reread from disk only when the results files change.

    Every request gets the same dicts, so treat them as read-only.
    """
    files = []
    for json_file in sorted(ROOT_DIR.glob("*/pytest-benchmark.json")):
        stat = json_file.stat()
        files.append((json_file, stat.st_mtime_ns, stat.st_size))
    return _load_runs(ROOT_DIR, tuple(files))


def get_run_data(run_ids):
    """Data for the given runs, in the given order, skipping runs that no longer exist."""
    if not run_ids:
        return {}
    runs = get_runs()
    return {run_id: runs[run_id] for run_id in run_ids if run_id in runs}


def get_collection(run_data: dict) -> BenchmarkCollection:
    """Build a BenchmarkCollection from raw run data (e.g. from get_run_data)."""
    return BenchmarkCollection(run_data or {}, registry)


def rename_run(old_name, new_name):
    """Rename a benchmark run folder.

    Returns:
        tuple: (success: bool, message: str, new_run_data: dict)
    """
    if not old_name or not new_name:
        return False, "Names cannot be empty", None

    if old_name == new_name:
        return False, "New name is the same as old name", None

    old_path = ROOT_DIR / old_name
    new_path = ROOT_DIR / new_name

    if not old_path.exists():
        return False, f"Run '{old_name}' not found", None

    if new_path.exists():
        return False, f"Run '{new_name}' already exists", None

    try:
        shutil.move(str(old_path), str(new_path))
        return True, f"Successfully renamed '{old_name}' to '{new_name}'", load_all_runs(ROOT_DIR)
    except Exception as e:
        return False, f"Error renaming run: {str(e)}", None
