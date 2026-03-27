import json
import os
import shutil

from lbench.cli.env import get_lbench_root_dir
from lbench.dashboard.metrics import MetricRegistry
from lbench.dashboard.metrics.benchmark_collection import BenchmarkCollection
from lbench.dashboard.metrics.groups import stats_group, execution_group, computed_group, dask_group

"""Registry for available metrics"""
registry = MetricRegistry()
for group in [stats_group, execution_group, computed_group, dask_group]:
    registry.register_group(group)

"""Load information about runs"""
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

def rename_run(old_name, new_name):
    """Rename a benchmark run folder.

    Args:
        old_name: Current folder name
        new_name: New folder name

    Returns:
        tuple: (success: bool, message: str, new_run_data: dict, new_collection: BenchmarkCollection)
    """
    global RUN_DATA, BENCHMARK_COLLECTION

    # Validate names
    if not old_name or not new_name:
        return False, "Names cannot be empty", RUN_DATA, BENCHMARK_COLLECTION

    if old_name == new_name:
        return False, "New name is the same as old name", RUN_DATA, BENCHMARK_COLLECTION

    old_path = ROOT_DIR / old_name
    new_path = ROOT_DIR / new_name

    # Check if old path exists
    if not old_path.exists():
        return False, f"Run '{old_name}' not found", RUN_DATA, BENCHMARK_COLLECTION

    # Check if new path already exists
    if new_path.exists():
        return False, f"Run '{new_name}' already exists", RUN_DATA, BENCHMARK_COLLECTION

    try:
        # Rename the folder
        shutil.move(str(old_path), str(new_path))

        # Reload all run data
        new_run_data = load_all_runs(ROOT_DIR)
        new_collection = BenchmarkCollection(new_run_data, registry)

        # Update globals
        RUN_DATA = new_run_data
        BENCHMARK_COLLECTION = new_collection

        return True, f"Successfully renamed '{old_name}' to '{new_name}'", new_run_data, new_collection
    except Exception as e:
        return False, f"Error renaming run: {str(e)}", RUN_DATA, BENCHMARK_COLLECTION

# Root directory where benchmark runs are stored
ROOT_DIR = get_lbench_root_dir()

# Global run data (needs to be defined before importing pages)
RUN_DATA = load_all_runs(ROOT_DIR)

# Initialize metrics collection
BENCHMARK_COLLECTION = BenchmarkCollection(RUN_DATA, registry)