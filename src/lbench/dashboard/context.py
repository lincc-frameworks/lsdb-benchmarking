import json
import os

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

# Root directory where benchmark runs are stored
ROOT_DIR = get_lbench_root_dir()

# Global run data (needs to be defined before importing pages)
RUN_DATA = load_all_runs(ROOT_DIR)

# Initialize metrics collection
BENCHMARK_COLLECTION = BenchmarkCollection(RUN_DATA, registry)