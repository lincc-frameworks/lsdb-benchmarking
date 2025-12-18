import os
import uuid
from pathlib import Path
import pytest
from pytest import fixture
from distributed import Client, get_task_stream, performance_report

from lbench.cli.env import CURRENT_DIR_ENV_VAR


@fixture
def single_thread_dask_client():
    with Client(n_workers=1, threads_per_worker=1) as client:
        yield client


@fixture(scope="session")
def benchmark_results_dir() -> Path:
    path = os.environ.get(CURRENT_DIR_ENV_VAR)
    if not path:
        pytest.fail(f"{CURRENT_DIR_ENV_VAR} not set — run via `lbench`")
    return Path(path)

@fixture
def dask_benchmark(benchmark, single_thread_dask_client: Client, benchmark_results_dir: Path):
    def dask_benchmark_func(func, *args, **kwargs):
        benchmark(func, *args, **kwargs)
        extra_metrics = {}
        with get_task_stream(single_thread_dask_client) as ts:
            func(*args, **kwargs)
        # ts is now a TaskStream object
        extra_metrics["n_tasks"] = len(ts.data)  # number of tasks executed
        extra_metrics["keys"] = [t["key"] for t in ts.data]
        extra_metrics["startstops"] = [t["startstops"] for t in ts.data]

        report_uuid = str(uuid.uuid4())
        performance_report_path = benchmark_results_dir / f"dask_performance_report_{report_uuid}.html"

        with performance_report(filename=performance_report_path):
            func(*args, **kwargs)

        extra_metrics["performance_report"] = str(performance_report_path)

        benchmark.extra_info.update(extra_metrics)

    return dask_benchmark_func
