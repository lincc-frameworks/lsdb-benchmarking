from pathlib import Path
import sys

from dask.sizeof import sizeof

import pytest
from pytest import fixture
from distributed import Client

from lbench.runner import run_cprofile, run_dask_benchmark, run_memray


@fixture
def single_thread_dask_client():
    with Client(n_workers=1, threads_per_worker=1) as client:
        yield client


@fixture(scope="session")
def benchmark_results_dir(pytestconfig) -> Path:
    if not hasattr(pytestconfig, "lbench_run_dir"):
        pytest.fail("lbench_run_dir not set — run pytest with `--lbench` option")
    path = pytestconfig.lbench_run_dir
    return Path(path)


@fixture
def lbench(benchmark_results_dir: Path, benchmark, request):
    def lbench_benchmark_func(func, *args, **kwargs):
        benchmark(func, *args, **kwargs)

        track_memory = request.node.get_closest_marker("lbench_memory") is not None

        if track_memory:
            peak_memory = run_memray(func, benchmark_results_dir, *args, **kwargs)
            benchmark.extra_info["peak_memory_bytes"] = peak_memory

        cprof_path = run_cprofile(func, benchmark_results_dir, *args, **kwargs)
        benchmark.extra_info["cprofile_path"] = cprof_path

    return lbench_benchmark_func


@fixture
def lbench_dask(lbench, benchmark, single_thread_dask_client: Client, benchmark_results_dir: Path):
    def dask_benchmark_func(func, *args, **kwargs):
        lbench(func, *args, **kwargs)
        benchmark.extra_info["dask"] = run_dask_benchmark(
            func, benchmark_results_dir, single_thread_dask_client, *args, **kwargs
        )

    return dask_benchmark_func


@fixture
def lbench_dask_collection(lbench_dask, benchmark):
    def collection_benchmark_func(collection):
        run_func = lambda: collection.compute()
        graph = collection.dask

        graph_len = len(graph)

        lbench_dask(run_func)
        benchmark.extra_info["dask"]["dask_graph_len"] = graph_len

    return collection_benchmark_func
