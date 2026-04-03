import cProfile
import uuid
from pathlib import Path
import sys

import memray
import pytest
from pytest import fixture
from distributed import Client, get_task_stream, performance_report
from distributed.diagnostics.memory_sampler import MemorySampler


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
        benchmark(func, *args, *kwargs)

        cprof_uuid = str(uuid.uuid4())
        cprof_output_path = benchmark_results_dir / f"cprofile_{cprof_uuid}.prof"

        track_memory = request.node.get_closest_marker("lbench_memory") is not None

        if track_memory:
            memray_output = benchmark_results_dir / f"memray_{uuid.uuid4()}.bin"
            with memray.Tracker(memray_output):
                with cProfile.Profile() as pr:
                    func(*args, **kwargs)
            reader = memray.FileReader(memray_output)
            benchmark.extra_info["peak_memory_bytes"] = reader.metadata.peak_memory
        else:
            with cProfile.Profile() as pr:
                func(*args, **kwargs)

        pr.dump_stats(cprof_output_path)
        benchmark.extra_info["cprofile_path"] = str(cprof_output_path)

    return lbench_benchmark_func


@fixture
def lbench_dask(lbench, benchmark, single_thread_dask_client: Client, benchmark_results_dir: Path):
    def dask_benchmark_func(func, *args, **kwargs):
        lbench(func, *args, **kwargs)
        extra_metrics = {}

        report_uuid = str(uuid.uuid4())
        performance_report_path = benchmark_results_dir / f"dask_performance_report_{report_uuid}.html"

        ms = MemorySampler()
        with performance_report(filename=performance_report_path):
            with ms.sample("benchmark"):
                with get_task_stream(single_thread_dask_client) as ts:
                    func(*args, **kwargs)

        memory_df = ms.to_pandas()
        if not memory_df.empty and "benchmark" in memory_df.columns:
            extra_metrics["peak_memory_bytes"] = int(memory_df["benchmark"].max())
        extra_metrics["n_tasks"] = len(ts.data)  # number of tasks executed
        extra_metrics["keys"] = [t["key"] for t in ts.data]
        extra_metrics["startstops"] = [t["startstops"] for t in ts.data]

        extra_metrics["performance_report"] = str(performance_report_path)

        benchmark.extra_info["dask"] = extra_metrics

    return dask_benchmark_func


@fixture
def lbench_dask_collection(lbench_dask, benchmark):
    def collection_benchmark_func(collection):
        run_func = lambda: collection.compute()
        graph = collection.dask

        # Measure graph length
        graph_len = len(graph)

        # Measure graph size in memory
        graph_size = 0
        for key in graph.keys():
            graph_size += sys.getsizeof(graph[key])
        lbench_dask(run_func)
        benchmark.extra_info["dask"]["dask_graph_len"] = graph_len
        benchmark.extra_info["dask"]["dask_graph_size_bytes"] = graph_size

    return collection_benchmark_func
