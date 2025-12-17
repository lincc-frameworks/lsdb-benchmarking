from upath import UPath
from pytest import fixture
from distributed import Client, get_task_stream


@fixture
def single_thread_dask_client():
    with Client(n_workers=1, threads_per_worker=1) as client:
        yield client


@fixture
def catalog_dir() -> UPath:
    return UPath("/epyc/data3/hats/catalogs")


@fixture
def gaia_collection_path(catalog_dir: UPath) -> UPath:
    return catalog_dir / "gaia_dr3"


@fixture
def gaia_s3_path() -> str:
    return "s3://stpubdata/gaia/gaia_dr3/public/hats"


@fixture
def dask_benchmark(benchmark, single_thread_dask_client: Client):
    def dask_benchmark_func(func, *args, **kwargs):
        extra_metrics = {}
        with get_task_stream(single_thread_dask_client) as ts:
            benchmark(func, *args, **kwargs)
        # ts is now a TaskStream object
        extra_metrics["n_tasks"] = len(ts.data)  # number of tasks executed
        extra_metrics["keys"] = [t["key"] for t in ts.data]
        extra_metrics["startstops"] = [t["startstops"] for t in ts.data]

        benchmark.extra_info.update(extra_metrics)

    return dask_benchmark_func
