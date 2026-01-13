import time

from dask import delayed


def test_sleep(lbench):
    def sleep_function():
        time.sleep(0.1)

    lbench(sleep_function)

def test_dask_sleep(dask_benchmark):
    @delayed
    def sleep_function():
        time.sleep(0.1)

    def dask_sleep_function():
        sleep_function().compute()

    dask_benchmark(dask_sleep_function)
