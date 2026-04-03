import time

import pytest
from dask import delayed


def test_sleep(lbench):
    def sleep_function():
        time.sleep(0.1)

    lbench(sleep_function)


@pytest.mark.lbench_memory
def test_sleep_with_mem(lbench):
    def sleep_with_mem():
        _ = [0] * 1_000_000
        time.sleep(0.1)

    lbench(sleep_with_mem)


def test_dask_sleep(lbench_dask):
    @delayed
    def sleep_function():
        time.sleep(0.1)

    def dask_sleep_function():
        sleep_function().compute()

    lbench_dask(dask_sleep_function)


def test_dask_sleep_col(lbench_dask_collection):
    @delayed
    def sleep_function():
        time.sleep(0.1)

    lbench_dask_collection(sleep_function())
