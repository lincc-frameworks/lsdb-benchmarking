# lbench - Benchmarking Tool for Python Projects

lbench is a benchmarking tool built on top of pytest and pytest-benchmark, designed to make it easy to write,
run, and analyze benchmarks for Python projects. It provides additional features like automatic result
logging, cProfile profiling and flamegraphs, Dask performance reporting, and a dashboard for visualizing
benchmark results.

## Installation

You can install lbench using pip from source:

```bash
git clone https://github.com/yourusername/lsdb-benchmarking.git
cd lsdb-benchmarking
pip install -e .
```

## Writing Benchmarks

### Basic Benchmarks with `lbench` Fixture

The `lbench` fixture extends pytest-benchmark with automatic cProfile profiling:

```python
def test_my_function(lbench):
    # Setup code here

    def benchmark_func():
        # Code to benchmark
        result = my_function()

    lbench(benchmark_func)
```

### Dask Benchmarks with `lbench_dask` Fixture

For benchmarking Dask operations, use the `lbench_dask` fixture:

```python
def test_my_dask_function(lbench_dask):
    # Setup code here

    def benchmark_func():
        # Dask code to benchmark
        result = my_dask_dataframe.compute()

    lbench_dask(benchmark_func)
```

The `lbench_dask` fixture automatically:

- Collects Dask task stream information
- Generates a Dask performance report
- Runs the benchmark with a single-threaded Dask client

## Running Benchmarks

To run benchmarks, use the `--lbench` option with pytest:

```bash
pytest --lbench benchmarks/
```

This will:

1. Create a timestamped directory for benchmark results
2. Run all benchmarks specified by the pytest command
3. Save pytest-benchmark JSON results
4. Save cProfile files for each benchmark
5. Save Dask performance reports for Dask benchmarks

## Configuring the Result Directory

lbench saves benchmark results in a result directory. You can configure this directory in two ways:

### 1. Using the `--lbench-root` pytest option

```bash
pytest --lbench --lbench-root=/path/to/results benchmarks/
```

### 2. Using the `LBENCH_ROOT` environment variable

```bash
export LBENCH_ROOT=/path/to/results
pytest --lbench benchmarks/
```

If neither is specified, lbench defaults to `./lbench_results` in the current working directory.

## Viewing Results with the Dashboard

lbench includes a dashboard for visualizing benchmark results:

```bash
lbench dash
```

This launches a web dashboard on port 8050 by default. You can specify a different port:

```bash
lbench dash --port 8051
```

The dashboard allows you to:

- Compare benchmark results across different runs
- View detailed profiling information
- Analyze Dask performance reports

## Example

Here's a complete example of a benchmark test:

```python
import pytest


@pytest.mark.parametrize("size", [1000, 10000, 100000])
def test_dataframe_operation(size, lbench):
    import pandas as pd

    # Setup
    df = pd.DataFrame({
        'A': range(size),
        'B': range(size)
    })

    def benchmark_func():
        # Operation to benchmark
        result = df['A'] + df['B']

    lbench(benchmark_func)
```

Run with:

```bash
pytest --lbench tests/test_dataframe_operation.py
```