# lbench - Benchmarking Tool for Python Projects

lbench is a benchmarking tool built on top of pytest and pytest-benchmark, designed to make it easy to write,
run, and analyze benchmarks for Python projects. It provides automatic result logging, cProfile profiling and
flamegraphs, Dask performance reporting, memory tracking, a Jupyter notebook magic, and a dashboard for
visualizing and comparing benchmark results over time.

## Installation

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
    def benchmark_func():
        result = my_function()

    lbench(benchmark_func)
```

Add `@pytest.mark.lbench_memory` to also track peak memory usage with memray:

```python
@pytest.mark.lbench_memory
def test_my_function(lbench):
    lbench(my_function)
```

### Dask Benchmarks with `lbench_dask` Fixture

```python
def test_my_dask_function(lbench_dask):
    def benchmark_func():
        result = my_dask_dataframe.compute()

    lbench_dask(benchmark_func)
```

The `lbench_dask` fixture automatically collects Dask task stream information, generates a Dask performance
report, and samples memory usage during execution. Use `lbench_dask_collection` to also record the Dask graph
size and node count:

```python
def test_collection(lbench_dask_collection):
    catalog = lsdb.read_hats(...)
    lbench_dask_collection(catalog)
```

## Running Benchmarks

```bash
pytest --lbench benchmarks/
```

This creates a timestamped result directory, runs all benchmarks, and saves:
- `pytest-benchmark.json` — timing stats and extra metrics
- `cprofile_*.prof` — cProfile data for each benchmark
- `dask_performance_report_*.html` — Dask performance reports (Dask benchmarks only)
- `memray_*.bin` — memory tracking data (memory-marked benchmarks only)

### Configuring the Result Directory

```bash
# Via flag
pytest --lbench --lbench-root=/path/to/results benchmarks/

# Via environment variable
export LBENCH_ROOT=/path/to/results
pytest --lbench benchmarks/
```

If neither is set, results are saved to `./lbench_results`.

## Running Benchmarks in Jupyter Notebooks

lbench provides a `%%lbench` cell magic that produces the same JSON log format as the pytest runner,
so notebook results appear alongside pytest results in the dashboard.

Load the extension once per notebook:

```python
%load_ext lbench.notebook
```

Then use the cell magic on any cell:

```python
%%lbench
my_expensive_function()
```

With options:

```python
%%lbench --rounds 10 --warmup --memory --profile --name my_benchmark
my_expensive_function()
```

Available options:

| Option | Short | Description |
|---|---|---|
| `--rounds N` | `-r` | Number of timed rounds (default: 5) |
| `--warmup` | `-w` | Run one un-timed warmup round first |
| `--memory` | `-m` | Track peak memory with memray |
| `--profile` | `-p` | Capture a cProfile `.prof` file |
| `--dask` | `-d` | Collect Dask metrics (task stream, memory, performance report) |
| `--collection VAR` | | Also record graph size/length from a Dask collection variable |
| `--name NAME` | `-n` | Name for this benchmark entry |

### Dask benchmarks in notebooks

```python
%%lbench --dask --rounds 3
my_collection.compute()

# With graph stats from a named variable:
%%lbench --dask --collection src_catalog --name catalog_scan
src_catalog.compute()
```

Results within a notebook session are accumulated into a single timestamped run directory. Call
`lbench.notebook.magic.reset_session()` to start a fresh run directory mid-session.

## Viewing Results with the Dashboard

```bash
lbench dash
lbench dash --port 8051
```

Or from a notebook:

```python
from lbench.dashboard.app import run_dashboard
run_dashboard(port=8050)
```

Calling `run_dashboard()` again will restart the server on the new settings.

### Dashboard Features

**Run browser (sidebar)**
- Lists all runs in chronological order
- Filter runs by date range with the date picker
- Rename runs with the pencil icon

**Benchmark tables**
- Per-benchmark cards showing timing stats, memory usage, and Dask metrics
- Links to open flamegraphs (cProfile) and Dask performance reports directly in the browser

**Trend plots**
- Click "Plot series" to switch to the trend view
- Select one or more benchmarks and a metric to plot performance over time
- Error bars show standard deviation where available
- Respects the active date filter

## Example

```python
import pytest

@pytest.mark.parametrize("size", [1000, 10000, 100000])
def test_dataframe_operation(size, lbench):
    import pandas as pd

    df = pd.DataFrame({'A': range(size), 'B': range(size)})

    def benchmark_func():
        result = df['A'] + df['B']

    lbench(benchmark_func)
```

```bash
pytest --lbench tests/test_dataframe_operation.py
```