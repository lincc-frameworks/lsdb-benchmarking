"""Core benchmark runner utilities shared between pytest fixtures and notebook magic."""

import cProfile
import json
import platform
import statistics
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional


def run_cprofile(func: Callable, run_dir: Path, *args, **kwargs) -> str:
    """Run func under cProfile, save .prof file, return its path."""
    cprof_path = run_dir / f"cprofile_{uuid.uuid4()}.prof"
    with cProfile.Profile() as pr:
        func(*args, **kwargs)
    pr.dump_stats(str(cprof_path))
    return str(cprof_path)


def run_dask_benchmark(
    func: Callable,
    run_dir: Path,
    client=None,
    *args,
    **kwargs,
) -> dict:
    """Run func with Dask metrics collection (memory, task stream, performance report).

    Returns the dict that should be stored as extra_info["dask"].
    If *client* is None, the current default distributed client is used.
    """
    from distributed import get_task_stream, performance_report
    from distributed.diagnostics.memory_sampler import MemorySampler

    if client is None:
        from distributed import get_client
        client = get_client()

    report_path = run_dir / f"dask_performance_report_{uuid.uuid4()}.html"

    ms = MemorySampler()
    with performance_report(filename=report_path):
        with ms.sample("benchmark"):
            with get_task_stream(client) as ts:
                func(*args, **kwargs)

    extra: dict = {}
    memory_df = ms.to_pandas()
    if not memory_df.empty and "benchmark" in memory_df.columns:
        extra["peak_memory_bytes"] = int(memory_df["benchmark"].max())
    extra["n_tasks"] = len(ts.data)
    extra["keys"] = [t["key"] for t in ts.data]
    extra["startstops"] = [t["startstops"] for t in ts.data]
    extra["performance_report"] = str(report_path)
    return extra


def run_memray(func: Callable, run_dir: Path, *args, **kwargs) -> int:
    """Run func under memray memory tracking, return peak memory in bytes."""
    import memray

    memray_path = run_dir / f"memray_{uuid.uuid4()}.bin"
    with memray.Tracker(memray_path):
        func(*args, **kwargs)
    reader = memray.FileReader(memray_path)
    return reader.metadata.peak_memory


def time_function(
    func: Callable,
    *args,
    rounds: int = 5,
    warmup: bool = False,
    **kwargs,
) -> list:
    """Execute func repeatedly with perf_counter timing, return list of elapsed seconds."""
    if warmup:
        func(*args, **kwargs)
    data = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        func(*args, **kwargs)
        data.append(time.perf_counter() - t0)
    return data


def compute_stats(data: list) -> dict:
    """Compute pytest-benchmark-compatible stats dict from a list of timing values."""
    n = len(data)
    mean = statistics.mean(data)
    median = statistics.median(data)
    stddev = statistics.stdev(data) if n > 1 else 0.0
    min_val = min(data)
    max_val = max(data)
    total = sum(data)

    if n >= 2:
        cuts = statistics.quantiles(data, n=4)
        q1, q3 = cuts[0], cuts[2]
    else:
        q1, q3 = min_val, max_val
    iqr = q3 - q1

    iqr_outliers = sum(
        1 for x in data if x < q1 - 1.5 * iqr or x > q3 + 1.5 * iqr
    )
    stddev_outliers = sum(1 for x in data if abs(x - mean) > stddev)

    return {
        "min": min_val,
        "max": max_val,
        "mean": mean,
        "median": median,
        "stddev": stddev,
        "rounds": n,
        "iterations": 1,
        "iqr": iqr,
        "q1": q1,
        "q3": q3,
        "iqr_outliers": iqr_outliers,
        "stddev_outliers": stddev_outliers,
        "outliers": f"{stddev_outliers};{iqr_outliers}",
        "ops": 1.0 / mean if mean > 0 else 0.0,
        "total": total,
        "data": data,
    }


def make_benchmark_entry(
    name: str,
    fullname: str,
    data: list,
    extra_info: Optional[dict] = None,
    group: Optional[str] = None,
    params: Optional[dict] = None,
) -> dict:
    """Build a benchmark entry dict in pytest-benchmark JSON format."""
    param_str = (
        "-".join(str(v) for v in params.values()) if params else None
    )
    return {
        "group": group,
        "name": name,
        "fullname": fullname,
        "params": params,
        "param": param_str,
        "extra_info": extra_info or {},
        "options": {
            "disable_gc": False,
            "timer": "perf_counter",
            "min_rounds": len(data),
            "max_time": None,
            "min_time": None,
            "warmup": False,
        },
        "stats": compute_stats(data),
    }


def get_machine_info() -> dict:
    info = {
        "node": platform.node(),
        "processor": platform.processor(),
        "machine": platform.machine(),
        "python_compiler": platform.python_compiler(),
        "python_implementation": platform.python_implementation(),
        "python_implementation_version": platform.python_version(),
        "python_version": platform.python_version(),
        "python_build": list(platform.python_build()),
        "release": platform.release(),
        "system": platform.system(),
    }
    try:
        import cpuinfo

        info["cpu"] = cpuinfo.get_cpu_info()
    except ImportError:
        info["cpu"] = {"brand_raw": platform.processor()}
    return info


def get_commit_info() -> dict:
    try:
        git_id = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
        git_time = subprocess.check_output(
            ["git", "log", "-1", "--format=%cI"], stderr=subprocess.DEVNULL
        ).decode().strip()
        git_branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
        dirty = (
            subprocess.call(["git", "diff", "--quiet"], stderr=subprocess.DEVNULL) != 0
        )
        return {
            "id": git_id,
            "time": git_time,
            "author_time": git_time,
            "dirty": dirty,
            "project": Path.cwd().name,
            "branch": git_branch,
        }
    except Exception:
        return {
            "id": None,
            "time": None,
            "author_time": None,
            "dirty": None,
            "project": Path.cwd().name,
            "branch": None,
        }


def write_benchmark_json(run_dir: Path, benchmarks: list) -> Path:
    """Append benchmarks to (or create) a pytest-benchmark-compatible JSON file."""
    json_path = run_dir / "pytest-benchmark.json"

    existing: list = []
    if json_path.exists() and json_path.stat().st_size > 0:
        try:
            with open(json_path) as f:
                existing = json.load(f).get("benchmarks", [])
        except (json.JSONDecodeError, KeyError):
            pass

    payload = {
        "machine_info": get_machine_info(),
        "commit_info": get_commit_info(),
        "benchmarks": existing + benchmarks,
        "datetime": datetime.now(timezone.utc).isoformat(),
        "version": "5.2.3",
    }

    with open(json_path, "w") as f:
        json.dump(payload, f, indent=4)

    return json_path
