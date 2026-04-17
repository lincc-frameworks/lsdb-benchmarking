"""
Jupyter cell magic for lbench benchmarks.

Usage
-----
Load the extension once per notebook::

    %load_ext lbench.notebook

Then use the cell magic (similar to %%timeit)::

    %%lbench
    my_expensive_function()

Options::

    %%lbench --rounds 10 --warmup --memory --profile --dask --name my_bench
    my_dask_function()

    # Also capture Dask graph stats from a collection variable:
    %%lbench --dask --collection my_df
    my_df.compute()

Options
-------
--rounds / -r       Number of timed rounds (default: 5)
--warmup / -w       Run one un-timed warmup round first
--memory / -m       Track peak memory with memray
--profile / -p      Capture a cProfile .prof file
--dask / -d         Collect Dask metrics (task stream, memory, performance report)
--collection VAR    Name of a Dask collection variable; also records graph size/length
--name / -n         Name for this benchmark entry (default: auto-generated)
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from IPython.core.magic import Magics, cell_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from lbench.cli.env import get_lbench_root_dir
from lbench.runner import (
    make_benchmark_entry,
    run_cprofile,
    run_dask_benchmark,
    run_memray,
    time_function,
    write_benchmark_json,
)

# -- session state -----------------------------------------------------------

_run_dir: Optional[Path] = None


def _get_run_dir() -> Path:
    global _run_dir
    if _run_dir is None:
        root = get_lbench_root_dir()
        run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        _run_dir = root / run_id
        _run_dir.mkdir(parents=True, exist_ok=True)
    return _run_dir


def reset_session():
    """Start a fresh run directory for this notebook session."""
    global _run_dir
    _run_dir = None


# -- display helpers ---------------------------------------------------------


def _fmt_time(seconds: float) -> str:
    if seconds >= 1:
        return f"{seconds:.3f} s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.3f} ms"
    if seconds >= 1e-6:
        return f"{seconds * 1e6:.3f} µs"
    return f"{seconds * 1e9:.3f} ns"


def _fmt_memory(nbytes: int) -> str:
    for unit, scale in [("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)]:
        if nbytes >= scale:
            return f"{nbytes / scale:.2f} {unit}"
    return f"{nbytes} B"


# -- magic class -------------------------------------------------------------


@magics_class
class LbenchMagics(Magics):
    """Provides the %%lbench cell magic."""

    @cell_magic
    @magic_arguments()
    @argument("--rounds", "-r", type=int, default=5, help="Number of timed rounds (default: 5)")
    @argument("--warmup", "-w", action="store_true", help="Run one un-timed warmup round before measuring")
    @argument("--memory", "-m", action="store_true", help="Track peak memory usage with memray")
    @argument("--profile", "-p", action="store_true", help="Capture a cProfile .prof file")
    @argument(
        "--dask",
        "-d",
        action="store_true",
        help="Collect Dask metrics (task stream, memory sampler, performance report)",
    )
    @argument(
        "--collection",
        type=str,
        default=None,
        metavar="VAR",
        help="Name of a Dask collection variable; also records graph size and length",
    )
    @argument("--name", "-n", type=str, default=None, help="Name for this benchmark entry")
    def lbench(self, line: str, cell: str):
        """Benchmark a cell's code and save results to a lbench-compatible JSON log."""
        args = parse_argstring(self.lbench, line)

        ip = self.shell
        ns = ip.user_ns

        name = args.name or f"cell_{datetime.now().strftime('%H%M%S')}"
        fullname = f"notebook::{name}"
        run_dir = _get_run_dir()

        code = compile(cell, f"<lbench:{name}>", "exec")

        def run_cell():
            exec(code, ns)  # noqa: S102 – intentional notebook execution

        # --- time -----------------------------------------------------------
        data = time_function(run_cell, rounds=args.rounds, warmup=args.warmup)

        # --- optional profiling ---------------------------------------------
        extra_info: dict = {}

        if args.profile:
            extra_info["cprofile_path"] = run_cprofile(run_cell, run_dir)

        if args.memory:
            extra_info["peak_memory_bytes"] = run_memray(run_cell, run_dir)

        # --- optional dask metrics ------------------------------------------
        if args.dask:
            dask_info = run_dask_benchmark(run_cell, run_dir)

            if args.collection:
                collection = ns.get(args.collection)
                if collection is None:
                    raise NameError(f"--collection: variable {args.collection!r} not found in namespace")
                graph = collection.dask
                dask_info["dask_graph_len"] = len(graph)
                dask_info["dask_graph_size_bytes"] = sum(sys.getsizeof(graph[k]) for k in graph)

            extra_info["dask"] = dask_info

        # --- build entry & write JSON ---------------------------------------
        entry = make_benchmark_entry(
            name=name,
            fullname=fullname,
            data=data,
            extra_info=extra_info,
        )
        json_path = write_benchmark_json(run_dir, [entry])

        # --- display --------------------------------------------------------
        stats = entry["stats"]
        print(
            f"{stats['rounds']} rounds  "
            f"mean: {_fmt_time(stats['mean'])} ± {_fmt_time(stats['stddev'])}  "
            f"(min: {_fmt_time(stats['min'])}, max: {_fmt_time(stats['max'])})"
        )
        if "peak_memory_bytes" in extra_info:
            print(f"peak memory: {_fmt_memory(extra_info['peak_memory_bytes'])}")
        if "dask" in extra_info:
            d = extra_info["dask"]
            print(f"dask tasks:  {d.get('n_tasks', '?')}", end="")
            if "peak_memory_bytes" in d:
                print(f"  peak memory: {_fmt_memory(d['peak_memory_bytes'])}", end="")
            if "dask_graph_len" in d:
                print(f"  graph nodes: {d['dask_graph_len']}", end="")
            print()
            print(f"perf report: {d['performance_report']}")
        if "cprofile_path" in extra_info:
            print(f"cProfile:    {extra_info['cprofile_path']}")
        print(f"log:         {json_path}")
