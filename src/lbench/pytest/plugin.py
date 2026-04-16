from __future__ import annotations

from datetime import datetime

import pytest

from lbench.cli.env import get_lbench_root_dir


def pytest_addoption(parser):
    group = parser.getgroup("lbench")
    group.addoption(
        "--lbench",
        action="store_true",
        help="Run benchmarks using lbench runner",
    )
    group.addoption(
        "--lbench-root",
        action="store",
        default=None,
        help="Root directory for lbench runs",
    )


def pytest_configure(config: pytest.Config):
    if not config.getoption("--lbench"):
        return

    root = config.getoption("--lbench-root")
    if root is None:
        root = get_lbench_root_dir()

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    # stash on config for fixtures
    config.lbench_run_dir = run_dir

    # configure pytest-benchmark
    config.option.benchmark_only = True
    config.option.benchmark_json = (run_dir / "pytest-benchmark.json").open("wb")  # kinda hacky

    terminal = config.pluginmanager.get_plugin("terminalreporter")
    if terminal:
        terminal.write_line(f"[lbench] running benchmarks in {run_dir}")


def pytest_sessionstart(session):
    if not session.config.getoption("--lbench"):
        return

    if not session.config.pluginmanager.hasplugin("benchmark"):
        pytest.exit(
            "pytest-benchmark is required for --lbench",
            returncode=2,
        )
