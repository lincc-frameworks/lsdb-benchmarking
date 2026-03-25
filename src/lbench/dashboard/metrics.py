"""
Flexible metrics system for benchmark data.

This module provides a registry-based system for defining and extracting metrics
from benchmark runs. It's designed to be extensible and resilient to missing data.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd


class Metric(ABC):
    """Base class for all metrics.

    Metrics extract specific values from raw benchmark data.
    They gracefully handle missing data by returning None.
    """

    def __init__(self, name: str, display_name: str, unit: str = "", description: str = ""):
        self.name = name
        self.display_name = display_name
        self.unit = unit
        self.description = description

    @abstractmethod
    def extract(self, benchmark_data: dict) -> Optional[float]:
        """Extract metric value from raw benchmark data.

        Args:
            benchmark_data: Raw benchmark dictionary from pytest-benchmark

        Returns:
            Metric value or None if not available
        """
        pass

    def is_available(self, benchmark_data: dict) -> bool:
        """Check if this metric is available for the given benchmark."""
        try:
            return self.extract(benchmark_data) is not None
        except (KeyError, TypeError, ValueError):
            return False

    def __repr__(self):
        return f"<Metric: {self.name}>"


class MetricRegistry:
    """Registry for managing available metrics.

    Allows registration of built-in and custom metrics.
    """

    def __init__(self):
        self._metrics: Dict[str, Metric] = {}

    def register(self, metric: Metric) -> Metric:
        """Register a metric.

        Args:
            metric: Metric instance to register

        Returns:
            The registered metric (for decorator usage)
        """
        self._metrics[metric.name] = metric
        return metric

    def get(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        return self._metrics.get(name)

    def list_all(self) -> List[Metric]:
        """Get all registered metrics."""
        return list(self._metrics.values())

    def get_available_metrics(self, benchmark_data: dict) -> List[Metric]:
        """Get metrics that are available for the given benchmark data.

        Args:
            benchmark_data: Raw benchmark dictionary

        Returns:
            List of available metrics
        """
        return [m for m in self._metrics.values() if m.is_available(benchmark_data)]


# Global registry
registry = MetricRegistry()


# --- Built-in Metrics ---

class StatsMetric(Metric):
    """Base class for metrics from the 'stats' section."""

    def __init__(self, name: str, display_name: str, unit: str = "s", stats_key: str = None):
        super().__init__(name, display_name, unit)
        self.stats_key = stats_key or name

    def extract(self, benchmark_data: dict) -> Optional[float]:
        try:
            value = benchmark_data.get("stats", {}).get(self.stats_key)
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None


# Register standard stats metrics
registry.register(StatsMetric("min", "Min Time", "s"))
registry.register(StatsMetric("max", "Max Time", "s"))
registry.register(StatsMetric("mean", "Mean Time", "s"))
registry.register(StatsMetric("median", "Median Time", "s"))
registry.register(StatsMetric("stddev", "Std Dev", "s"))
registry.register(StatsMetric("iqr", "IQR", "s"))
registry.register(StatsMetric("q1", "Q1", "s"))
registry.register(StatsMetric("q3", "Q3", "s"))


class CountMetric(Metric):
    """Metric for counts (rounds, iterations)."""

    def __init__(self, name: str, display_name: str, stats_key: str = None):
        super().__init__(name, display_name, unit="", description=f"Number of {name}")
        self.stats_key = stats_key or name

    def extract(self, benchmark_data: dict) -> Optional[float]:
        try:
            value = benchmark_data.get("stats", {}).get(self.stats_key)
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None


registry.register(CountMetric("rounds", "Rounds"))
registry.register(CountMetric("iterations", "Iterations"))


# --- Computed Metrics ---

class CoefficientOfVariation(Metric):
    """Coefficient of variation (stddev / mean)."""

    def __init__(self):
        super().__init__(
            "cv",
            "Coefficient of Variation",
            "",
            "Relative standard deviation (stddev/mean)"
        )

    def extract(self, benchmark_data: dict) -> Optional[float]:
        try:
            stats = benchmark_data.get("stats", {})
            mean = stats.get("mean")
            stddev = stats.get("stddev")
            if mean and stddev and mean != 0:
                return float(stddev) / float(mean)
        except (TypeError, ValueError, ZeroDivisionError):
            pass
        return None


registry.register(CoefficientOfVariation())


# --- Dask Metrics ---

class DaskMetric(Metric):
    """Base class for Dask-related metrics."""

    def get_dask_stats(self, benchmark_data: dict) -> Optional[dict]:
        """Extract dask stats if available."""
        try:
            return benchmark_data.get("extra_info", {}).get("dask")
        except (KeyError, TypeError):
            return None


class DaskTaskCount(DaskMetric):
    """Number of Dask tasks."""

    def __init__(self):
        super().__init__("dask_n_tasks", "Dask Task Count", "", "Number of Dask tasks executed")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                return float(dask_stats.get("n_tasks"))
            except (TypeError, ValueError):
                pass
        return None


class DaskTotalTime(DaskMetric):
    """Total Dask execution time."""

    def __init__(self):
        super().__init__("dask_total_time", "Dask Total Time", "s", "Total Dask task execution time")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                startstops = dask_stats.get("startstops", [])
                times = [
                    sum([k["stop"] - k["start"] for k in s])
                    for s in startstops
                ]
                return sum(times) if times else None
            except (TypeError, ValueError, KeyError):
                pass
        return None


registry.register(DaskTaskCount())
registry.register(DaskTotalTime())


# --- Data Structures ---

@dataclass
class BenchmarkRun:
    """Represents a single benchmark run with structured access to metrics."""

    name: str  # Benchmark name (fullname)
    run_id: str  # Run identifier (timestamp)
    timestamp: datetime
    raw_data: dict
    _metric_cache: Dict[str, Optional[float]] = field(default_factory=dict, repr=False)

    def get_metric_value(self, metric: Metric) -> Optional[float]:
        """Get value for a specific metric, with caching.

        Args:
            metric: Metric to extract

        Returns:
            Metric value or None if not available
        """
        if metric.name not in self._metric_cache:
            self._metric_cache[metric.name] = metric.extract(self.raw_data)
        return self._metric_cache[metric.name]

    def get_available_metrics(self) -> List[Metric]:
        """Get all metrics available for this run."""
        return registry.get_available_metrics(self.raw_data)

    def has_metric(self, metric: Metric) -> bool:
        """Check if this run has a value for the given metric."""
        return self.get_metric_value(metric) is not None


class BenchmarkCollection:
    """Collection of all benchmark runs with querying capabilities."""

    def __init__(self, run_data: dict):
        """Initialize from raw run data.

        Args:
            run_data: Dict mapping run_id -> benchmark data (from load_all_runs)
        """
        self.runs: List[BenchmarkRun] = []
        self._benchmark_index: Dict[str, List[BenchmarkRun]] = {}

        # Parse all runs
        for run_id, run_info in run_data.items():
            try:
                timestamp = pd.to_datetime(run_id)
            except (ValueError, TypeError):
                timestamp = None

            for bm_data in run_info.get("benchmarks", []):
                bm_name = bm_data.get("fullname")
                if not bm_name:
                    continue

                run = BenchmarkRun(
                    name=bm_name,
                    run_id=run_id,
                    timestamp=timestamp,
                    raw_data=bm_data
                )

                self.runs.append(run)

                # Index by benchmark name
                if bm_name not in self._benchmark_index:
                    self._benchmark_index[bm_name] = []
                self._benchmark_index[bm_name].append(run)

    def get_benchmark_names(self) -> List[str]:
        """Get all unique benchmark names."""
        return sorted(self._benchmark_index.keys())

    def get_runs(self, benchmark: str) -> List[BenchmarkRun]:
        """Get all runs for a specific benchmark.

        Args:
            benchmark: Benchmark name

        Returns:
            List of runs, sorted by timestamp
        """
        runs = self._benchmark_index.get(benchmark, [])
        return sorted(runs, key=lambda r: r.timestamp if r.timestamp else datetime.min)

    def get_metric_series(self, benchmark: str, metric: Metric) -> pd.DataFrame:
        """Get time series data for a specific benchmark and metric.

        Args:
            benchmark: Benchmark name
            metric: Metric to extract

        Returns:
            DataFrame with columns: run_id, timestamp, value
        """
        runs = self.get_runs(benchmark)

        data = []
        for run in runs:
            value = run.get_metric_value(metric)
            if value is not None:  # Only include runs where metric is available
                data.append({
                    "run_id": run.run_id,
                    "timestamp": run.timestamp,
                    "value": value,
                })

        return pd.DataFrame(data)

    def get_available_metrics_for_benchmark(self, benchmark: str) -> List[Metric]:
        """Get metrics that are available for at least one run of this benchmark.

        Args:
            benchmark: Benchmark name

        Returns:
            List of available metrics
        """
        runs = self.get_runs(benchmark)
        if not runs:
            return []

        # Get union of all available metrics across all runs
        available = set()
        for run in runs:
            for metric in run.get_available_metrics():
                available.add(metric.name)

        # Return metric objects in a consistent order
        return [m for m in registry.list_all() if m.name in available]

    def get_common_metrics(self) -> List[Metric]:
        """Get metrics that are available across most benchmarks.

        Returns:
            List of commonly available metrics
        """
        if not self.runs:
            return []

        # Count how many benchmarks have each metric
        metric_counts = {}
        for bm_name in self.get_benchmark_names():
            available = self.get_available_metrics_for_benchmark(bm_name)
            for metric in available:
                metric_counts[metric.name] = metric_counts.get(metric.name, 0) + 1

        # Return metrics available in at least one benchmark
        available_names = set(metric_counts.keys())
        return [m for m in registry.list_all() if m.name in available_names]
