"""
Flexible metrics system for benchmark data.

This module provides a registry-based system for defining and extracting metrics
from benchmark runs. It's designed to be extensible and resilient to missing data.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd


def format_duration(seconds, digits=3):
    """
    Format a duration in seconds using the most appropriate unit.
    Returns (value_str, unit).
    """
    if seconds is None:
        return "-", ""

    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return str(seconds), ""

    if seconds >= 1:
        return f"{seconds:.{digits}f}", "s"
    elif seconds >= 1e-3:
        return f"{seconds * 1e3:.{digits}f}", "ms"
    elif seconds >= 1e-6:
        return f"{seconds * 1e6:.{digits}f}", "µs"
    else:
        return f"{seconds * 1e9:.{digits}f}", "ns"


class Metric(ABC):
    """Base class for all metrics.

    Metrics extract specific values from raw benchmark data.
    They gracefully handle missing data by returning None.
    Metrics can also control how they're displayed in tables and trends.
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

    def format_value(self, value: Optional[float]) -> str:
        """Format the metric value for display.

        Override this for custom formatting (e.g., time units).

        Args:
            value: Raw metric value

        Returns:
            Formatted string
        """
        if value is None:
            return "-"
        return f"{value:.3f}"

    def get_table_column_name(self) -> str:
        """Get the column name for this metric in tables.

        Returns:
            Column name with unit if applicable
        """
        if self.unit:
            return f"{self.display_name} ({self.unit})"
        return self.display_name

    def supports_error_bars(self) -> bool:
        """Check if this metric supports error bars in trend plots.

        Returns:
            True if error bars are available
        """
        return False

    def get_error_bar_metric(self) -> Optional['Metric']:
        """Get the metric to use for error bars (e.g., stddev for mean).

        Returns:
            Metric for error bars or None
        """
        return None

    def __repr__(self):
        return f"<Metric: {self.name}>"


class MetricGroup:
    """Groups related metrics together (e.g., stats, dask metrics).

    Used for organizing table display and grouping metrics logically.
    """

    def __init__(self, name: str, display_name: str, metrics: List[Metric] = None):
        self.name = name
        self.display_name = display_name
        self.metrics = metrics or []

    def add_metric(self, metric: Metric):
        """Add a metric to this group."""
        self.metrics.append(metric)

    def is_available(self, benchmark_data: dict) -> bool:
        """Check if any metrics in this group are available."""
        return any(m.is_available(benchmark_data) for m in self.metrics)

    def get_available_metrics(self, benchmark_data: dict) -> List[Metric]:
        """Get metrics from this group that are available."""
        return [m for m in self.metrics if m.is_available(benchmark_data)]

    def to_dataframe(self, benchmark_data: dict) -> Optional[pd.DataFrame]:
        """Create a DataFrame for this metric group's table.

        Args:
            benchmark_data: Raw benchmark data

        Returns:
            DataFrame with one row or None if no metrics available
        """
        available = self.get_available_metrics(benchmark_data)
        if not available:
            return None

        data = {}
        for metric in available:
            value = metric.extract(benchmark_data)
            formatted = metric.format_value(value)
            col_name = metric.get_table_column_name()
            data[col_name] = [formatted]

        return pd.DataFrame(data)


class MetricRegistry:
    """Registry for managing available metrics and groups.

    Allows registration of built-in and custom metrics.
    """

    def __init__(self):
        self._metrics: Dict[str, Metric] = {}
        self._groups: Dict[str, MetricGroup] = {}

    def register(self, metric: Metric) -> Metric:
        """Register a metric.

        Args:
            metric: Metric instance to register

        Returns:
            The registered metric (for decorator usage)
        """
        self._metrics[metric.name] = metric
        return metric

    def register_group(self, group: MetricGroup) -> MetricGroup:
        """Register a metric group.

        Args:
            group: MetricGroup instance to register

        Returns:
            The registered group
        """
        self._groups[group.name] = group
        # Also register individual metrics
        for metric in group.metrics:
            self.register(metric)
        return group

    def get(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        return self._metrics.get(name)

    def get_group(self, name: str) -> Optional[MetricGroup]:
        """Get a metric group by name."""
        return self._groups.get(name)

    def list_all(self) -> List[Metric]:
        """Get all registered metrics."""
        return list(self._metrics.values())

    def list_groups(self) -> List[MetricGroup]:
        """Get all registered metric groups."""
        return list(self._groups.values())

    def get_available_metrics(self, benchmark_data: dict) -> List[Metric]:
        """Get metrics that are available for the given benchmark data.

        Args:
            benchmark_data: Raw benchmark dictionary

        Returns:
            List of available metrics
        """
        return [m for m in self._metrics.values() if m.is_available(benchmark_data)]

    def get_available_groups(self, benchmark_data: dict) -> List[MetricGroup]:
        """Get metric groups that have at least one available metric.

        Args:
            benchmark_data: Raw benchmark dictionary

        Returns:
            List of available groups
        """
        return [g for g in self._groups.values() if g.is_available(benchmark_data)]


# Global registry
registry = MetricRegistry()


# --- Built-in Metrics ---

class StatsMetric(Metric):
    """Base class for metrics from the 'stats' section with time formatting."""

    def __init__(self, name: str, display_name: str, stats_key: str = None, error_bar_metric: str = None):
        super().__init__(name, display_name, unit="s")
        self.stats_key = stats_key or name
        self._error_bar_metric_name = error_bar_metric

    def extract(self, benchmark_data: dict) -> Optional[float]:
        try:
            value = benchmark_data.get("stats", {}).get(self.stats_key)
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def format_value(self, value: Optional[float]) -> str:
        """Format using appropriate time units."""
        formatted, unit = format_duration(value)
        return formatted

    def get_table_column_name(self) -> str:
        """Get column name with dynamic units based on typical values."""
        # For table headers, we'll use a fixed unit - formatting handles display
        # But we want to show what the base unit is
        return f"{self.display_name} (s)"

    def supports_error_bars(self) -> bool:
        """Time metrics with stddev support error bars."""
        return self._error_bar_metric_name is not None

    def get_error_bar_metric(self) -> Optional[Metric]:
        """Get the stddev metric for error bars."""
        if self._error_bar_metric_name:
            return registry.get(self._error_bar_metric_name)
        return None


# Create stats metrics
min_metric = StatsMetric("min", "Min")
max_metric = StatsMetric("max", "Max")
stddev_metric = StatsMetric("stddev", "Std Dev")
mean_metric = StatsMetric("mean", "Mean", error_bar_metric="stddev")
median_metric = StatsMetric("median", "Median")
iqr_metric = StatsMetric("iqr", "IQR")
q1_metric = StatsMetric("q1", "Q1")
q3_metric = StatsMetric("q3", "Q3")

# Create stats group
stats_group = MetricGroup(
    "stats",
    "Performance Statistics",
    [min_metric, max_metric, mean_metric, median_metric, stddev_metric, iqr_metric, q1_metric, q3_metric]
)
registry.register_group(stats_group)


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

    def format_value(self, value: Optional[float]) -> str:
        """Format as integer."""
        if value is None:
            return "-"
        return str(int(value))


rounds_metric = CountMetric("rounds", "Rounds")
iterations_metric = CountMetric("iterations", "Iterations")

# Create execution group
execution_group = MetricGroup(
    "execution",
    "Execution Info",
    [rounds_metric, iterations_metric]
)
registry.register_group(execution_group)


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

    def format_value(self, value: Optional[float]) -> str:
        """Format as percentage."""
        if value is None:
            return "-"
        return f"{value * 100:.2f}%"


cv_metric = CoefficientOfVariation()

computed_group = MetricGroup(
    "computed",
    "Computed Metrics",
    [cv_metric]
)
registry.register_group(computed_group)


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
        super().__init__("dask_n_tasks", "Task Count", "", "Number of Dask tasks executed")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                return float(dask_stats.get("n_tasks"))
            except (TypeError, ValueError):
                pass
        return None

    def format_value(self, value: Optional[float]) -> str:
        """Format as integer."""
        if value is None:
            return "-"
        return str(int(value))


class DaskTotalTime(DaskMetric):
    """Total Dask execution time."""

    def __init__(self):
        super().__init__("dask_total_time", "Total Time", "s", "Total Dask task execution time")

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

    def format_value(self, value: Optional[float]) -> str:
        """Format using appropriate time units."""
        formatted, unit = format_duration(value)
        return formatted


dask_task_count = DaskTaskCount()
dask_total_time = DaskTotalTime()

dask_group = MetricGroup(
    "dask",
    "Dask Metrics",
    [dask_task_count, dask_total_time]
)
registry.register_group(dask_group)


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
