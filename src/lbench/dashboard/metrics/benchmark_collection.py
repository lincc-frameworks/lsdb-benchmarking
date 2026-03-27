import pandas as pd
from dataclasses import field, dataclass
from datetime import datetime
from typing import Optional, Dict, List

from lbench.dashboard.metrics.metric import Metric
from lbench.dashboard.metrics.registry import MetricRegistry


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

    def get_available_metrics(self, registry: "MetricRegistry") -> List[Metric]:
        """Get all metrics available for this run."""
        return registry.get_available_metrics(self.raw_data)

    def has_metric(self, metric: Metric) -> bool:
        """Check if this run has a value for the given metric."""
        return self.get_metric_value(metric) is not None


class BenchmarkCollection:
    """Collection of all benchmark runs with querying capabilities."""

    def __init__(self, run_data: dict, registry: MetricRegistry):
        """Initialize from raw run data.

        Args:
            run_data: Dict mapping run_id -> benchmark data (from load_all_runs)
        """
        self._registry = registry
        self.runs: List[BenchmarkRun] = []
        self._benchmark_index: Dict[str, List[BenchmarkRun]] = {}

        # Parse all runs
        for run_id, run_info in run_data.items():
            try:
                timestamp = pd.to_datetime(run_info.get("datetime"))
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
            for metric in run.get_available_metrics(self._registry):
                available.add(metric.name)

        # Return metric objects in a consistent order
        return [m for m in self._registry.list_all() if m.name in available]

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
        return [m for m in self._registry.list_all() if m.name in available_names]
