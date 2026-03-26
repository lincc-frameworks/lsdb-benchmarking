from typing import Optional

from lbench.dashboard.metrics import Metric
from lbench.dashboard.metrics.metric_group import MetricGroup
from lbench.dashboard.utils import format_duration


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


dask_group = MetricGroup(
    "dask",
    "Dask Metrics",
    [DaskTaskCount(), DaskTotalTime()]
)