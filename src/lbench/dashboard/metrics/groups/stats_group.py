from typing import Optional

from lbench.dashboard.metrics import DurationMetric, Metric
from lbench.dashboard.metrics.metric_group import MetricGroup


class StatsMetric(DurationMetric):
    """Base class for metrics from the 'stats' section with time formatting."""

    def __init__(self, name: str, display_name: str, stats_key: str = None, error_bar_metric: "StatsMetric" = None):
        super().__init__(name, display_name)
        self.stats_key = stats_key or name
        self._error_bar_metric = error_bar_metric

    def extract(self, benchmark_data: dict) -> Optional[float]:
        try:
            value = benchmark_data.get("stats", {}).get(self.stats_key)
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def get_error_bar_metric(self) -> Optional[Metric]:
        return self._error_bar_metric


# Create stats metrics
min_metric = StatsMetric("min", "Min")
max_metric = StatsMetric("max", "Max")
stddev_metric = StatsMetric("stddev", "Std Dev")
mean_metric = StatsMetric("mean", "Mean", error_bar_metric=stddev_metric)
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
