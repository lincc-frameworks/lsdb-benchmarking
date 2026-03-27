from typing import Optional, Dict, Any

from lbench.dashboard.metrics import DurationMetric, Metric
from lbench.dashboard.metrics.metric_group import MetricGroup


class StatsMetric(DurationMetric):
    """Base class for metrics from the 'stats' section with time formatting."""

    def __init__(self, name: str, display_name: str, stats_key: str = None):
        super().__init__(name, display_name)
        self.stats_key = stats_key or name

    def extract(self, benchmark_data: dict) -> Optional[float]:
        try:
            value = benchmark_data.get("stats", {}).get(self.stats_key)
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None


# Create stats metrics
min_metric = StatsMetric("min", "Min")
max_metric = StatsMetric("max", "Max")
stddev_metric = StatsMetric("stddev", "Std Dev")
mean_metric = StatsMetric("mean", "Mean")
median_metric = StatsMetric("median", "Median")
iqr_metric = StatsMetric("iqr", "IQR")
q1_metric = StatsMetric("q1", "Q1")
q3_metric = StatsMetric("q3", "Q3")


class StatsGroup(MetricGroup):
    """Statistics metric group with error bar configuration."""

    def get_error_bar_config(self, metric: Metric) -> Optional[Dict[str, Any]]:
        """Mean uses stddev for error bars."""
        if metric.name == "mean":
            return {
                "metric": stddev_metric,
                "type": "symmetric"
            }
        return None


# Create stats group
stats_group = StatsGroup(
    "stats",
    "",
    [min_metric, max_metric, mean_metric, median_metric, stddev_metric, iqr_metric, q1_metric, q3_metric]
)
