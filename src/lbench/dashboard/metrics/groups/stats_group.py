from typing import Optional

from lbench.dashboard.metrics import Metric
from lbench.dashboard.metrics.metric_group import MetricGroup
from lbench.dashboard.utils import format_duration


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
        #if self._error_bar_metric_name:
        #    return registry.get(self._error_bar_metric_name)
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
