import pandas as pd
from typing import Optional, List
from lbench.dashboard.metrics.metric import Metric


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

