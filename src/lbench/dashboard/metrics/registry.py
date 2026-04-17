from typing import Optional, List, Dict

from lbench.dashboard.metrics.metric import Metric
from lbench.dashboard.metrics.metric_group import MetricGroup


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
