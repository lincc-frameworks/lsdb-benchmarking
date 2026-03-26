from typing import Optional

from lbench.dashboard.metrics import Metric
from lbench.dashboard.metrics.metric_group import MetricGroup


class CountMetric(Metric):
    """Metric for counts (rounds, iterations)."""

    def __init__(self, name: str, display_name: str, stats_key: str = None):
        super().__init__(name, display_name, description=f"Number of {name}")
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


execution_group = MetricGroup(
    "execution",
    "Execution Info",
    [
        CountMetric("rounds", "Rounds"),
        CountMetric("iterations", "Iterations")
    ]
)
