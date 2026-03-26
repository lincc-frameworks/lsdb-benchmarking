from typing import Optional

from lbench.dashboard.metrics import Metric
from lbench.dashboard.metrics.metric_group import MetricGroup


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


computed_group = MetricGroup(
    "computed",
    "Computed Metrics",
    [CoefficientOfVariation()]
)