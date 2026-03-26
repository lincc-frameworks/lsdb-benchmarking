"""
Flexible metrics system for benchmark data.

This module provides a registry-based system for defining and extracting metrics
from benchmark runs. It's designed to be extensible and resilient to missing data.
"""

from abc import ABC, abstractmethod
from typing import Optional

from lbench.dashboard.utils import format_duration, format_memory


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

    def get_table_column_name(self, value=None) -> str:
        """Get the column name for this metric in tables.

        Override to derive a dynamic unit from the value.

        Returns:
            Column name with unit if applicable
        """
        if self.unit:
            return f"{self.display_name} ({self.unit})"
        return self.display_name

    def get_plot_scale_and_unit(self, values) -> tuple:
        """Return (divisor, unit_label) for plotting based on actual data values.

        Override this to auto-select a human-readable scale from the data.
        The default returns no scaling and the metric's declared unit.
        """
        return 1.0, self.unit

    def get_error_bar_metric(self) -> Optional['Metric']:
        """Get the metric to use for error bars (e.g., stddev for mean).

        Returns:
            Metric for error bars or None
        """
        return None

    def __repr__(self):
        return f"<Metric: {self.name}>"


class DurationMetric(Metric, ABC):
    """Base for metrics whose raw value is in seconds, with dynamic time-unit formatting."""

    def format_value(self, value: Optional[float]) -> str:
        formatted, _ = format_duration(value)
        return formatted

    def get_table_column_name(self, value=None) -> str:
        _, unit = format_duration(value)
        return f"{self.display_name} ({unit})" if unit else self.display_name

    def get_plot_scale_and_unit(self, values) -> tuple:
        representative = float(values.median())
        for threshold, unit in [(1e-3, "ms"), (1e-6, "µs"), (1e-9, "ns")]:
            if representative >= threshold:
                return float(threshold), unit
        return 1.0, "s"


class MemoryMetric(Metric, ABC):
    """Base for metrics whose raw value is in bytes, with dynamic binary-unit formatting."""

    def format_value(self, value: Optional[int]) -> str:
        formatted, _ = format_memory(value)
        return formatted

    def get_table_column_name(self, value=None) -> str:
        _, unit = format_memory(value)
        return f"{self.display_name} ({unit})" if unit else self.display_name

    def get_plot_scale_and_unit(self, values) -> tuple:
        representative = int(values.median())
        for threshold, unit in [(2**40, "TiB"), (2**30, "GiB"), (2**20, "MiB"), (2**10, "KiB")]:
            if representative >= threshold:
                return int(threshold), unit
        return 1, "B"
