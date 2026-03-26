"""
Flexible metrics system for benchmark data.

This module provides a registry-based system for defining and extracting metrics
from benchmark runs. It's designed to be extensible and resilient to missing data.
"""

from abc import ABC, abstractmethod
from typing import Optional


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

    def get_table_column_name(self) -> str:
        """Get the column name for this metric in tables.

        Returns:
            Column name with unit if applicable
        """
        if self.unit:
            return f"{self.display_name} ({self.unit})"
        return self.display_name

    def supports_error_bars(self) -> bool:
        """Check if this metric supports error bars in trend plots.

        Returns:
            True if error bars are available
        """
        return False

    def get_error_bar_metric(self) -> Optional['Metric']:
        """Get the metric to use for error bars (e.g., stddev for mean).

        Returns:
            Metric for error bars or None
        """
        return None

    def __repr__(self):
        return f"<Metric: {self.name}>"
