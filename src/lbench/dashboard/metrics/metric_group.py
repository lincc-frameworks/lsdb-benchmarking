import pandas as pd
from typing import Optional, List, Dict, Any
from lbench.dashboard.metrics.metric import Metric


class MetricGroup:
    """Groups related metrics together (e.g., stats, dask metrics).

    Each group is responsible for:
    - Rendering its own display on the runs page
    - Defining which metrics are plottable
    - Providing error bar configurations
    - Adding action buttons (reports, visualizations, etc.)
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
            col_name = metric.get_table_column_name(value)
            data[col_name] = [formatted]

        return pd.DataFrame(data)

    def render_card(self, benchmark_data: dict, run_name: str) -> Optional[Any]:
        """Render the complete card/section for this metric group on the runs page.

        This method should be overridden by subclasses to provide custom rendering.
        Default implementation renders a simple table.

        Args:
            benchmark_data: Raw benchmark data for a single benchmark
            run_name: Name of the run (for building URLs, etc.)

        Returns:
            Dash component (typically dbc.CardBody or list of components) or None if not available
        """
        # Import here to avoid circular dependencies
        import dash_bootstrap_components as dbc
        from dash import html

        if not self.is_available(benchmark_data):
            return None

        df = self.to_dataframe(benchmark_data)
        if df is None:
            return None

        # Default rendering: simple table with optional title
        components = []

        # Only show title for non-stats groups (stats is the main table)
        if self.display_name:
            components.append(html.H5(self.display_name, className="card-title"))

        components.append(dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True))

        return dbc.CardBody(components)

    def get_plottable_metrics(self, benchmark_data: dict = None) -> List[Metric]:
        """Get metrics from this group that should appear in the trends plot dropdown.

        Args:
            benchmark_data: Optional benchmark data to filter by availability

        Returns:
            List of metrics that can be plotted
        """
        if benchmark_data:
            return self.get_available_metrics(benchmark_data)
        return self.metrics

    def get_error_bar_config(self, metric: Metric) -> Optional[Dict[str, Any]]:
        """Get error bar configuration for a specific metric when plotting.

        Args:
            metric: The metric being plotted

        Returns:
            Dict with:
                - 'metric': The Metric to use for error bars
                - 'type': 'symmetric' or 'asymmetric'
            or None if no error bars
        """
        return None  # Default: no error bars

    def get_action_buttons(self, benchmark_data: dict, run_name: str) -> List[Any]:
        """Get action buttons for this metric group (e.g., "Open Report", "View Flamegraph").

        Args:
            benchmark_data: Raw benchmark data
            run_name: Name of the run (for building URLs)

        Returns:
            List of Dash button components
        """
        return []  # Default: no action buttons
