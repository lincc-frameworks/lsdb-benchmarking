from typing import Optional

from lbench.dashboard.metrics import Metric, DurationMetric, MemoryMetric
from lbench.dashboard.metrics.metric_group import MetricGroup


class DaskMetric(Metric):
    """Base class for Dask-related metrics."""

    def get_dask_stats(self, benchmark_data: dict) -> Optional[dict]:
        """Extract dask stats if available."""
        try:
            return benchmark_data.get("extra_info", {}).get("dask")
        except (KeyError, TypeError):
            return None


class DaskTaskCount(DaskMetric):
    """Number of Dask tasks."""

    def __init__(self):
        super().__init__("dask_n_tasks", "Dask Task Count")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                return float(dask_stats.get("n_tasks"))
            except (TypeError, ValueError):
                pass
        return None

    def format_value(self, value: Optional[float]) -> str:
        if value is None:
            return "-"
        return str(int(value))


class DaskTotalTime(DaskMetric, DurationMetric):
    """Total Dask execution time."""

    def __init__(self):
        super().__init__("dask_total_time", "Dask Total Time")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                startstops = dask_stats.get("startstops", [])
                times = [
                    sum([k["stop"] - k["start"] for k in s])
                    for s in startstops
                ]
                return sum(times) if times else None
            except (TypeError, ValueError, KeyError):
                pass
        return None


class DaskPeakMemory(DaskMetric, MemoryMetric):
    """Peak memory during Dask execution."""

    def __init__(self):
        super().__init__("dask_peak_memory", "Dask Peak Memory")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                return dask_stats.get("peak_memory_bytes", None)
            except (TypeError, ValueError):
                pass
        return None


class DaskGraphLength(DaskMetric):
    """Size of dask graph"""

    def __init__(self):
        super().__init__("dask_graph_length", "Dask Graph Length")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                return dask_stats.get("dask_graph_len", None)
            except (TypeError, ValueError):
                pass
        return None


class DaskGraphSize(DaskMetric, MemoryMetric):
    """Memory size of dask graph"""

    def __init__(self):
        super().__init__("dask_graph_size_bytes", "Dask Graph Size")

    def extract(self, benchmark_data: dict) -> Optional[float]:
        dask_stats = self.get_dask_stats(benchmark_data)
        if dask_stats:
            try:
                return dask_stats.get("dask_graph_size_bytes", None)
            except (TypeError, ValueError):
                pass
        return None


class DaskGroup(MetricGroup):
    """Dask metric group with custom rendering for task breakdown and action buttons."""

    def render_card(self, benchmark_data: dict, run_name: str) -> Optional[any]:
        """Render Dask metrics table, task breakdown, and action buttons."""
        import dash_bootstrap_components as dbc
        from dash import html
        import pandas as pd
        from pathlib import Path

        if not self.is_available(benchmark_data):
            return None

        components = []

        # Add main metrics table
        df = self.to_dataframe(benchmark_data)
        if df is not None:
            components.append(html.H5(self.display_name, className="card-title"))
            components.append(dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True))

        # Add task breakdown table
        extra_info = benchmark_data.get("extra_info", {})
        dask_stats = extra_info.get("dask")

        if dask_stats:
            keys = [k[0] for k in dask_stats.get("keys", [])]
            if keys:
                from lbench.dashboard.utils import format_duration

                times = [
                    sum([k["stop"] - k["start"] for k in s])
                    for s in dask_stats.get("startstops", [])
                ]

                total_time_by_key = {}
                for k, t in zip(keys, times):
                    total_time_by_key[k] = total_time_by_key.get(k, 0) + t

                sorted_key_times = sorted(
                    total_time_by_key.items(), key=lambda x: x[1], reverse=True
                )

                formatted_times = [format_duration(t) for _, t in sorted_key_times]

                task_table = pd.DataFrame({
                    "task_key": [k for k, _ in sorted_key_times],
                    "total time": [f"{v} {u}" for v, u in formatted_times],
                })

                components.append(html.H5("Dask Task Times", className="card-title mt-3"))
                components.append(
                    dbc.Table.from_dataframe(task_table, striped=True, bordered=True, hover=True))

        return dbc.CardBody(components) if components else None

    def get_action_buttons(self, benchmark_data: dict, run_name: str):
        """Return Dask performance report button if available."""
        from dash import html
        from pathlib import Path

        buttons = []
        extra_info = benchmark_data.get("extra_info", {})
        dask_stats = extra_info.get("dask")

        if dask_stats:
            report_path = dask_stats.get("performance_report")
            if report_path:
                report_name = Path(report_path).name
                buttons.append(html.A(
                    "Open Dask Performance Report",
                    href=f"/file/{run_name}/{report_name}",
                    target="_blank",
                    className="btn btn-outline-primary mt-2",
                    role="button",
                ))

        return buttons


dask_group = DaskGroup(
    "dask",
    "Dask Metrics",
    [DaskTaskCount(), DaskTotalTime(), DaskPeakMemory(), DaskGraphLength(), DaskGraphSize()]
)
