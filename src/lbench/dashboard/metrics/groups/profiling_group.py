from typing import Optional, List, Any
from pathlib import Path

from lbench.dashboard.metrics.metric_group import MetricGroup


class ProfilingGroup(MetricGroup):
    """Profiling metric group for flamegraphs and other profiling outputs.

    This group doesn't have metrics, but provides action buttons.
    """

    def __init__(self):
        super().__init__("profiling", "Profiling", [])

    def is_available(self, benchmark_data: dict) -> bool:
        """Check if profiling data is available."""
        extra_info = benchmark_data.get("extra_info", {})
        return "cprofile_path" in extra_info

    def render_card(self, benchmark_data: dict, run_name: str) -> Optional[Any]:
        """Profiling group doesn't render a card, only provides buttons."""
        return None

    def get_action_buttons(self, benchmark_data: dict, run_name: str) -> List[Any]:
        """Return flamegraph button if available."""
        from dash import html

        buttons = []
        extra_info = benchmark_data.get("extra_info", {})

        if "cprofile_path" in extra_info:
            profile_path = extra_info["cprofile_path"]
            profile_name = Path(profile_path).name
            buttons.append(html.A(
                "Open Flamegraph",
                href=f"/flamegraph/{run_name}/{profile_name}",
                target="_blank",
                className="btn btn-outline-secondary mt-2",
                role="button",
            ))

        return buttons

    def get_plottable_metrics(self, benchmark_data: dict = None) -> List:
        """Profiling group has no plottable metrics."""
        return []


profiling_group = ProfilingGroup()
