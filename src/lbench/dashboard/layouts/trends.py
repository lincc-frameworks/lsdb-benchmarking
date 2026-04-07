import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

from lbench.dashboard.context import registry, BENCHMARK_COLLECTION


def trends_panel():
    return html.Div(
        id="trends-view",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Select benchmarks:", className="fw-bold"),
                            dcc.Dropdown(
                                id="benchmark-selector",
                                options=[{"label": b, "value": b} for b in BENCHMARK_COLLECTION.get_benchmark_names()],
                                placeholder="Select one or more benchmarks",
                                multi=True,
                            ),
                        ],
                        width=6,
                    ),
                    dbc.Col(
                        [
                            html.Label("Select metric:", className="fw-bold"),
                            dcc.Dropdown(
                                id="metric-selector",
                                options=[{"label": m.display_name, "value": m.name} for m in BENCHMARK_COLLECTION.get_common_metrics()],
                                value="mean",
                                placeholder="Select a metric",
                            ),
                        ],
                        width=6,
                    ),
                ],
                className="mb-3",
                style={"flexShrink": "0"},
            ),
            dcc.Graph(
                id="trend-plot",
                figure={"layout": {"title": "Select a benchmark and metric to view trends"}},
                style={"flex": "1", "minHeight": "0"},
            ),
        ],
        style={"display": "none", "padding": "20px", "height": "100%", "flexDirection": "column"},
    )


def _apply_date_filter(df: pd.DataFrame, date_filter: dict) -> pd.DataFrame:
    if not date_filter or df.empty:
        return df
    start_raw = date_filter.get("start_date")
    end_raw = date_filter.get("end_date")
    if not start_raw and not end_raw:
        return df
    timestamps = df["timestamp"].dt.tz_localize(None) if df["timestamp"].dt.tz is not None else df["timestamp"]
    mask = pd.Series(True, index=df.index)
    if start_raw:
        mask &= timestamps >= pd.to_datetime(start_raw)
    if end_raw:
        mask &= timestamps < pd.to_datetime(end_raw) + pd.Timedelta(days=1)
    return df[mask]


@callback(
    Output("trend-plot", "figure"),
    Input("benchmark-selector", "value"),
    Input("metric-selector", "value"),
    Input("date-filter-store", "data"),
)
def update_trend_plot(selected_benchmarks, selected_metric_name, date_filter):
    if not selected_benchmarks or not selected_metric_name:
        return {"layout": {"title": "Select one or more benchmarks and a metric to view trends"}}

    metric = registry.get(selected_metric_name)
    if not metric:
        return {"layout": {"title": f"Metric '{selected_metric_name}' not found"}}

    fig = go.Figure()

    series = {
        b: _apply_date_filter(BENCHMARK_COLLECTION.get_metric_series(b, metric), date_filter)
        for b in selected_benchmarks
    }
    series = {b: df for b, df in series.items() if not df.empty}

    if not series:
        return {"layout": {"title": "No data available for the selected benchmarks and metric"}}

    all_values = pd.concat([df["value"] for df in series.values()])
    scale, plot_unit = metric.get_plot_scale_and_unit(all_values)

    for benchmark, df in series.items():
        trace_kwargs = {
            "x": df["timestamp"],
            "y": df["value"] / scale,
            "mode": "lines+markers",
            "name": benchmark,
        }

        error_bar_config = metric.get_error_bar_config()
        if error_bar_config:
            error_bar_metric = error_bar_config["metric"]
            error_df = _apply_date_filter(
                BENCHMARK_COLLECTION.get_metric_series(benchmark, error_bar_metric), date_filter
            )
            if not error_df.empty:
                merged = df.merge(error_df, on=["run_id", "timestamp"], suffixes=("", "_error"))
                if "value_error" in merged.columns:
                    trace_kwargs["error_y"] = dict(type="data", array=merged["value_error"] / scale, visible=True)
                    trace_kwargs["x"] = merged["timestamp"]
                    trace_kwargs["y"] = merged["value"] / scale

        fig.add_trace(go.Scatter(**trace_kwargs))

    y_axis_label = metric.display_name
    if plot_unit:
        y_axis_label += f" ({plot_unit})"

    fig.update_layout(
        xaxis_title="Run",
        yaxis_title=y_axis_label,
        hovermode="x unified",
        title=f"Trends: {metric.display_name}",
        legend={"orientation": "h", "yanchor": "top", "y": -0.1, "xanchor": "center", "x": 0.5},
    )
    return fig
