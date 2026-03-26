import dash
from dash import html, Input, Output, dcc, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from lbench.dashboard.app import BENCHMARK_COLLECTION
from lbench.dashboard.context import registry

dash.register_page(__name__, path='/trends', name='Trends')


# Page layout
def layout():
    benchmarks = BENCHMARK_COLLECTION.get_benchmark_names()
    common_metrics = BENCHMARK_COLLECTION.get_common_metrics()

    return dbc.Container(
        [
            html.H3("Trends"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Select Benchmarks:", className="fw-bold"),
                            dcc.Dropdown(
                                id="benchmark-selector",
                                options=[{"label": b, "value": b} for b in benchmarks],
                                placeholder="Select one or more benchmarks",
                                multi=True,
                            ),
                        ],
                        width=6,
                    ),
                    dbc.Col(
                        [
                            html.Label("Select Metric:", className="fw-bold"),
                            dcc.Dropdown(
                                id="metric-selector",
                                options=[
                                    {
                                        "label": f"{m.display_name} ({m.unit})" if m.unit else m.display_name,
                                        "value": m.name,
                                    }
                                    for m in common_metrics
                                ],
                                value="mean",  # Default to mean
                                placeholder="Select a metric",
                            ),
                        ],
                        width=6,
                    ),
                ],
                className="mb-3",
            ),
            dcc.Graph(
                id="trend-plot",
                figure={"layout": {"title": "Select a benchmark and metric to view trends"}},
            ),
        ],
        style={"padding": "20px"},
    )


@callback(
    Output("trend-plot", "figure"),
    Input("benchmark-selector", "value"),
    Input("metric-selector", "value"),
)
def update_trend_plot(selected_benchmarks, selected_metric_name):
    if not selected_benchmarks or not selected_metric_name:
        return {"layout": {"title": "Select one or more benchmarks and a metric to view trends"}}

    # Get the metric object
    metric = registry.get(selected_metric_name)
    if not metric:
        return {"layout": {"title": f"Metric '{selected_metric_name}' not found"}}

    fig = go.Figure()

    # Check if this metric supports error bars
    error_bar_metric = None
    if metric.supports_error_bars():
        error_bar_metric = metric.get_error_bar_metric()

    for benchmark in selected_benchmarks:
        # Use the new metrics system to get data
        df = BENCHMARK_COLLECTION.get_metric_series(benchmark, metric)

        if df.empty:
            continue

        # Prepare trace kwargs
        trace_kwargs = {
            "x": df["timestamp"],
            "y": df["value"],
            "mode": "lines+markers",
            "name": benchmark,
        }

        # Add error bars if available
        if error_bar_metric:
            error_df = BENCHMARK_COLLECTION.get_metric_series(benchmark, error_bar_metric)
            if not error_df.empty:
                # Merge error data with main data
                merged = df.merge(error_df, on=["run_id", "timestamp"], suffixes=("", "_error"))
                if "value_error" in merged.columns:
                    trace_kwargs["error_y"] = dict(
                        type="data",
                        array=merged["value_error"],
                        visible=True
                    )
                    # Update x and y to use merged data
                    trace_kwargs["x"] = merged["timestamp"]
                    trace_kwargs["y"] = merged["value"]

        # Add trace for this benchmark
        fig.add_trace(go.Scatter(**trace_kwargs))

    # Update layout with metric information
    y_axis_label = f"{metric.display_name}"
    if metric.unit:
        y_axis_label += f" ({metric.unit})"

    fig.update_layout(
        xaxis_title="Run",
        yaxis_title=y_axis_label,
        hovermode="x unified",
        title=f"Trends: {metric.display_name}",
    )

    return fig
