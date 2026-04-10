import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

from lbench.dashboard.context import registry, get_collection


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
                                options=[],
                                placeholder="Select one or more benchmarks",
                                multi=True,
                            ),
                        ],
                        width=6,
                    ),
                    dbc.Col(
                        [
                            html.Label("Select metrics:", className="fw-bold"),
                            dcc.Dropdown(
                                id="metric-selector",
                                options=[],
                                value=["mean"],
                                placeholder="Select one or more metrics",
                                multi=True,
                            ),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Chart type:", className="fw-bold"),
                            dbc.RadioItems(
                                id="chart-type-selector",
                                options=[
                                    {"label": "Line", "value": "line"},
                                    {"label": "Bar", "value": "bar"},
                                ],
                                value="line",
                                inline=True,
                            ),
                        ],
                        width=2,
                        className="d-flex flex-column justify-content-start",
                    ),
                ],
                className="mb-3",
                style={"flexShrink": "0"},
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Select runs:", className="fw-bold"),
                            dcc.Dropdown(
                                id="bar-run-selector",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="Select runs to include",
                            ),
                        ],
                        width=12,
                    ),
                ],
                id="bar-run-selector-row",
                className="mb-3",
                style={"display": "none", "flexShrink": "0"},
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
    Output("benchmark-selector", "options"),
    Output("metric-selector", "options"),
    Output("bar-run-selector", "options"),
    Output("bar-run-selector", "value"),
    Input("run-data-store", "data"),
)
def refresh_trend_options(run_data):
    collection = get_collection(run_data)
    benchmark_options = [{"label": b, "value": b} for b in collection.get_benchmark_names()]
    metric_options = [{"label": m.display_name, "value": m.name} for m in collection.get_common_metrics()]
    run_ids = list((run_data or {}).keys())
    run_options = [{"label": r, "value": r} for r in run_ids]
    return benchmark_options, metric_options, run_options, run_ids


@callback(
    Output("bar-run-selector-row", "style"),
    Input("chart-type-selector", "value"),
)
def toggle_run_selector(chart_type):
    if chart_type == "bar":
        return {"flexShrink": "0"}
    return {"display": "none", "flexShrink": "0"}


@callback(
    Output("trend-plot", "figure"),
    Input("benchmark-selector", "value"),
    Input("metric-selector", "value"),
    Input("date-filter-store", "data"),
    Input("chart-type-selector", "value"),
    Input("bar-run-selector", "value"),
    Input("run-data-store", "data"),
)
def update_trend_plot(selected_benchmarks, selected_metric_names, date_filter, chart_type, selected_runs, run_data):
    if not selected_benchmarks or not selected_metric_names:
        return {"layout": {"title": "Select one or more benchmarks and metrics to view trends"}}

    if isinstance(selected_metric_names, str):
        selected_metric_names = [selected_metric_names]

    collection = get_collection(run_data)

    # Build (metric, series, scale, unit) tuples, skipping metrics with no data
    metrics_data = []
    for metric_name in selected_metric_names:
        metric = registry.get(metric_name)
        if not metric:
            continue
        series = {
            b: _apply_date_filter(collection.get_metric_series(b, metric), date_filter)
            for b in selected_benchmarks
        }
        series = {b: df for b, df in series.items() if not df.empty}
        if not series:
            continue
        all_values = pd.concat([df["value"] for df in series.values()])
        scale, unit = metric.get_plot_scale_and_unit(all_values)
        metrics_data.append((metric, series, scale, unit))

    if not metrics_data:
        return {"layout": {"title": "No data available for the selected benchmarks and metrics"}}

    # Assign y-axes: group metrics by unit, up to 2 axes (left/right)
    unit_to_axis: dict[str, int] = {}
    metric_axis: dict[str, int] = {}
    for metric, series, scale, unit in metrics_data:
        if unit not in unit_to_axis:
            unit_to_axis[unit] = min(len(unit_to_axis) + 1, 2)
        metric_axis[metric.name] = unit_to_axis[unit]

    axis_labels: dict[int, list[str]] = {}
    for metric, _, _, unit in metrics_data:
        ax = metric_axis[metric.name]
        label = metric.display_name + (f" ({unit})" if unit else "")
        if ax not in axis_labels:
            axis_labels[ax] = []
        if label not in axis_labels[ax]:
            axis_labels[ax].append(label)

    multi_metric = len(metrics_data) > 1
    fig = go.Figure()

    for metric, series, scale, unit in metrics_data:
        # Bar charts don't support multiple y-axes with grouped bars — always use y1
        ax = metric_axis[metric.name] if chart_type == "line" else 1
        yaxis_ref = "y" if ax == 1 else "y2"

        if chart_type == "bar":
            traces = _make_bar_traces(series, scale, selected_runs, metric, multi_metric, yaxis_ref)
        else:
            traces = _make_line_traces(series, scale, metric, date_filter, multi_metric, yaxis_ref, collection)

        for trace in traces:
            fig.add_trace(trace)

    metric_names_str = " / ".join(m.display_name for m, *_ in metrics_data)
    title_prefix = "Comparison" if chart_type == "bar" else "Trends"

    layout_kwargs = {
        "title": f"{title_prefix}: {metric_names_str}",
        "xaxis_title": "Run",
        "yaxis": {"title": " / ".join(axis_labels.get(1, []))},
        "legend": {"orientation": "h", "yanchor": "top", "y": -0.1, "xanchor": "center", "x": 0.5},
    }

    if chart_type == "line" and 2 in axis_labels:
        layout_kwargs["yaxis2"] = {
            "title": " / ".join(axis_labels[2]),
            "overlaying": "y",
            "side": "right",
        }

    if chart_type == "line":
        layout_kwargs["hovermode"] = "x unified"
    if chart_type == "bar":
        layout_kwargs["barmode"] = "group"

    fig.update_layout(**layout_kwargs)
    return fig


def _make_line_traces(series, scale, metric, date_filter, multi_metric, yaxis_ref, collection):
    traces = []
    for benchmark, df in series.items():
        name = f"{benchmark} ({metric.display_name})" if multi_metric else benchmark
        trace_kwargs = {
            "x": df["timestamp"],
            "y": df["value"] / scale,
            "mode": "lines+markers",
            "name": name,
            "yaxis": yaxis_ref,
        }

        error_bar_config = metric.get_error_bar_config()
        if error_bar_config:
            error_bar_metric = error_bar_config["metric"]
            error_df = _apply_date_filter(
                collection.get_metric_series(benchmark, error_bar_metric), date_filter
            )
            if not error_df.empty:
                merged = df.merge(error_df, on=["run_id", "timestamp"], suffixes=("", "_error"))
                if "value_error" in merged.columns:
                    trace_kwargs["error_y"] = dict(type="data", array=merged["value_error"] / scale, visible=True)
                    trace_kwargs["x"] = merged["timestamp"]
                    trace_kwargs["y"] = merged["value"] / scale

        traces.append(go.Scatter(**trace_kwargs))
    return traces


def _make_bar_traces(series, scale, selected_runs, metric, multi_metric, yaxis_ref):
    run_ids_in_data = set()
    for df in series.values():
        run_ids_in_data.update(df["run_id"].tolist())

    if selected_runs:
        run_ids = [r for r in selected_runs if r in run_ids_in_data]
    else:
        run_ids = sorted(run_ids_in_data)

    if not run_ids:
        return []

    traces = []
    for benchmark, df in series.items():
        name = f"{benchmark} ({metric.display_name})" if multi_metric else benchmark
        df_filtered = df[df["run_id"].isin(run_ids)]
        df_filtered = df_filtered.set_index("run_id").reindex(run_ids).reset_index()
        traces.append(
            go.Bar(
                x=df_filtered["run_id"],
                y=df_filtered["value"] / scale,
                name=name,
                yaxis=yaxis_ref,
            )
        )
    return traces
