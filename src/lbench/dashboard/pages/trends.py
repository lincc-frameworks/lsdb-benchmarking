import dash
from dash import html, Input, Output, dcc, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from lbench.dashboard.app import RUN_DATA

dash.register_page(__name__, path='/trends', name='Trends')


# Page layout
def layout():
    benchmarks = sorted(
        {
            bm["fullname"]
            for run in RUN_DATA.values()
            for bm in run.get("benchmarks", [])
        }
    )

    return dbc.Container(
        [
            html.H3("Trends"),
            dcc.Dropdown(
                id="benchmark-selector",
                options=[{"label": b, "value": b} for b in benchmarks],
                placeholder="Select one or more benchmarks",
                multi=True,
            ),
            dcc.Graph(
                id="trend-plot",
                figure={"layout": {"title": "Select a benchmark to view trends"}},
            ),
        ],
        style={"padding": "20px"},
    )


@callback(
    Output("trend-plot", "figure"),
    Input("benchmark-selector", "value"),
)
def update_trend_plot(selected_benchmarks):
    if not selected_benchmarks:
        return {"layout": {"title": "Select one or more benchmarks to view trends"}}

    fig = go.Figure()

    for benchmark in selected_benchmarks:
        rows = []
        for run_name, run in RUN_DATA.items():
            for bm in run.get("benchmarks", []):
                if bm["fullname"] == benchmark:
                    stats = bm.get("stats", {})
                    rows.append(
                        {
                            "run": run_name,
                            "mean": stats.get("mean"),
                            "stddev": stats.get("stddev"),
                        }
                    )
        if not rows:
            continue

        df = pd.DataFrame(rows).sort_values("run")
        df["run"] = pd.to_datetime(df["run"])

        fig.add_trace(
            go.Scatter(
                x=df["run"],
                y=df["mean"],
                mode="lines+markers",
                error_y=dict(type="data", array=df["stddev"], visible=True),
                name=benchmark,
            ),
        )

    fig.update_layout(
        xaxis_title="Run",
        yaxis_title="Mean Time (s)",
        hovermode="x unified",
    )

    return fig
