import dash
from dash import html, Input, Output, dcc, callback
import dash_bootstrap_components as dbc
import pandas as pd
from pathlib import Path
from lbench.dashboard.context import registry, RUN_DATA
from lbench.dashboard.utils import format_duration

dash.register_page(__name__, path='/', name='Runs')

def benchmark_to_table(bm, run_name):
    """Create table cards for a benchmark using metric groups."""
    card_children = [dbc.CardHeader(bm["fullname"])]

    # Get available metric groups and render their tables
    available_groups = registry.get_available_groups(bm)

    for group in available_groups:
        df = group.to_dataframe(bm)
        if df is not None:
            card_children.append(
                dbc.CardBody([
                    html.H5(group.display_name, className="card-title") if group.name != "stats" else None,
                    dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True)
                ])
            )

    # Handle special Dask task breakdown table (not a simple metric)
    buttons = []
    if "extra_info" in bm and bm["extra_info"]:
        extra = bm["extra_info"]

        if "dask" in extra:
            dask_stats = extra["dask"]
            keys = [k[0] for k in dask_stats.get("keys", [])]

            if keys:
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

                total_time_table = pd.DataFrame(
                    {
                        "task_key": [k for k, _ in sorted_key_times],
                        "total time": [f"{v} {u}" for v, u in formatted_times],
                    }
                )

                card_children.append(
                    dbc.CardBody(
                        [
                            html.H5("Dask Task Times", className="card-title"),
                            dbc.Table.from_dataframe(
                                total_time_table, striped=True, bordered=True, hover=True
                            ),
                        ]
                    )
                )

            # --- Dask report button ---
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

        if "cprofile_path" in extra:
            profile_path = extra["cprofile_path"]
            profile_name = Path(profile_path).name
            buttons.append(html.A(
                "Open Flamegraph",
                href=f"/flamegraph/{run_name}/{profile_name}",
                target="_blank",
                className="btn btn-outline-secondary mt-2",
                role="button",
            ))

    if len(buttons) > 0:
        card_children.append(
            dbc.CardBody(
                html.Div(
                    buttons,
                    className="btn-group mt-2",
                ),
                className="text-end",
            )
        )

    return dbc.Card(card_children, className="mb-3")


def benchmarks_to_tables(run_name, run_data):
    return [benchmark_to_table(bm, run_name) for bm in run_data.get("benchmarks", [])]


def create_sidebar(run_data, active_run=None):
    return dbc.ListGroup(
        [
            dbc.ListGroupItem(
                r,
                id={"type": "run-item", "index": i},
                action=True,
                active=(r == active_run),
            )
            for i, r in enumerate(run_data.keys())
        ],
        id="run-list",
    )


# Page layout
layout = dbc.Container(
    [
        dcc.Store(id="run-data-store", data=RUN_DATA),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H3("Runs"),
                        html.Div(
                            id="sidebar-container",
                            children=create_sidebar(RUN_DATA),
                        ),
                    ],
                    width=3,
                    style={
                        "borderRight": "1px solid #ccc",
                        "height": "100vh",
                        "overflowY": "auto",
                    },
                ),
                dbc.Col(
                    [html.Div(id="benchmark-tables-container")],
                    width=9,
                    style={
                        "padding": "20px",
                        "overflowY": "auto",
                        "height": "100vh",
                    },
                ),
            ]
        ),
    ],
    style={"padding": "20px"},
)


# --- Callback to update benchmark tables and highlight selected run ---
@callback(
    Output("benchmark-tables-container", "children"),
    Output("sidebar-container", "children"),
    Input({"type": "run-item", "index": dash.ALL}, "n_clicks"),
    Input("run-data-store", "data"),
)
def update_benchmarks_and_sidebar(n_clicks_list, run_data):
    ctx = dash.ctx
    triggered = (
        ctx.triggered_id
    )  # This is the dict {"type": "run-item", "index": i} or None
    if not run_data or not isinstance(run_data, dict):
        return html.Div("No run data found"), create_sidebar({})
    if not triggered or triggered == "run-data-store":
        # No click yet
        return html.Div("Select a run from the sidebar"), create_sidebar(run_data)
    # Use triggered index directly
    if isinstance(triggered, dict) and "index" in triggered:
        idx = triggered["index"]
        run_name = list(run_data.keys())[idx]
        sidebar = create_sidebar(run_data, active_run=run_name)
        tables = benchmarks_to_tables(run_name, run_data[run_name])
        return tables, sidebar
    return html.Div("Select a run from the sidebar"), create_sidebar(run_data)
