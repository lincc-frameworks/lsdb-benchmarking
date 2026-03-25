import dash
from dash import html, Input, Output, dcc, callback
import dash_bootstrap_components as dbc
import pandas as pd
from pathlib import Path
from lbench.dashboard.app import RUN_DATA, BENCHMARK_COLLECTION
from lbench.dashboard.metrics import registry

dash.register_page(__name__, path='/', name='Runs')

# --- Helper functions ---
def format_duration(seconds, digits=3):
    """
    Format a duration in seconds using the most appropriate unit.
    Returns (value_str, unit).
    """
    if seconds is None:
        return "-", ""

    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return str(seconds), ""

    if seconds >= 1:
        return f"{seconds:.{digits}f}", "s"
    elif seconds >= 1e-3:
        return f"{seconds * 1e3:.{digits}f}", "ms"
    elif seconds >= 1e-6:
        return f"{seconds * 1e6:.{digits}f}", "µs"
    else:
        return f"{seconds * 1e9:.{digits}f}", "ns"


def benchmark_to_table(bm, run_name):
    stats = bm.get("stats", {})
    # Round values and add units to headings
    min_v, min_u = format_duration(stats.get("min"))
    max_v, max_u = format_duration(stats.get("max"))
    mean_v, mean_u = format_duration(stats.get("mean"))
    std_v, std_u = format_duration(stats.get("stddev"))

    df = pd.DataFrame(
        {
            f"min ({min_u})": [min_v],
            f"max ({max_u})": [max_v],
            f"mean ({mean_u})": [mean_v],
            f"stddev ({std_u})": [std_v],
            "rounds": [stats.get("rounds")],
            "iterations": [stats.get("iterations")],
        }
    )

    dask_table = None
    total_time_table = None
    dask_report_button = None
    flamegraph_button = None

    if "extra_info" in bm and bm["extra_info"]:
        extra = bm["extra_info"]

        if "dask" in extra:
            dask_stats = extra["dask"]
            n_tasks = dask_stats.get("n_tasks")
            keys = [k[0] for k in dask_stats.get("keys", [])]

            times = [
                sum([k["stop"] - k["start"] for k in s])
                for s in dask_stats.get("startstops", [])
            ]

            total_dask_time = sum(times)

            total_time_by_key = {}
            for k, t in zip(keys, times):
                total_time_by_key[k] = total_time_by_key.get(k, 0) + t

            total_time_fmt, total_time_u = format_duration(total_dask_time)

            dask_table = pd.DataFrame(
                {
                    "n_tasks": [n_tasks],
                    f"total dask time ({total_time_u})": [total_time_fmt],
                }
            )

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

            # --- Dask report button ---
            report_path = dask_stats.get("performance_report")
            if report_path:
                report_name = Path(report_path).name
                dask_report_button = html.A(
                    "Open Dask Performance Report",
                    href=f"/file/{run_name}/{report_name}",
                    target="_blank",
                    className="btn btn-outline-primary mt-2",
                    role="button",
                )

        if "cprofile_path" in extra:
            profile_path = extra["cprofile_path"]
            profile_name = Path(profile_path).name
            flamegraph_button = html.A(
                "Open Flamegraph",
                href=f"/flamegraph/{run_name}/{profile_name}",
                target="_blank",
                className="btn btn-outline-secondary mt-2",
                role="button",
            )

    card_children = [
        dbc.CardHeader(bm["fullname"]),
        dbc.CardBody(
            dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True)
        ),
    ]

    buttons = []

    if dask_table is not None:
        card_children.append(
            dbc.CardBody(
                [
                    html.H5("Dask Metrics", className="card-title"),
                    dbc.Table.from_dataframe(
                        dask_table, striped=True, bordered=True, hover=True
                    ),
                ]
            )
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
    if dask_report_button is not None:
        buttons.append(dask_report_button)
    if flamegraph_button is not None:
        buttons.append(flamegraph_button)

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
