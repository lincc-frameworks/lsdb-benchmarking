import dash
from dash import html, Input, Output, State, dcc, callback, no_update
import dash_bootstrap_components as dbc
import pandas as pd
from pathlib import Path
from lbench.dashboard.context import registry, RUN_DATA, rename_run
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
    items = []
    for i, r in enumerate(run_data.keys()):
        # Create item with text that's clickable and separate edit button
        items.append(
            dbc.ListGroupItem(
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                r,
                                id={"type": "run-item-text", "index": i},
                                n_clicks=0,
                            ),
                            width=10,
                            style={"padding": "8px 0", "cursor": "pointer"}
                        ),
                        dbc.Col(
                            html.Button(
                                html.I(className="bi bi-pencil"),
                                id={"type": "run-edit-btn", "index": i},
                                className="btn btn-link btn-sm",
                                style={
                                    "padding": "4px 8px",
                                    "color": "#666"
                                },
                            ),
                            width=2,
                            style={"padding": "0", "textAlign": "right"}
                        ),
                    ],
                    className="g-0",
                    style={"alignItems": "center"}
                ),
                active=(r == active_run),
                style={"padding": "4px 12px"}
            )
        )

    return dbc.ListGroup(items, id="run-list")


# Page layout
layout = dbc.Container(
    [
        dcc.Store(id="run-data-store", data=RUN_DATA),
        dcc.Store(id="rename-old-name", data=""),  # Store old name for rename
        # Rename modal
        dbc.Modal(
            [
                dbc.ModalHeader("Rename Run"),
                dbc.ModalBody(
                    [
                        dbc.Label("New name:"),
                        dbc.Input(
                            id="rename-input",
                            type="text",
                            placeholder="Enter new name",
                        ),
                        html.Div(id="rename-error", className="text-danger mt-2"),
                    ]
                ),
                dbc.ModalFooter(
                    [
                        dbc.Button("Cancel", id="rename-cancel-btn", color="secondary"),
                        dbc.Button("Rename", id="rename-confirm-btn", color="primary"),
                    ]
                ),
            ],
            id="rename-modal",
            is_open=False,
        ),
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
    Input({"type": "run-item-text", "index": dash.ALL}, "n_clicks"),
    Input("run-data-store", "data"),
)
def update_benchmarks_and_sidebar(n_clicks_list, run_data):
    ctx = dash.ctx
    triggered = ctx.triggered_id

    if not run_data or not isinstance(run_data, dict):
        return html.Div("No run data found"), create_sidebar({})

    if not triggered or triggered == "run-data-store":
        # No click yet
        return html.Div("Select a run from the sidebar"), create_sidebar(run_data)

    # Use triggered index directly
    if isinstance(triggered, dict) and triggered.get("type") == "run-item-text":
        idx = triggered.get("index")
        if idx is not None:
            run_name = list(run_data.keys())[idx]
            sidebar = create_sidebar(run_data, active_run=run_name)
            tables = benchmarks_to_tables(run_name, run_data[run_name])
            return tables, sidebar

    return html.Div("Select a run from the sidebar"), create_sidebar(run_data)


# --- Callback to handle rename modal and operations ---
@callback(
    Output("rename-modal", "is_open"),
    Output("rename-input", "value"),
    Output("rename-old-name", "data"),
    Output("rename-error", "children"),
    Output("run-data-store", "data", allow_duplicate=True),
    Input({"type": "run-edit-btn", "index": dash.ALL}, "n_clicks"),
    Input("rename-cancel-btn", "n_clicks"),
    Input("rename-confirm-btn", "n_clicks"),
    State("rename-old-name", "data"),
    State("rename-input", "value"),
    State("run-data-store", "data"),
    prevent_initial_call=True,
)
def handle_rename(edit_clicks, cancel_clicks, confirm_clicks, old_name, new_name, run_data):
    ctx = dash.ctx
    triggered_id = ctx.triggered_id

    # If nothing triggered, don't do anything
    if not triggered_id:
        return no_update, no_update, no_update, no_update, no_update

    # Open modal when edit button clicked
    if isinstance(triggered_id, dict) and triggered_id.get("type") == "run-edit-btn":
        # Find which button was clicked by checking which one was actually clicked
        idx = triggered_id.get("index")
        if idx is not None and edit_clicks[idx]:
            run_name = list(run_data.keys())[idx]
            return True, run_name, run_name, "", no_update
        return no_update, no_update, no_update, no_update, no_update

    # Cancel button - close modal
    if triggered_id == "rename-cancel-btn" and cancel_clicks:
        return False, "", "", "", no_update

    # Confirm button - perform rename
    if triggered_id == "rename-confirm-btn" and confirm_clicks:
        # Perform the rename
        success, message, new_run_data, new_collection = rename_run(old_name, new_name)

        if success:
            # Close modal and update data
            return False, "", "", "", new_run_data
        else:
            # Show error, keep modal open
            return no_update, no_update, no_update, message, no_update

    return no_update, no_update, no_update, no_update, no_update
