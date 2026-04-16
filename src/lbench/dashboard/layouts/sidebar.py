import dash
import pandas as pd
from dash import html, Input, Output, State, dcc, callback, no_update
import dash_bootstrap_components as dbc

from lbench.dashboard.context import rename_run
from lbench.dashboard.layouts.tables import benchmarks_to_tables


def filter_runs_by_date(run_data, filter_data):
    if not filter_data:
        return run_data
    start_raw = filter_data.get("start_date")
    end_raw = filter_data.get("end_date")
    if not start_raw and not end_raw:
        return run_data

    start = pd.to_datetime(start_raw) if start_raw else None
    end = pd.to_datetime(end_raw) + pd.Timedelta(days=1) if end_raw else None

    filtered = {}
    for run_id, run_info in run_data.items():
        try:
            ts = pd.to_datetime(run_info.get("datetime"))
            if ts is not None and ts.tzinfo is not None:
                ts = ts.tz_localize(None)
        except Exception:
            ts = None
        if ts is None:
            filtered[run_id] = run_info
            continue
        if start and ts < start:
            continue
        if end and ts >= end:
            continue
        filtered[run_id] = run_info
    return filtered


def create_sidebar(run_data, active_run=None):
    items = []
    for i, r in enumerate(run_data.keys()):
        items.append(
            dbc.ListGroupItem(
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(r, id={"type": "run-item-text", "index": i}, n_clicks=0),
                            width=10,
                            style={"padding": "8px 0", "cursor": "pointer"},
                        ),
                        dbc.Col(
                            html.Button(
                                html.I(className="bi bi-pencil"),
                                id={"type": "run-edit-btn", "index": i},
                                className="btn btn-link btn-sm",
                                style={"padding": "4px 8px", "color": "#666"},
                            ),
                            width=2,
                            style={"padding": "0", "textAlign": "right"},
                        ),
                    ],
                    className="g-0",
                    style={"alignItems": "center"},
                ),
                active=(r == active_run),
                style={"padding": "4px 12px"},
            )
        )
    return dbc.ListGroup(items, id="run-list")


def rename_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader("Rename Run"),
            dbc.ModalBody(
                [
                    dbc.Label("New name:"),
                    dbc.Input(id="rename-input", type="text", placeholder="Enter new name"),
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
    )


def sidebar_panel():
    return dbc.Col(
        [
            html.H3("Runs", style={"flexShrink": "0"}),
            html.Div(
                [
                    dcc.DatePickerRange(
                        id="date-range-picker",
                        display_format="YYYY-MM-DD",
                        style={"fontSize": "12px"},
                    ),
                    html.Div(
                        [
                            dbc.Button(
                                "Apply", id="apply-filter-btn", color="primary", size="sm", className="me-1"
                            ),
                            dbc.Button("Clear", id="clear-filter-btn", color="secondary", size="sm"),
                            dbc.Button(
                                "Plot series",
                                id="plot-range-btn",
                                color="success",
                                size="sm",
                                style={"marginLeft": "auto"},
                            ),
                        ],
                        style={"marginTop": "10px", "display": "flex"},
                    ),
                ],
                style={
                    "borderTop": "1px solid #ccc",
                    "padding": "1em 0",
                    "flexShrink": "0",
                    "position": "relative",
                    "zIndex": 10,
                },
            ),
            html.Div(
                id="sidebar-container",
                children=create_sidebar({}),
                style={"overflowY": "auto", "flex": "1", "minHeight": "0"},
            ),
        ],
        width=3,
        style={
            "borderRight": "1px solid #ccc",
            "height": "100%",
            "display": "flex",
            "flexDirection": "column",
            "padding": "20px 12px",
        },
    )


# --- Date filter ---


@callback(
    Output("date-filter-store", "data"),
    Input("apply-filter-btn", "n_clicks"),
    Input("clear-filter-btn", "n_clicks"),
    State("date-range-picker", "start_date"),
    State("date-range-picker", "end_date"),
    prevent_initial_call=True,
)
def handle_date_filter(apply_clicks, clear_clicks, start_date, end_date):
    triggered_id = dash.ctx.triggered_id
    if triggered_id == "clear-filter-btn":
        return {}
    if triggered_id == "apply-filter-btn":
        return {"start_date": start_date, "end_date": end_date}
    return no_update


@callback(
    Output("date-range-picker", "start_date"),
    Output("date-range-picker", "end_date"),
    Input("date-filter-store", "data"),
    Input("run-data-store", "data"),
)
def sync_date_picker(date_filter, _run_data):
    if not date_filter:
        return None, None
    return date_filter.get("start_date"), date_filter.get("end_date")


# --- Benchmark tables + sidebar ---


@callback(
    Output("benchmark-tables-container", "children"),
    Output("sidebar-container", "children"),
    Input({"type": "run-item-text", "index": dash.ALL}, "n_clicks"),
    Input("run-data-store", "data"),
    Input("date-filter-store", "data"),
)
def update_benchmarks_and_sidebar(n_clicks_list, run_data, date_filter):
    triggered = dash.ctx.triggered_id

    def placeholder(msg):
        return html.Div(
            msg,
            style={
                "height": "100%",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center",
                "color": "#888",
                "fontSize": "1.1rem",
            },
        )

    if not run_data or not isinstance(run_data, dict):
        return placeholder("No run data found"), create_sidebar({})

    filtered_run_data = filter_runs_by_date(run_data, date_filter)

    if not triggered or triggered in ("run-data-store", "date-filter-store"):
        return placeholder("Select a run from the sidebar or plot series"), create_sidebar(filtered_run_data)

    if isinstance(triggered, dict) and triggered.get("type") == "run-item-text":
        idx = triggered.get("index")
        if idx is not None:
            run_name = list(filtered_run_data.keys())[idx]
            return benchmarks_to_tables(run_name, run_data[run_name]), create_sidebar(
                filtered_run_data, active_run=run_name
            )

    return placeholder("Select a run from the sidebar or plot series"), create_sidebar(filtered_run_data)


# --- Panel switching ---


@callback(
    Output("right-panel-view", "data"),
    Input("plot-range-btn", "n_clicks"),
    prevent_initial_call=True,
)
def show_trends(_):
    return "trends"


@callback(
    Output("right-panel-view", "data", allow_duplicate=True),
    Input({"type": "run-item-text", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def show_tables(_):
    return "tables"


# --- Rename ---


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
    triggered_id = dash.ctx.triggered_id

    if not triggered_id:
        return no_update, no_update, no_update, no_update, no_update

    if isinstance(triggered_id, dict) and triggered_id.get("type") == "run-edit-btn":
        idx = triggered_id.get("index")
        if idx is not None and edit_clicks[idx]:
            run_name = list(run_data.keys())[idx]
            return True, run_name, run_name, "", no_update
        return no_update, no_update, no_update, no_update, no_update

    if triggered_id == "rename-cancel-btn" and cancel_clicks:
        return False, "", "", "", no_update

    if triggered_id == "rename-confirm-btn" and confirm_clicks:
        success, message, new_run_data = rename_run(old_name, new_name)
        if success:
            return False, "", "", "", new_run_data
        return no_update, no_update, no_update, message, no_update

    return no_update, no_update, no_update, no_update, no_update
