from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc

from lbench.dashboard.context import load_all_runs, ROOT_DIR
from lbench.dashboard.layouts.sidebar import sidebar_panel, rename_modal
from lbench.dashboard.layouts.tables import tables_panel
from lbench.dashboard.layouts.trends import trends_panel


def _navbar():
    return dbc.NavbarSimple(
        brand="lbench Dashboard",
        brand_href="/",
        brand_style={"paddingLeft": "1em"},
        color="primary",
        dark=True,
        sticky="top",
        fluid=True,
    )


def _container():
    return dbc.Container(
        [
            dcc.Location(id="url", refresh=False),
            dcc.Store(id="date-filter-store", data={}),
            dcc.Store(id="run-data-store", data={}),
            dcc.Store(id="rename-old-name", data=""),
            dcc.Store(id="right-panel-view", data="tables"),
            rename_modal(),
            dbc.Row(
                [
                    sidebar_panel(),
                    dbc.Col(
                        [tables_panel(), trends_panel()],
                        width=9,
                        style={"height": "100%", "padding": "0"},
                    ),
                ],
                style={"flex": "1", "minHeight": "0", "margin": "0"},
            ),
        ],
        fluid=True,
        style={
            "flex": "1",
            "overflow": "hidden",
            "paddingLeft": "1em",
            "paddingRight": "1em",
            "paddingTop": "0",
            "paddingBottom": "0",
            "display": "flex",
            "flexDirection": "column",
        },
    )


layout = html.Div(
    [_navbar(), _container()],
    style={"height": "100vh", "overflow": "hidden", "display": "flex", "flexDirection": "column"},
)


@callback(
    Output("run-data-store", "data", allow_duplicate=True),
    Input("url", "pathname"),
    prevent_initial_call="initial_duplicate",
)
def reload_on_page_load(_pathname):
    return load_all_runs(ROOT_DIR)


@callback(
    Output("tables-view", "style"),
    Output("trends-view", "style"),
    Input("right-panel-view", "data"),
)
def toggle_right_panel(view):
    tables_style = {"padding": "20px", "overflowY": "auto", "height": "100%"}
    trends_style = {"padding": "20px", "height": "100%", "display": "flex", "flexDirection": "column"}
    if view == "trends":
        return {**tables_style, "display": "none"}, trends_style
    return tables_style, {**trends_style, "display": "none"}
