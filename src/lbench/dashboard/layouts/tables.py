from datetime import timezone

import dash_bootstrap_components as dbc
from dash import html
from lbench.dashboard.context import registry


def tables_panel():
    return html.Div(
        id="tables-view",
        children=[html.Div(id="benchmark-tables-container", style={"height": "100%"})],
        style={"padding": "20px", "overflowY": "auto", "height": "100%"},
    )


def _fmt_run_datetime(dt_str: str) -> str:
    from datetime import datetime
    try:
        dt = datetime.fromisoformat(dt_str)
        local_dt = dt.astimezone().replace(tzinfo=None)
        return local_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt_str


def benchmark_to_table(bm, run_name, run_datetime=None):
    header_content = html.Div(
        [
            html.Span(bm["fullname"]),
            html.Span(
                run_datetime,
                style={"fontSize": "0.8em", "color": "#888", "marginLeft": "1em", "fontWeight": "normal"},
            ) if run_datetime else None,
        ],
        style={"display": "flex", "justifyContent": "space-between", "alignItems": "baseline"},
    )
    card_children = [dbc.CardHeader(header_content)]

    all_groups = registry.list_groups()

    for group in all_groups:
        card_body = group.render_card(bm, run_name)
        if card_body:
            card_children.append(card_body)

    buttons = []
    for group in all_groups:
        buttons.extend(group.get_action_buttons(bm, run_name))

    if buttons:
        card_children.append(
            dbc.CardBody(
                html.Div(buttons, className="btn-group mt-2"),
                className="text-end",
            )
        )

    return dbc.Card(card_children, className="mb-3")


def benchmarks_to_tables(run_name, run_data):
    run_datetime = _fmt_run_datetime(run_data["datetime"]) if "datetime" in run_data else None
    return [benchmark_to_table(bm, run_name, run_datetime) for bm in run_data.get("benchmarks", [])]
