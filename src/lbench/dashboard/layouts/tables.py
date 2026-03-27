import dash_bootstrap_components as dbc
from dash import html
from lbench.dashboard.context import registry

def tables_panel():
    return html.Div(
        id="tables-view",
        children=[html.Div(id="benchmark-tables-container", style={"height": "100%"})],
        style={"padding": "20px", "overflowY": "auto", "height": "100%"},
    )

def benchmark_to_table(bm, run_name):
    card_children = [dbc.CardHeader(bm["fullname"])]

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
    return [benchmark_to_table(bm, run_name) for bm in run_data.get("benchmarks", [])]
