import dash
from dash import html, dcc
import dash_bootstrap_components as dbc


def create_navbar():
    return dbc.NavbarSimple(
        brand="lbench Dashboard",
        brand_href="/",
        color="primary",
        dark=True,
        children=[
            dbc.NavItem(dcc.Link("Runs", href="/", className="nav-link")),
            dbc.NavItem(dcc.Link("Trends", href="/trends", className="nav-link")),
        ],
        sticky="top",
    )

app = dash.Dash(
    __name__, external_stylesheets=[dbc.themes.FLATLY], use_pages=True
)
app.title = "lbench Dashboard"
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        create_navbar(),
        dash.page_container,
    ]
)

def run_dashboard(port=8050):
    app.run(debug=True, port=port)