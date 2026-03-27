import dash
from lbench.dashboard.layout import layout
import dash_bootstrap_components as dbc

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css"
    ],
)

def run_dashboard(port=8050):
    app.title = "lbench Dashboard"
    app.layout = layout
    app.run(debug=True, port=port)
