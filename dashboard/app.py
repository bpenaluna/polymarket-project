import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
import dash_mantine_components as dmc
from dash.dependencies import Input, Output

app = Dash(__name__)

# app.layout = html.Div([
#     html.H1("BTC Price / Prediction", style={'textAlign':'center'}),
#     dcc.Graph(id="polymarket-price"),
#     dcc.Graph(id="btc-price"),
#     dcc.Interval(id="interval", interval=2_000, n_intervals=0)  # refresh every 10s
# ])

app.layout = dmc.MantineProvider([
    dmc.Container([
        dmc.Title("BTC Price / Prediction Dashboard", c="blue", order=3),
        dmc.SimpleGrid([
            dcc.Graph(id="polymarket-price"),
            dcc.Graph(id="btc-price")
        ], cols={"base": 1, "md": 2})
    ], fluid=True),
    dcc.Interval(id="interval", interval=2_000, n_intervals=0)  # refresh every 2s
])

@app.callback(Output("polymarket-price", "figure"), Input("interval", "n_intervals"))
def update_pm_chart(_):
    con = duckdb.connect()
    df = con.execute("""
        SELECT timestamp, up_outcome, down_outcome
        FROM delta_scan('/app/data/pm_data')
        ORDER BY timestamp
    """).df()
    fig = px.line(df, x="timestamp", y=["up_outcome", "down_outcome"],
                  title="Up/Down Outcome Prices Over Time")
    return fig

@app.callback(Output("btc-price", "figure"), Input("interval", "n_intervals"))
def update_cg_chart(_):
    con = duckdb.connect()
    df = con.execute("""
        SELECT timestamp, usd
        FROM delta_scan('/app/data/cg_data')
        ORDER BY timestamp
    """).df()
    fig = px.line(df, x="timestamp", y="usd", title="Bitcoin Price")
    return fig

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)