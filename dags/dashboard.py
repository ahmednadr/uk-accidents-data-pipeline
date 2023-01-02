def dashboard(path):
    import pandas as pd
    import dash
    from dash import html
    from dash import dcc
    import dash_bootstrap_components as dbc
    from dash_bootstrap_templates import load_figure_template
    import plotly.graph_objs as go
    import plotly.express as px
    from dash.dependencies import Input,Output

    load_figure_template('LITERA')
    app = dash.Dash(external_stylesheets=[dbc.themes.LITERA])

    df = pd.read_parquet(path)

    SIDEBAR_STYLE = {
        "position": "fixed",
        "top": 0,
        "left": 0,
        "bottom": 0,
        "width": "24rem",
        "padding": "2rem 1rem",
        "background-color": "darkslategray",
        "color":"white"
    }

    cols = df.columns
    num_cols = list(df._get_numeric_data().columns)
    cat_cols = list(set(cols) - set(num_cols))

    @app.callback(
        Output('x-axis', 'options'),
        Output('y-axis', 'options'),
        Input('plot', 'value'))
    def set_plot_options(selected_plot):
        x = []
        y = []
        if selected_plot == "histogram":
            x,y = cat_cols , num_cols
        elif selected_plot == "scatter":
            x = y = num_cols
        elif selected_plot ==  "line":
            x,y = cat_cols , num_cols
        return x,y

    @app.callback(
        Output('x-axis', 'value'),
        Input('x-axis', 'options'))
    def set_x_value(available_options):
        return available_options[2]

    @app.callback(
        Output('y-axis', 'value'),
        Input('y-axis', 'options'))
    def set_y_value(available_options):
        return available_options[2]

    @app.callback(
        Output('display-selected-plot-str', 'children'),
        Input('plot', 'value'),
        Input('x-axis', 'value'),
        Input('y-axis', 'value'))
    def set_display_children(selected_plot, selected_x, selected_y):
        return u'{} plot of x-axis {} and y-axis {}'.format(
            selected_plot, selected_x,selected_y
        )

    @app.callback(
        Output('display-selected-plot', 'figure'),
        Input('plot', 'value'),
        Input('x-axis', 'value'),
        Input('y-axis', 'value'))
    def plot(plot,x_axis,y_axis):
        if plot == "scatter":
            fig = px.scatter(df,x=x_axis,y=y_axis)
            fig.update_traces(marker_color='darkslategray')
            return fig
        elif plot == "histogram":
            fig = px.histogram(df,x=x_axis,y=y_axis)
            fig.update_traces(marker_color='darkslategray')
            return fig
        elif plot == "line":
            fig = px.line(df,x=x_axis,y=y_axis)
            fig.update_traces(marker_color='darkslategray')
            return fig

    sidebar = html.Div(
        [
            html.H2("Filters"),
            html.Hr(),
            dbc.Nav(
                [
                    dcc.Dropdown(["scatter","histogram","line"],id = 'plot',value="scatter",style={"color":"black"}),
                    html.Br(),
                    dcc.Dropdown(id = 'x-axis',style={"color":"black"}),
                    html.Br(),
                    dcc.Dropdown(id = 'y-axis',style={"color":"black"}),
                    html.Br(),
                    html.Div(id='display-selected-plot-str')
                ],
                vertical=True,
                pills=True,
            ),
        ],
        style=SIDEBAR_STYLE,
    )

    app.layout = html.Div(children = [
                    dbc.Row([
                        dbc.Col(),
                        dbc.Col(html.H1('The 1991 accidents report'),width = 8, style = {'margin-left':'15px','margin-top':'25px'})
                        ]),
                    dbc.Row(
                        [dbc.Col(sidebar), 
                        dbc.Col(dcc.Graph(id="display-selected-plot"), width = 8, style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'})
                        ])
        ],
    )

    # if __name__ == '__main__':
    app.run_server(port = 9001, debug=True)