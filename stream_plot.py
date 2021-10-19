import dash
import dash_html_components as dhtml
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table
import pandas as pd
from sqlalchemy import create_engine

import time
import plotly
import logging
from configparser import ConfigParser
from app_log import init_log

""" load config file """
config = ConfigParser()
config.read('config.properties')
sql_config = config['mysql']

log = init_log(log_name="kafka-producer-demo",
               level=logging.INFO,
               formatting=config['logging']['format'].__str__(),
               datefmt=config['logging']['datefmt'].__str__(),
               save_to_file=True)

# Create dash application
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.scripts.config.serve_locally = True


def built_pd_df_from_sql():
    print('starting built_pd_df_from_sql ' + time.strftime('%Y-%m-%d %H:%M:%S'))
    current_refresh_time_temp = None

    mysql_jdbc_url = f"mysql+mysqlconnector://" \
                     f"{sql_config['username']}:{sql_config['password']}@" \
                     f"{sql_config['host']}:{sql_config['port']}/" \
                     f"{sql_config['database']}"

    print(f'jdbc url: {mysql_jdbc_url}')

    db_engine = create_engine(mysql_jdbc_url, echo=False)
    sql_query_for_grid = 'select * from meetup_rsvp_message_agg_detail_tbl ' \
                         'where batch_id in (' \
                         '  select max(batch_id) ' \
                         '  from meetup_rsvp_message_agg_detail_tbl) ' \
                         'order by response_count ' \
                         'desc limit 10'
    df1 = pd.read_sql_query(sql_query_for_grid, db_engine)
    sql_query_for_bar_graph = "select group_name, " \
                              "case when response = 'yes' then " \
                              " response_count else 0 end " \
                              " as yes_response_count " \
                              "case when response = 'no' then " \
                              " response_count else 0 end " \
                              " as no_response_count " \
                              'where batch_id in (' \
                              ' select max(batch_id) ' \
                              ' from meetup_rsvp_message_agg_detail_tbl) ' \
                              'order by response_count ' \
                              'desc limit 10'
    df2 = pd.read_sql_query(sql_query_for_bar_graph, db_engine)

    print(
        'completing built_pd_df_from_sql ' + time.strftime('%Y-%m-%d %H:%M:%S'))

    return {'df1': df1, 'df2': df2}


df1_df1_dictionary_object = built_pd_df_from_sql()
df1 = df1_df1_dictionary_object['df1']
df2 = df1_df1_dictionary_object['df2']
print('for first time')
print(df1.head())
print(df2.head())

# Assign html content to dash application layout
app.layout = dhtml.Div(
    [
        dhtml.H2(
            children="Real-Time Dashboard for Meetup Group's RSVP",
            style={
                "textAlign": "center",
                "color": "#4285F4",
                "font-weight": "bold",
                "font-family": "Verdana", }),
        dhtml.Div(
            id="current_refresh_time",
            children="Current Refresh Time: ",
            style={
                "textAlign": "center",
                "color": "black",
                "font-weight": "bold",
                "fontSize": 10,
                "font-family": "Verdana", }),
        dhtml.Div([
            dhtml.Div([
                dcc.Graph(id='live-update-graph-bar')]),
            dhtml.Div([
                dhtml.Br(),
                dash_table.DataTable(
                    id='datatable-paging',
                    columns=[{'name': i, 'id': i}
                             for i in sorted(['group_country',
                                              'group_state',
                                              'group_city',
                                              'group_lat',
                                              'group_lon',
                                              'response',
                                              'response_count',
                                              'batch_id'])])],
                className='six columns')],
            className='row'),
        dcc.Interval(
            id='interval-component',
            interval=10000,
            n_intervals=0)])


@app.callback(
    Output("current_refresh_time", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_layout(n):
    # current_refresh_time
    global current_refresh_time_temp
    current_refresh_time_temp = time.strftime('%Y-%m-%d %H:%M:%S')
    return "Current Refresh time: {}".format(current_refresh_time_temp)

@app.callback(
    Output("live-update-graph-bar", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph_bar(n):
    traces = list()
    bar_1 = plotly.graph_objs.Bar(
        x=df2['group_name'], y=df2['yes_response_count'], name='Yes')
    traces.append(bar_1)
    bar_2 = plotly.graph_objs.Bar(
        x=df2['group_name'], y=df2['no_response_count'], name='No')
    traces.append(bar_2)
    layout = plotly.graph_objs.Layout(
        barmode='group',
        xaxis_tickangle=-25,
        title_text="Meetup Group's RSVP Count",
        title_font=dict(
            family="Verdana", size=12, color="black"
        ),
    )

    return {'data': traces, 'layout': layout}

@app.callback(
    Output("live-update-graph-bar", "figure"),
    [Input("interval-component", "n_intervals")]
)
