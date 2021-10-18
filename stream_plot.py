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
    sql_query_for_grid = 'select * from meetup_rsvp_message_agg_detail_tbl'
    df1 = pd.read_sql_query(sql_query_for_grid, db_engine)
    sql_query_for_bar_graph = ''
    return {}


df1_df1_dictionary_object = built_pd_df_from_sql()
df1 = df1_df1_dictionary_object['df1']
df2 = df1_df1_dictionary_object['df2']
print('for first time')
print(df1.head())
print(df2.head())

