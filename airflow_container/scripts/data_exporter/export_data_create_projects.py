import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import sys
from datetime import  datetime, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config.yaml')

utils_path = os.path.join(current_dir, '..', 'utils')
sys.path.append(utils_path)
from cache_utils import load_config, connect_to_mongodb
from postgres_utils import clean_dataframe, add_orther_columns, add_column_if_not_exists, add_prrimary_key_and_constraint

# def export_data_to_postgres():
#     config = load_config(config_path)
#     create_config = config['create']
#     print(create_config['POSTGRES_HOST'])

def export_to_postgres(pg_hook, df, schema_name, table_name):
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(
        name=table_name,
        schema_name=schema_name,
        con=engine,
        index = False,
        if_exists='append',
        method='multi'
    )

def export_data(df, schema_name, table_name, primary_key_column_name, execution_date):
    df = add_orther_columns(df, execution_date)
    df = clean_dataframe(df)
    
    pg_hook = PostgresHook(postgres_conn_id='')
    add_column_if_not_exists(pg_hook, df, schema_name, table_name)
    export_to_postgres(pg_hook, df, schema_name, table_name)
    add_prrimary_key_and_constraint(pg_hook, schema_name, table_name, primary_key_column_name)