from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import os

# Helper functions
def add_other_columns(df, execution_date):
    df['_mage_created_at'] = pd.to_datetime(execution_date)
    df['_mage_updated_at'] = pd.to_datetime(execution_date)
    return df

def clean_dataframe(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
    return df

def add_column_if_not_exists(pg_hook, df, schema_name, table_name):
    column_types = df.dtypes
    for column, dtype in column_types.items():
        _postgres_dype = ''
        if dtype == 'int64':
            _postgres_dype = 'BIGINT'
        elif dtype == 'float64':
            _postgres_dype = 'DOUBLE PRECISION'
        elif dtype == 'object':
            _postgres_dype = 'TEXT'
        elif dtype == 'bool':
            _postgres_dype = 'BOOLEAN'
        elif dtype == 'datetime64[ns]':
            _postgres_dype = 'TIMESTAMP'
        else:
            _postgres_dype = 'TEXT'

        query = f'''
            ALTER TABLE {schema_name}.{table_name}
            ADD COLUMN IF NOT EXISTS "{column}" {_postgres_dype};
        '''
        pg_hook.run(query)
        print(f"Added column {column} to {schema_name}.{table_name}")

def export_to_postgres(pg_hook, df, schema_name, table_name, primary_key_column_name):
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        index=False,
        if_exists='append',
        method='multi'
    )

def add_primary_key_and_constraint(pg_hook, schema_name, table_name, primary_key_column_name):
    query = f'''
        ALTER TABLE {schema_name}.{table_name}
        ADD PRIMARY KEY ("{primary_key_column_name}");
        ALTER TABLE {schema_name}.{table_name}
        ADD CONSTRAINT {table_name}_unique_{primary_key_column_name}
        UNIQUE ("{primary_key_column_name}");
    '''
    pg_hook.run(query)

# Main export function
def export_data_to_postgres(ds, **kwargs):
    # Parameters from Airflow
    schema_name = kwargs['schema_name']
    table_name = kwargs['table_name']
    primary_key_column_name = kwargs['primary_key_column_name']
    execution_date = kwargs['execution_date']

    # DataFrame simulation (replace with your actual data loading logic)
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    }
    df = pd.DataFrame(data)

    # Clean and preprocess the DataFrame
    df = add_other_columns(df, execution_date)
    df = clean_dataframe(df)

    # PostgreSQL Hook
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection')

    # Check if table exists and add columns if necessary
    add_column_if_not_exists(pg_hook, df, schema_name, table_name)

    # Export data
    export_to_postgres(pg_hook, df, schema_name, table_name, primary_key_column_name)

    # Add primary key and constraints if table was created
    add_primary_key_and_constraint(pg_hook, schema_name, table_name, primary_key_column_name)

# Define Airflow DAG
default_args = {
    'owner': 'vietlam',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'export_data_to_postgres_dag',
    default_args=default_args,
    description='Export data to PostgreSQL',
    schedule_interval=None,
    catchup=False,
) as dag:

    export_task = PythonOperator(
        task_id='export_data_to_postgres',
        python_callable=export_data_to_postgres,
        provide_context=True,
        op_kwargs={
            'schema_name': 'your_schema',
            'table_name': 'your_table',
            'primary_key_column_name': 'id',
            'execution_date': '{{ ds }}',
        },
    )
    export_task
