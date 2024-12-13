from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import sys
from datetime import  datetime, timedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config.yaml')

utils_path = os.path.join(current_dir, '..', 'utils')
sys.path.append(utils_path)
from cache_utils import load_config, connect_to_mongodb, get_postgres_connection
from postgres_utils import clean_dataframe, add_other_columns, add_column_if_not_exists, add_primary_key_and_constraint

# def export_data_to_postgres():
#     config = load_config(config_path)
#     create_config = config['create']
#     print(create_config['POSTGRES_HOST'])

# conn = get_postgres_connection()
# cursor  = conn.cursor()
# query = "select * from public.users"
# cursor.execute(query)

# rows = cursor.fetchall()
# for row in rows:
#     print(row)
# cursor.close()
# conn.close()

def export_to_postgres(connection, df, schema_name, table_name, primary_key_column_name):
    cur = connection.cursor()

    # Tạo schema nếu chưa tồn tại
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    connection.commit()

    # Chuyển đổi dataframe thành list tuple để chèn vào bảng
    data = [tuple(row) for row in df.to_records(index=False)]
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))

    # Chèn dữ liệu vào bảng
    insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
    cur.executemany(insert_query, data)
    connection.commit()
    cur.close()

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

    # PostgreSQL Connection
    connection = get_postgres_connection()

    try:
        # Check if table exists and add columns if necessary
        add_column_if_not_exists(connection, df, schema_name, table_name)

        # Export data
        export_to_postgres(connection, df, schema_name, table_name, primary_key_column_name)

        # Add primary key and constraints if table was created
        add_primary_key_and_constraint(connection, schema_name, table_name, primary_key_column_name)
    finally:
        connection.close()

default_args = {
    'owner': 'vietlam',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG (
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
            'schema_name': 'public',
            'table_name': 'test',
            'primary_key_column_name': 'id',
            'execution_date': '{{ ds }}',
        },
    )
    export_task
