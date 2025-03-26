# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta
# import os

# airflow_home = os.environ['AIRFLOW_HOME']
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 5, 12),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     'sync_data_create_branches',
#     default_args=default_args,
#     schedule='@daily',
#     catchup=False
# ) as dag:
#     # load_data_from_create_branches = BashOperator(
#     #     task_id='load_data',
#     #     #bash_command=f'python3 {airflow_home}/scripts/data_loaders/load_data_from_create_branches.py',
#     #     bash_command='echo $SPARK_HOME'
#     # )

#     install_pymongo = BashOperator(
#         task_id='install_pymongo',
#         bash_command='pip install pymongo'
#     )

#     transform_data = BashOperator(
#         task_id='transform_data',
#         bash_command=f'python3 {airflow_home}/scripts/data_loaders/load_data_from_create_branches.py',
#         # bash_command='echo $JAVA_HOME'
#     )

# # load_data_from_create_branches >> transform_data
# install_pymongo >> transform_data