from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import gzip
import shutil
import os
from datetime import datetime, timedelta

airflow_home = os.environ['AIRFLOW_HOME']
LOCAL_DATA_DIR = f"{airflow_home}/plugins/raw_data"

import stat

def extract_gzip():
    for file in os.listdir(LOCAL_DATA_DIR):
        if file.endswith(".gz"):
            file_path = os.path.join(LOCAL_DATA_DIR, file)
            output_path = file_path.replace(".gz", "")

            try:
                if not os.access(LOCAL_DATA_DIR, os.W_OK):
                    os.chmod(LOCAL_DATA_DIR, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  

                with gzip.open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

                os.remove(file_path)
                os.chmod(output_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

            except PermissionError:
                print(f"Permission denied !: {output_path}")
            except Exception as e:
                print(f"Handling Error {file}: {e}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_gzip',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
    
    extract_gzip = PythonOperator(
        task_id="extract_gzip_file",
        python_callable=extract_gzip,
        execution_timeout=timedelta(minutes=2)
    )
    note_task1 = BashOperator(
        task_id='notified1',
        bash_command=f'echo "EXTRACT GZIP SUCESSFULL" && ls {airflow_home}/plugins/raw_data/'
    )
    gcs_copy_operator = BashOperator(
        task_id="gcs_copy_to_bucket",
        bash_command=f'ls -l {LOCAL_DATA_DIR} && python3 {airflow_home}/plugins/gcs_utils.py'
    )
    note_task2 = BashOperator(
        task_id="notified2",
        bash_command='echo "LOAD JSON FILE TO BUCKET SUCESSFULL"'
    )

    delete_json_files = BashOperator(
        task_id="delete_json_files",
        bash_command=f'rm -rf {LOCAL_DATA_DIR}/*.json && echo "Deleted all JSON files from raw_data"'
    )
extract_gzip >> note_task1 >> gcs_copy_operator >> note_task2 >> delete_json_files
