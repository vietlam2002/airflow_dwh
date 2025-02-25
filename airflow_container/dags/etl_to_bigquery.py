from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import gzip
import shutil
import os
from datetime import datetime, timedelta
airflow_home = os.environ['AIRFLOW_HOME']
LOCAL_DATA_DIR = f"{airflow_home}/plugins"

import stat

def extract_gzip():
    for file in os.listdir(LOCAL_DATA_DIR):
        if file.endswith(".gz"):
            file_path = os.path.join(LOCAL_DATA_DIR, file)
            output_path = file_path.replace(".gz", "")

            try:
                # Kiểm tra và thay đổi quyền trước khi ghi
                if not os.access(LOCAL_DATA_DIR, os.W_OK):
                    os.chmod(LOCAL_DATA_DIR, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # Cấp quyền đọc, ghi, thực thi

                with gzip.open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

                os.remove(file_path)

                # Đảm bảo file đầu ra có quyền ghi
                os.chmod(output_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

            except PermissionError:
                print(f"⚠️ Không đủ quyền ghi vào: {output_path}")
            except Exception as e:
                print(f"❌ Lỗi khi xử lý {file}: {e}")


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
        python_callable=extract_gzip
    )
    note_task = BashOperator(
        task_id='Notified',
        bash_command='echo "EXTRACT SUCESSFULL"'
    )
extract_gzip >> note_task
