# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import os
# import sys

# # Thêm đường dẫn thư mục scripts vào sys.path
# SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../scripts')
# sys.path.append(SCRIPTS_DIR)

# # Import các hàm từ scripts
# from data_loaders.load_data_from_create_projects import load_data_mongo
# from data_transformer.transform_data_create_projects import transform_data_project
# # from data_exporter.export_data_create_projects import export_data
# # Default arguments cho DAG
# default_args = {
#     'owner': 'vietlam',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 11, 12),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def load_data_task(**kwargs):
#     """Hàm load data và trả về qua XCom"""
#     data = load_data_mongo()  # Giả sử hàm load_data_mongo trả về DataFrame hoặc danh sách
#     return data  # Dữ liệu này sẽ được lưu trong XCom

# def transform_data_task(**kwargs):
#     """Hàm transform data nhận dữ liệu từ XCom"""
#     # Lấy dữ liệu từ Task trước qua XCom
#     ti = kwargs['ti']
#     loaded_data = ti.xcom_pull(task_ids='load_data')  # Pull từ task_id 'load_data'
#     transform_data_project(loaded_data)  # Gọi hàm transform với dữ liệu đầu vào

# # def export_data_task(**kwarg):
# #     ti = kwarg['ti']
# #     transformed_data = ti.xcom_pull(task_ids='transform_data')
# #     # export_data

# with DAG(
#     'sync_data_create_projects',
#     default_args=default_args,
#     description='sync_data_create_projects',
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     # Task 1: Load data từ MongoDB
#     load_data = PythonOperator(
#         task_id='load_data',
#         python_callable=load_data_task,
#         provide_context=True,  # Cho phép truyền context như kwargs
#     )

#     # Task 2: Transform data
#     transform_data = PythonOperator(
#         task_id='transform_data',
#         python_callable=transform_data_task,
#         provide_context=True,
#     )

#     # Định nghĩa Dependency
#     load_data >> transform_data
