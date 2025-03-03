from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
import pandas as pd
import json
from datetime import datetime

# Cấu hình
BUCKET_NAME = "nexar_event_bucket"
DATASET_NAME = "test"
TABLE_EVENTS = "events"
TABLE_GEO = "geo"
TABLE_EVENT_PARAMS = "event_params"
GCS_PATH = "event_dump_*.json"  # Đọc nhiều file theo pattern
PROJECT_ID = "optical-torch-452002-c8"

# Khởi tạo DAG
default_args = {"owner": "airflow", "start_date": datetime(2024, 2, 26), "retries": 1}
dag = DAG("etl_gcs_to_bq", default_args=default_args, schedule_interval=None, catchup=False)

def read_json_from_gcs(bucket_name, file_name):
    """ Đọc file NDJSON từ GCS và trả về list """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    return [json.loads(line) for line in content.strip().split("\n")]

def transform_data(**kwargs):
    """ Chuyển đổi dữ liệu JSON thành DataFrame chuẩn hóa """
    ti = kwargs["ti"]
    files = ti.xcom_pull(task_ids="list_gcs_files")  # Lấy danh sách file từ GCS

    all_events, all_geo, all_params = [], [], []

    for file_name in files:
        json_data = read_json_from_gcs(BUCKET_NAME, file_name)

        events_data, geo_data, event_params_data = [], [], []
        
        for event in json_data:
            # Tạo geo_id từ thông tin địa lý
            geo = event.get("geo", {})
            geo_str = f"{geo.get('country', 'NA')}-{geo.get('city', 'NA')}-{geo.get('region', 'NA')}"
            geo_id = abs(hash(geo_str + event["user_id"])) % (10**9)

            geo_data.append({
                "geo_id": geo_id,
                "country": geo.get("country", ""),
                "city": geo.get("city", ""),
                "region": geo.get("region", ""),
                "continent": geo.get("continent", ""),
                "sub_continent": geo.get("sub_continent", ""),
                "metro": geo.get("metro", "")
            })

            # Tạo event_id duy nhất
            event_id = f"{event['event_timestamp']}-{event['user_id']}"

            events_data.append({
                "event_id": event_id,
                "event_date": event["event_date"],
                "event_timestamp": int(event["event_timestamp"]),
                "event_name": event["event_name"],
                "user_id": event["user_id"],
                "geo_id": geo_id
            })

            # Bóc tách event_params
            for param in event.get("event_params", []):
                event_params_data.append({
                    "event_id": event_id,
                    "param_key": param["key"],
                    "param_value": list(param["value"].values())[0]
                })

        all_events.extend(events_data)
        all_geo.extend(geo_data)
        all_params.extend(event_params_data)

    # Convert thành DataFrame
    df_events = pd.DataFrame(all_events)
    df_geo = pd.DataFrame(all_geo).drop_duplicates()  # Loại bỏ dữ liệu trùng lặp
    df_event_params = pd.DataFrame(all_params)

    # Lưu vào file CSV (cần thiết để load vào BQ)
    df_events.to_csv("/tmp/events.csv", index=False)
    df_geo.to_csv("/tmp/geo.csv", index=False)
    df_event_params.to_csv("/tmp/event_params.csv", index=False)

    # Đẩy XCom để sử dụng trong task tiếp theo
    ti.xcom_push(key="events_file", value="/tmp/events.csv")
    ti.xcom_push(key="geo_file", value="/tmp/geo.csv")
    ti.xcom_push(key="params_file", value="/tmp/event_params.csv")

# Tasks
list_gcs_files = GCSListObjectsOperator(
    task_id="list_gcs_files",
    bucket=BUCKET_NAME,
    prefix="event_dump_",
    delimiter=".json",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_events_bq = GCSToBigQueryOperator(
    task_id="load_events_bq",
    bucket=BUCKET_NAME,
    source_objects=["/tmp/events.csv"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_EVENTS}",
    schema_fields=[
        {"name": "event_id", "type": "STRING"},
        {"name": "event_date", "type": "STRING"},
        {"name": "event_timestamp", "type": "INTEGER"},
        {"name": "event_name", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "geo_id", "type": "INTEGER"},
    ],
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    source_format="CSV",
    dag=dag,
)

load_geo_bq = GCSToBigQueryOperator(
    task_id="load_geo_bq",
    bucket=BUCKET_NAME,
    source_objects=["/tmp/geo.csv"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_GEO}",
    schema_fields=[
        {"name": "geo_id", "type": "INTEGER"},
        {"name": "country", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "continent", "type": "STRING"},
        {"name": "sub_continent", "type": "STRING"},
        {"name": "metro", "type": "STRING"},
    ],
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    source_format="CSV",
    dag=dag,
)

load_params_bq = GCSToBigQueryOperator(
    task_id="load_params_bq",
    bucket=BUCKET_NAME,
    source_objects=["/tmp/event_params.csv"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_EVENT_PARAMS}",
    schema_fields=[
        {"name": "event_id", "type": "STRING"},
        {"name": "param_key", "type": "STRING"},
        {"name": "param_value", "type": "STRING"},
    ],
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    source_format="CSV",
    dag=dag,
)

# Flow
list_gcs_files >> transform_task
transform_task >> [load_events_bq, load_geo_bq, load_params_bq]

