from google.cloud import storage
import json
import pandas as pd
from google.oauth2 import service_account
# Khai báo thông tin bucket và file
BUCKET_NAME = "nexar_event_bucket"
FILE_NAME = "event_dump_000000000000.json"
credentials_path = '/opt/airflow/plugins/optical-torch-452002-c8-d798135ff968.json'
project_id = 'optical-torch-452002-c8'
bucket_name = 'nexar_event_bucket'
credentials = service_account.Credentials.from_service_account_file(credentials_path)


def read_json_from_gcs(bucket_name, file_name):
    """ Đọc file JSON từ GCS và trả về dữ liệu dưới dạng list """
    storage_client = storage.Client(credentials=credentials, project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    # HANDLE NDJSON
    json_objects = [json.loads(line) for line in content.strip().split("\n")]

    return json_objects

def transform_data(json_data):
    """ Transform dữ liệu JSON thành DataFrame để kiểm tra """
    
    events_data = []
    geo_data = []
    event_params_data = []
    
    for event in json_data:
        # Tạo geo_id từ thông tin địa lý
        geo = event.get("geo", {})
        geo_str = f"{geo.get('country', '')}-{geo.get('city', '')}-{geo.get('region', '')}"
        geo_id = abs(hash(geo_str)) % (10**9)  # Hash thành INT để JOIN nhanh hơn
        
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
            "event_timestamp": event["event_timestamp"],
            "event_name": event["event_name"],
            "user_id": event["user_id"],
            "geo_id": geo_id
        })
        
        # Bóc tách event_params
        for param in event.get("event_params", []):
            event_params_data.append({
                "event_id": event_id,
                "param_key": param["key"],
                "param_value": list(param["value"].values())[0]  # Lấy giá trị của param
            })
    
    # Convert thành DataFrame để kiểm tra
    df_events = pd.DataFrame(events_data)
    df_geo = pd.DataFrame(geo_data).drop_duplicates()  # Tránh trùng lặp địa lý
    df_event_params = pd.DataFrame(event_params_data)
    
    return df_events, df_geo, df_event_params

# Chạy thử
json_data = read_json_from_gcs(BUCKET_NAME, FILE_NAME)
df_events, df_geo, df_event_params = transform_data(json_data)

# Hiển thị kết quả
print("📌 Events:")
print(df_events.head())

print("\n📌 Geo:")
print(df_geo.head())

print("\n📌 Event Params:")
print(df_event_params.head())

