from google.cloud import storage
import json
import pandas as pd
from gcs_utils import GCStorage
from google.oauth2 import service_account
import pathlib

credentials_path = '/opt/airflow/plugins/optical-torch-452002-c8-d798135ff968.json'
project_id = 'optical-torch-452002-c8'
bucket_name = 'nexar_event_bucket'
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Prepare the variables
working_dir = pathlib.Path.cwd()
#files_folder = working_dir.joinpath('raw_data')
files_folder = pathlib.Path('/opt/airflow/plugins/raw_data')

#downloads_folder = working_dir.join_path()

# Construct GCStorage instance
storage_client = storage.Client(credentials=credentials, project=project_id)
gcs = GCStorage(storage_client)

def read_json_from_gcs(file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    # Handle NDJSON
    json_objects = [json.loads(line) for line in content.strip().split("\n")]
    return json_objects

def transform_events():
    all_events = []
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="event_dump_"))

    batch_size = 10
    batches = [blobs[i:i+batch_size] for i in range(0, len(blobs), batch_size)]

    total_batches = len(batches)
    print(f"Total batched: {total_batches}")

    for batch_idx, batch in enumerate(batches):
        print(f"Start hande batch {batch_idx + 1}/{total_batches} have {len(batch)} file...")
        for blob in batch:
            json_data = read_json_from_gcs(blob.name)

            for event in json_data:

                if "user_id" not in event:
                    print(f"Continue...: {event}")
                    continue

                event_id = f"{event['event_timestamp']}-{event['user_id']}"
                all_events.append({
                    "event_id": event_id,
                    "event_date": event["event_date"],
                    "event_timestamp": int(event["event_timestamp"]),
                    "event_name": event["event_name"],
                    "user_id": event["user_id"]
            })
        print(f"Done batch {batch_idx + 1}/{total_batches}!")
    df_events = pd.DataFrame(all_events)
    df_events.to_csv("/opt/airflow/scripts/data/events.csv", index=False)
    print("Create table Event Sucessfull!")
if __name__ == "__main__":
    transform_events()


