from google.cloud import bigquery, bigquery_datatransfer
from google.oauth2 import service_account
class DatasetManager:
    def __init__(self, client):
        self.client = client
    
    def delete_dataset(self, dataset_id):
        self.client.delete_dataset(dataset_id, not_found_ok=True)

    def create_dataset(self, dataset_id, location='US'):
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset_ = self.client.create_dataset(dataset, timeout=30)
        print("Dataset {}.{} created".format(self.client.project, dataset.dataset_id))
        return dataset_
    
    def list_dataset(self, project_id):
        datasets = self.client.list_datasets(project=project_id)
        return [dataset.dataset_id for dataset in datasets]

    def copy_dataset(self, source_project_id, source_dataset_id, destination_project_id, destination_dataset_id, display_name):
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()
        transfer_config = bigquery_datatransfer.TransferConfig(
            destination_dataset_id=destination_dataset_id,
            display_name=display_name,
            data_source_id="cross_region_copy",
            params={
                "source_project_id": source_project_id,
                "source_dataset_id": source_dataset_id,
            },

        )
        transfer_config = transfer_client.create_transfer_config(
            parent=transfer_client.common_project_path(destination_project_id),
            transfer_config=transfer_config,
        )
        print(f"Created transfer config: {transfer_config.name}")


credentials_path = '/home/vietlam/Desktop/My_Repository/apache_airflow_docs/airflow_container/plugins/optical-torch-452002-c8-d798135ff968.json'
project_id = 'optical-torch-452002-c8'
bucket_name = 'nexar_event_bucket'
credentials = service_account.Credentials.from_service_account_file(credentials_path)
# Construct BigQuery Client Object
client = bigquery.Client(credentials=credentials, project=project_id)
dataset_manager = DatasetManager(client)
print("Using Project ID:", client.project)

datasets = list(client.list_datasets())
print("Datasets:", [dataset.dataset_id for dataset in datasets])
#query = "SELECT * FROM `optical-torch-452002-c8.nexar_event.event` "
#query_job = client.query(query)
#results = query_job.result()


