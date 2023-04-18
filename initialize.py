from google.cloud import bigquery
from google.cloud import storage
import json


class Initialize:
    def __init__(self):
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.location = "US"

    def create_dataset(self, dataset_id):
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.location
        dataset = self.bq_client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(self.bq_client.project, dataset.dataset_id))
    
    def initialize_datasets(self, datasets_to_create):
        print("--Begin to create datasets--")
        datasets_exists = list(map(lambda x: x.dataset_id, self.bq_client.list_datasets()))
        for dataset in datasets_to_create:
            if dataset in datasets_exists:
                print(f"{dataset} already exists")
                continue
            dataset_id = "{}.{}".format(self.bq_client.project, dataset)
            self.create_dataset(dataset_id)
        print("--All datasets created--")

    def create_schema(self, dataset_id, schema_name):
        schema = []
        schema_file = json.load("schema/{}}.json".format(dataset_id))
        schema_lst = schema_file[schema_name]
        for field in schema_lst:
            schema.append(bigquery.SchemaField(field["name"], field["type"], field["mode"]))
        return schema
    
    def create_table(self, dataset_id, table_id, schema_name):
        table = bigquery.Table(dataset_id, table_id)
        table.schema = self.create_schema(dataset_id, schema_name)
        table = self.bq_client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


    def main(self):
        datasets_to_create = ["nfts_dataset", "crypto_dataset"]
        self.initialize_datasets(datasets_to_create)
        

 
if __name__ == "__main__":
    initialize = Initialize()
    # datasets = ["nfts_dataset", "crypto_dataset"]
    # initialize.create_all_datasets()
    # datasets = initialize.bq_client.list_datasets()
    # results = list(map(lambda x: x.dataset_id, datasets))
    # print(datasets)
    