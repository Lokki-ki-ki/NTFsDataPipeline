from google.cloud import storage
from dotenv import load_dotenv
import os
load_dotenv()
credential=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential
project_id=os.environ.get('PROJECT_ID')

class GoogleHelper:
    # TODO: Add to ntfs_prices_initialize and crypto initialize
    def __init__(self):
        self.project_id = project_id
    
    def create_bucket(self, bucket_name):
        storage_client = storage.Client()
        # Check whether bucket exist
        bucket = storage_client.lookup_bucket(bucket_name)
        if bucket:
            print(f"Bucket {bucket.name} already exists.")
            return
        bucket = storage_client.create_bucket(bucket_name, location="asia-southeast1", project=self.project_id)
        print(f"Bucket {bucket.name} created.")


if __name__ == "__main__":
    google_helper = GoogleHelper()
    google_helper.create_bucket("nftport_bucket")