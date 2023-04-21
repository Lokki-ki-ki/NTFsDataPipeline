from google.cloud import storage

class GoogleHelper:
    # TODO: Add to ntfs_prices_initialize and crypto initialize
    def __init__(self):
        self.project_id = "nft-dashboard-381202"
    
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