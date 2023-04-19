"""
### NFTs Top 50 selling contract ETL DAG (All time)
This DAG is fetch_data_task >> load_to_gcs_task >> load_data_to_bq_task
"""
# [START import_module]
from __future__ import annotations
import sys
import os
from textwrap import dedent
import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# Configure for NoModuleFOund error
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from NFTPort.nfts_top_selling_fetch import FetchTopSellingNFTs
# [END import_module]

# Define global variable for current datetime
current_datetime = datetime.datetime.now()

# [START define fucntions]
def fetch_data():
    
    fetch_data = FetchTopSellingNFTs()
    fetch_data.fetch_nfts("all")

nfts_schema = [
    # TODO: better define the schema
    # TODO: price will overflow for INTEGER type
    {'name': 'chain', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'contract_address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'picture', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rank', 'type': 'STRING', 'mode': 'NULLABLE'},
    
]
# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'nfts_top_selling_etl_all_time',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = __doc__
    # Fetch data and store in local
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data
    )
    fetch_data_task.doc_md = dedent(
        """\
    #### Fetch_data
    This task fetch latest 100 transactions data for a collection from the blockchain
    """
    )

    # TODO: configure the bucket and path
    # Load data fectched that is stored in local to GCS
    load_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='transform',
        src="/tmp/fetch_nfts_top.csv",
        dst=f"data/fetch_nfts_top_all{current_datetime}.csv",
        bucket="nftport_bucket",
        mime_type="text/csv",
        dag=dag
    )
    load_to_gcs_task.doc_md = dedent(
        """\
    #### Upload_to_gcs
    This task upload the data fetched to GCS bucket
    """
    )

    # Load all the hourly data currently in GCS to a BigQuery
    load_data_to_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket="nftport_bucket",
        source_objects=[f"data/fetch_nfts_top_all{current_datetime}.csv"],
        destination_project_dataset_table="nftport_pipeline.nftport_all_time",
        source_format="CSV",
        allow_quoted_newlines = True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields= nfts_schema,
        dag=dag
    )
    load_data_to_bq_task.doc_md = dedent(
        """\
    #### Upload_to_gcs
    This task combine all the data fetched and upload to GCS bucket and load to BigQuery
    """
    )

    fetch_data_task >> load_to_gcs_task >> load_data_to_bq_task
# [END define dag]
