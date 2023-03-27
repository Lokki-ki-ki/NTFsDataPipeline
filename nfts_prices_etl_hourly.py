"""
### NFTs Prices ETL DAG (Hourly)
This DAG is fetch_data_task >> load_to_gcs_task >> load_data_to_bq_task
"""
# [START import_module]
from __future__ import annotations
from textwrap import dedent
import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from helpers.nfts_prices_fetch import FetchData
# [END import_module]

# [START define fucntions]
def fetch_data():
    """
    This task fetch latest 100 transactions data for several collections from the blockchain
    """
    fetch_data = FetchData()
    fetch_data.fetch_transactions_for_collections()

nfts_prices_schema = [
    # TODO: better define the schema
    {'name': 'collection_address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'marketplace', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'token_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'seller', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'buyer', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'price_decimal', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'price_currency', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'protocol_fee', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'protocol_decimal', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'protocol_fee_currency', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'taker', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transaction_hash', 'type': 'STRING', 'mode': 'NULLABLE'}
]
# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'nfts_price_etl_hourly',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
        src="/tmp/fetch_transactions_for_collections.csv",
        dst=f"data/fetch_transactions_for_collection{datetime.datetime.now()}.csv",
        bucket="nfts_pipeline_test",
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
        bucket="nfts_pipeline_test",
        source_objects=[f"data/*.csv"],
        destination_project_dataset_table="nfts_pipeline.nfts_pipeline_hourly",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        schema_fields= nfts_prices_schema,
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