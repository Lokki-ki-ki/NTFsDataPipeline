"""
### NFTs Prices ETL DAG (Hourly)
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
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
# Configure for NoModuleFOund error
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from nfts_prices_fetch import FetchData
# [END import_module]

# [START define fucntions]
nfts_prices_schema = [
    # TODO: better define the schema
    # TODO: price will overflow for INTEGER type
    {'name': 'collection_address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'marketplace', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'token_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'seller', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'buyer', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'price_currency', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'price', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'protocol_fee_currency', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'protocol_fee', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transaction_hash', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'block_number', 'type': 'INTEGER', 'mode': 'NULLABLE'}
]

collections_to_address = {'Perky_Platypus': '0x793f969bc50a848efd57e5ad177ffa26773e4b14', 
                          'Cryptopunks': '0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB', 
                          'Pudgy_Penguins': '0xBd3531dA5CF5857e7CfAA92426877b022e612cf8', 
                          'Mutant_Alien_Ape_Yacht_Club': '0xc4df6018f90f91bad7e24f89279305715b3a276f'}

def fetch_data():
    """
    This task fetch latest 100 transactions data for several collections from the blockchain
    """
    fetch_data = FetchData()
    fetch_data.fetch_transactions_for_collections(collections_to_address)
# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'nfts_price_etl_daily',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule_interval='0 * * * *',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=['Group Project'],
) as dag:
    dag.doc_md = __doc__
    # Fetch data and store in local
    fetch_data_task = PythonOperator(
        task_id='fetch_data_and_transform',
        python_callable=fetch_data
    )
    fetch_data_task.doc_md = dedent(
        """\
    #### Fetch_data
    This task fetch latest 100 transactions data for a collection from the blockchain
    """
    )

    project_id="nft-dashboard-381202"
    dataset="nfts_pipeline"
    # table="nfts_pipeline_collection_one"
    tables = [f'nfts_pipeline_collection_{i}' for i in list(collections_to_address.keys())]

    # TODO: configure the bucket and path
    # Load data fectched that is stored in local to GCS
    load_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='load_data_to_gcs',
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

    load_data_to_bq_staging_task = GCSToBigQueryOperator(
        task_id='from_gcs_load_to_bq_staging',
        bucket="nfts_pipeline_test",
        source_objects=["data/*.csv"],
        destination_project_dataset_table="nfts_pipeline.nfts_staging_table",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=nfts_prices_schema,
        dag=dag
    )
    load_data_to_bq_staging_task.doc_md = dedent(
        """\
    #### Load all data under data folder to big query staging table
    This task load all the data fetched and upload to GCS bucket and load to BigQuery
    """
    )

    with TaskGroup("from_staging_load_collections_to_bq") as load_data_to_bq_task:
        load_tasks = []
        for i in range(4):
            address = list(collections_to_address.values())[i]
            print(address)
            task = BigQueryExecuteQueryOperator(
                task_id=f'load_collection_to_bq_{tables[i]}',
                use_legacy_sql=False,
                sql=f'''
                    MERGE `{project_id}.{dataset}.{tables[i]}` T
                    USING (
                        SELECT collection_address, marketplace, token_id, seller, buyer, price_currency, price, protocol_fee_currency, protocol_fee, transaction_hash, block_number
                        FROM nfts_pipeline.nfts_staging_table
                        WHERE collection_address = '{address}'
                        GROUP BY collection_address, marketplace, token_id, seller, buyer, price_currency, price, protocol_fee_currency, protocol_fee, transaction_hash, block_number
                    ) S 
                    ON T.transaction_hash = S.transaction_hash and T.token_id = S.token_id
                    WHEN MATCHED THEN
                    UPDATE SET
                        price = S.price,
                        block_number = S.block_number
                    WHEN NOT MATCHED THEN
                    INSERT ROW;
                    ''',
                # sql="SELECT * FROM nfts_pipeline.nfts_pipeline_staging WHERE collection_address = '0x793f969bc50a848efd57e5ad177ffa26773e4b14'",
                dag=dag
            )
            load_tasks.append(task)
        load_tasks

    move_current_data_to_archive_task = GCSToGCSOperator(
    task_id='move_current_data_gcs_to_archive',
    source_bucket="nfts_pipeline_test",
    source_object="data/*.csv",
    destination_bucket="nfts_pipeline_test",
    destination_object="archive/",
    move_object=True,
    dag=dag
    )
    move_current_data_to_archive_task.doc_md = dedent(
        """\
    #### Move Current Data to Archive
    This task moves the current data to archive folder
    """
    )

    fetch_data_task >> load_to_gcs_task >> load_data_to_bq_staging_task >> load_data_to_bq_task >> move_current_data_to_archive_task
# [END define dag]
