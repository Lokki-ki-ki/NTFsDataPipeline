"""
### NFTs Prices ETL DAG (Daily)
This DAG is 
"""
# [START import_module]
# from __future__ import annotations
from textwrap import dedent
import pendulum
from functools import reduce
import datetime
import os, sys
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from nfts_prices_fetch import FetchData
# [END import_module]

# [START define fucntions]
# TODO: also fetch block_number, block_timestamp
nfts_prices_schema = [
    # TODO: better define the schema and check for possible overflow
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
    lst_collections = list(collections_to_address.values())
    fetch_data.initial_transactions_for_collections(lst_collections)


# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'nfts_price_etl_initialize',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=['Group Project'],
) as dag:
    dag.doc_md = __doc__

    # Step 1
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

    load_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='transform',
        src="/tmp/fetch_transactions_for_collections.csv",
        dst=f"data/fetch_transactions_for_collection_{datetime.datetime.now()}.csv",
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

    # Step 2
    # TODO: create databucket and folder
    load_data_to_bq_staging_task = GCSToBigQueryOperator(
        task_id='load_to_bq_staging',
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

    # Step 2
    project_id="nft-dashboard-381202"
    dataset="nfts_pipeline"
    # table="nfts_pipeline_collection_one"
    tables = []
    with TaskGroup(f"create_collection_tables") as create_collection_tables_task:
        create_tasks = []
        for collection, address in collections_to_address.items():
            task = BigQueryCreateEmptyTableOperator(
                task_id=f'create_collection_table_{collection}',
                project_id=project_id,
                dataset_id=dataset,
                table_id=f'nfts_pipeline_collection_{collection}',
                schema_fields=nfts_prices_schema,
                dag=dag
            )
            tables.append(f'nfts_pipeline_collection_{collection}')
            create_tasks.append(task)
        # reduce(lambda x, y: x >> y, tasks)
        # TODO parallerize the task
        create_tasks


    with TaskGroup("load_collections_to_bq") as load_collections_to_bq_task:
        load_tasks = []
        for i in range(4):
            address = list(collections_to_address.values())[i]
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

    # load_collection_three_to_bq_task =

    # TODO: configure the bucket and path
    # Load data fectched that is stored in local to GCS
    move_current_data_to_archive_task = GCSToGCSOperator(
        task_id='move_current_data_to_archive',
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

    fetch_data_task >> load_to_gcs_task >> load_data_to_bq_staging_task >> create_collection_tables_task >> load_collections_to_bq_task >> move_current_data_to_archive_task
    
   
    
# [END define dag]
