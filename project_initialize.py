"""
## Initialize the project for setting up the gcs dataset and bq tables
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
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from NFTPort.nfts_top_selling_fetch import FetchTopSellingNFTs
from gcloud_helper import GoogleHelper
# [END import_module]

# [START define fucntions]
project_id="nft-dashboard-381202"
nfts_schema = [
    {'name': 'chain', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'contract_address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rank', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rank_date', 'type': 'DATE', 'mode': 'NULLABLE'},
]

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

crypto_prices_schema = [
    # TODO: better define the schema and check for possible overflow
    {'name': 'Open', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'High', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Low', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Close', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Adj_Close', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Volumn', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'Time', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Prev_Close', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Simple_Return', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'Log_Return', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
]

collections_to_address = {'Perky_Platypus': '0x793f969bc50a848efd57e5ad177ffa26773e4b14', 
                          'Cryptopunks': '0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB', 
                          'Pudgy_Penguins': '0xBd3531dA5CF5857e7CfAA92426877b022e612cf8', 
                          'Mutant_Alien_Ape_Yacht_Club': '0xc4df6018f90f91bad7e24f89279305715b3a276f'}

tables = {'all_time':'', 'daily': '', 'weekly': '', 'monthly': ''}

def create_gcs_bucket():
    gcs_helper = GoogleHelper()
    gcs_helper.create_bucket("nftport_bucket")
    gcs_helper.create_bucket("nfts_pipeline_test")
# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'project_initialization',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=['Group Project'],
) as dag:
    dag.doc_md = __doc__

    create_gcs_bucket_task = PythonOperator(
        task_id='create_gcs_bucket',
        python_callable=create_gcs_bucket
    )

    with TaskGroup("create_bq_datasets") as create_bq_datasets_task:

        task1 = BigQueryCreateEmptyDatasetOperator(
            task_id='create_bq_dataset_crypto',
            project_id=project_id,
            dataset_id="crypto_pipeline",
            if_exists='log',
            dag=dag
        )

        task2 = BigQueryCreateEmptyDatasetOperator(
            task_id='create_bq_dataset_nfts',
            project_id=project_id,
            dataset_id="nfts_pipeline",
            if_exists='log',
            dag=dag
        )

        task3 = BigQueryCreateEmptyDatasetOperator(
            task_id='create_bq_dataset_nftport',
            project_id=project_id,
            dataset_id="nftport_pipeline",
            if_exists='log',
            dag=dag
        )
        [task1, task2, task3]

    with TaskGroup("create_bq_table_nftport") as create_bq_table_nftport_task:

        task1 = BigQueryCreateEmptyTableOperator(
            task_id='create_bq_table_nftport_weekly',
            dataset_id='nftport_pipeline',
            table_id='nftport_weekly',
            schema_fields=nfts_schema,
            project_id=project_id,
            if_exists='log',
            location='US'
        )

        task2 = BigQueryCreateEmptyTableOperator(
            task_id='create_bq_table_nftport_daily',
            dataset_id='nftport_pipeline',
            table_id='nftport_daily',
            schema_fields=nfts_schema,
            project_id=project_id,
            if_exists='log',
            location='US'
        )

        task3 = BigQueryCreateEmptyTableOperator(
            task_id='create_bq_table_nftport_monthly',
            dataset_id='nftport_pipeline',
            table_id='nftport_monthly',
            schema_fields=nfts_schema,
            project_id=project_id,
            if_exists='log',
            location='US'
        )
        [task1, task2, task3]

    with TaskGroup("create_bq_table_nfts") as create_bq_table_nfts_task:
        create_tasks = []
        for collection, address in collections_to_address.items():
            task = BigQueryCreateEmptyTableOperator(
                task_id=f'create_collection_table_{collection}',
                project_id=project_id,
                dataset_id='nfts_pipeline',
                table_id=f'nfts_pipeline_collection_{collection}',
                if_exists='log',
                schema_fields=nfts_prices_schema,
                dag=dag
            )
            create_tasks.append(task)
        # reduce(lambda x, y: x >> y, tasks)
        # TODO parallerize the task
        create_tasks

    create_crypto_tables_task = BigQueryCreateEmptyTableOperator(
        task_id='create_crypto_tables',
        project_id=project_id,
        dataset_id='crypto_pipeline',
        table_id='crypto_eth_prices',
        schema_fields=crypto_prices_schema,
        if_exists='log',
        dag=dag
    )

create_gcs_bucket_task >> create_bq_datasets_task >> [create_bq_table_nftport_task, create_bq_table_nfts_task, create_crypto_tables_task]
    
   
    
# [END define dag]