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
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from NFTPort.nfts_top_selling_fetch import FetchTopSellingNFTs
from gcloud_helper import GoogleHelper
# [END import_module]

# [START define fucntions]
nfts_schema = [
    {'name': 'chain', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'contract_address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rank', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rank_date', 'type': 'DATE', 'mode': 'NULLABLE'},
]

tables = {'all_time':'', 'daily': '', 'weekly': '', 'monthly': ''}

def create_gcs_bucket():
    gcs_helper = GoogleHelper()
    gcs_helper.create_bucket("nftport_bucket")
    gcs_helper.create_bucket()

# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'project_initialize',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=['Group Project'],
) as dag:
    dag.doc_md = __doc__

    create_gcs_bucket_task = PythonOperator(
        task_id='create_gcs_bucket',
        python_callable=create_gcs_bucket
    )

    with TaskGroup("create_bq_table") as create_bq_table_task:

        task = BigQueryCreateEmptyTableOperator(
            task_id='create_bq_table',
            dataset_id='nftport_pipeline',
            table_id='nftport_weekly',
            schema_fields=nfts_schema,
            project_id="nft-dashboard-381202",
            location='US'
        )

create_gcs_bucket_task >> create_bq_datasets_task >> create_bq_table_task
    
   
    
# [END define dag]