"""
### DAG Draft
This DAG is fetch_data>>upload_to_gcs>>inform_admin
"""
from __future__ import annotations
from nfts_prices_fetch import fetch_transactions_for_collection
from textwrap import dedent
import pendulum
import pandas as pd
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# [END import_module]

# [START instantiate_dag]
with DAG(
    'airflow_draft',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='DAG draft for group project',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START main_flow]
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_transactions_for_collection
    )
    fetch_data_task.doc_md = dedent(
        """\
    #### Fetch_data
    This task fetch latest 100 transactions data for a collection from the blockchain
    """
    )

    # TODO: configure the bucket and path
    transform_task = LocalFilesystemToGCSOperator(
        task_id='transform',
        src="/tmp/fetch_transactions_for_collection.csv",
        dst="data/fetch_transactions_for_collection.csv",
        bucket="nfts_pipeline",
        mime_type="text/csv",
        dag=dag
    )
    transform_task.doc_md = dedent(
        """\
    #### Upload_to_gcs
    This task upload the data fetched to GCS
    """
    )

    load_task = GCSToBigQueryOperator(
        task_id='load',
        bucket="nfts_pipeline",
        source_objects=["data/fetch_transactions_for_collection.csv"],
        destination_project_dataset_table="nfts_pipeline.nft_transactions",
        source_format="CSV"
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    fetch_data_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]