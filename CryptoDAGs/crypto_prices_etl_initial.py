"""
### Crypto Prices ETL DAG (Initialize)
This DAG is for initializing the Crypto Prices Database. It will create a dataset and a table in BigQuery.
"""
# [START import_module]
# from __future__ import annotations
import os
import sys
from textwrap import dedent
import pendulum
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from crypto_prices_fetch import FetchData
load_dotenv()
credential=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential
# [END import_module]

# [START define fucntions]
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

def fetch_data():
    fetch_data = FetchData()
    ticker = "ETH-USD"
    file_name = fetch_data.fetch_crypto_prices_by_interval(ticker, 1000)
    return file_name

# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'crypto_prices_etl_initialize',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule='@once',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=False,
    tags=['Group Project'],
) as dag:
    dag.doc_md = __doc__
    # Fetch data and store in local
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        dag=dag
    )
    fetch_data_task.doc_md = dedent(
        """\
    #### Fetch_data
    This task fetch the data from yahoo finance, transform and store in local csv
    """
    )

    # Load data fectched that is stored in local to GCS
    load_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='load_to_gcs',
        src="/tmp/{{ ti.xcom_pull(task_ids='fetch_data') }}",
        dst="crypto/{{ ti.xcom_pull(task_ids='fetch_data') }}",
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

    project_id="nft-dashboard-381202"
    dataset="crypto_pipeline"
    table="crypto_eth_prices"

    create_bq_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        project_id=project_id,
        dataset_id=dataset,
        dag=dag
    )

    load_data_to_bq_staging_task = GCSToBigQueryOperator(
        task_id='load_to_bq_staging',
        bucket="nfts_pipeline_test",
        source_objects=["crypto/{{ ti.xcom_pull(task_ids='fetch_data') }}"],
        destination_project_dataset_table=f"{dataset}.crypto_prices_staging",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=crypto_prices_schema,
        dag=dag
    )
    load_data_to_bq_staging_task.doc_md = dedent(
        """\
    #### Load all data under data folder to big query staging table
    This task load all the data fetched and upload to GCS bucket and load to BigQuery
    """
    )

    # TODO: wont create new table when there is already one(need double check)
    create_crypto_tables_task = BigQueryCreateEmptyTableOperator(
        task_id='create_crypto_tables',
        project_id=project_id,
        dataset_id=dataset,
        table_id=table,
        schema_fields=crypto_prices_schema,
        dag=dag
    )

    load_staging_to_bq_task = BigQueryExecuteQueryOperator(
        task_id='load_staging_to_bq',
        # TODO: configure the query which can eliminate the duplicates
        use_legacy_sql=False,
        sql=f'''
            MERGE `{project_id}.{dataset}.{table}` T
            USING (
                SELECT Open, High, Low, Close, Adj_Close, Volumn, Date, Time, Prev_Close, Simple_Return, Log_Return
                FROM {dataset}.crypto_prices_staging
                GROUP BY Open, High, Low, Close, Adj_Close, Volumn, Date, Time, Prev_Close, Simple_Return, Log_Return
            ) S 
            ON S.Open = T.Open AND S.High = T.High AND S.Low = T.Low AND S.Close = T.Close AND S.Adj_Close = T.Adj_Close AND S.Volumn = T.Volumn AND S.Date = T.Date AND S.Time = T.Time AND S.Prev_Close = T.Prev_Close AND S.Simple_Return = T.Simple_Return AND S.Log_Return = T.Log_Return
            WHEN MATCHED THEN
            UPDATE SET
                Open = S.Open, High = S.High, Low = S.Low, Close = S.Close, Adj_Close = S.Adj_Close, Volumn = S.Volumn
            WHEN NOT MATCHED THEN
            INSERT ROW;
            ''',
        dag=dag
    )

    # load_collection_three_to_bq_task =

    # TODO: configure the bucket and path
    # Load data fectched that is stored in local to GCS
    move_current_data_to_archive_task = GCSToGCSOperator(
        task_id='move_current_data_to_archive',
        source_bucket="nfts_pipeline_test",
        source_object="crypto/{{ ti.xcom_pull(task_ids='fetch_data') }}",
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

    fetch_data_task >> create_bq_dataset_task >> load_data_to_bq_staging_task >> create_crypto_tables_task >> load_staging_to_bq_task >> move_current_data_to_archive_task
    
   