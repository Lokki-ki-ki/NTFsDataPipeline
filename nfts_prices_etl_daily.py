"""
### NFTs Prices ETL DAG (Daily)
This DAG is 
"""
# [START import_module]
from __future__ import annotations
from textwrap import dedent
import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator
# [END import_module]

# [START define fucntions]
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

def check_collection_tables():
    """
    This task checks if the tables for the collections are created
    """
    if True:
        return "create_collection_tables"
    else:
        return "load_collection_one_to_bq"
# [END define fucntions]

# [START define dag]
with DAG(
    # TODO: configuration for the dag
    'nfts_price_etl_daily',
    default_args={'retries': 2},
    description='DAG draft for group project',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = __doc__
    # Fetch data and store in local
    load_data_to_bq_staging_task = GCSToBigQueryOperator(
        task_id='load_to_bq_staging',
        bucket="nfts_pipeline_test",
        source_objects=["data/*.csv"],
        destination_project_dataset_table="nfts_pipeline.nfts_pipeline_staging",
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

    project_id="nft-dashboard-381202"
    dataset="nfts_pipeline"
    table="nfts_pipeline_collection_one"

    # load_collection_two_to_bq_task =
    check_collection_tables_task = BranchPythonOperator(
        task_id='check_collection_tables',
        python_callable=check_collection_tables,
        dag=dag
    )

    load_collection_one_to_bq_task = BigQueryExecuteQueryOperator(
        task_id='load_collection_one_to_bq',
        # TODO: configure the query which can eliminate the duplicates
        use_legacy_sql=False,
        sql=f'''
            MERGE `{project_id}.{dataset}.{table}` T
            USING (
                SELECT collection_address, marketplace, token_id, seller, buyer, price, price_decimal, price_currency, protocol_fee, protocol_decimal, protocol_fee_currency, taker, transaction_hash
                FROM nfts_pipeline.nfts_pipeline_staging
                WHERE collection_address = '0x793f969bc50a848efd57e5ad177ffa26773e4b14'
                GROUP BY collection_address, marketplace, token_id, seller, buyer, price, price_decimal, price_currency, protocol_fee, protocol_decimal, protocol_fee_currency, taker, transaction_hash
            ) S 
            ON T.transaction_hash = S.transaction_hash
            WHEN MATCHED THEN
            UPDATE SET
                price = S.price,
                price_decimal = S.price_decimal
            WHEN NOT MATCHED THEN
            INSERT ROW;
            ''',
        # sql="SELECT * FROM nfts_pipeline.nfts_pipeline_staging WHERE collection_address = '0x793f969bc50a848efd57e5ad177ffa26773e4b14'",
        dag=dag
    )

    create_collection_tables_task = BigQueryCreateEmptyTableOperator(
        task_id='create_collection_tables',
        project_id=project_id,
        dataset_id=dataset,
        table_id=table,
        schema_fields=nfts_prices_schema,
        dag=dag
    )

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

    load_data_to_bq_staging_task >> check_collection_tables_task
    check_collection_tables_task >> create_collection_tables_task >> load_collection_one_to_bq_task >>move_current_data_to_archive_task
    check_collection_tables_task >> load_collection_one_to_bq_task >> move_current_data_to_archive_task
   
    
# [END define dag]