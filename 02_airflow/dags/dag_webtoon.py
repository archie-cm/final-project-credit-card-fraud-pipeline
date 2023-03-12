from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from helpers.line_webtoon_official_scrape import scrape_official_webtoons
from helpers.parquet_helper import format_to_parquet
from helpers.gcs_utils import upload_to_gcs

# DAG

args = {
    'owner': 'Ammar Chalifah',
    'email':['ammar.chalifah@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':10,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='dag_webtoon',
    default_args=args,
    schedule_interval='0 9 * * 0',
    start_date=datetime(2022, 4, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['webtoon'],
    max_active_runs = 1
)

# Variables

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = "line_webtoon_insight"

# Tasks

scrape_webtoon_officials = PythonOperator(
    task_id = 'scrape_webtoon_officials',
    python_callable = scrape_official_webtoons,
    op_kwargs = {
        'output_file': '{{ task.task_id }}_{{ ds }}.csv'
    },
    dag = dag
)

format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet_task",
    python_callable=format_to_parquet,
    op_kwargs={
        "src_file": 'scrape_webtoon_officials_{{ ds }}.csv',
    },
    dag = dag
)

local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": "raw/webtoon_insight/scrape_webtoon_officials_{{ ds }}.parquet",
        "local_file": "scrape_webtoon_officials_{{ ds }}.parquet",
    },
    dag = dag
)

bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    task_id="bigquery_external_table_task",
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": "raw-webtoon-insight",
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET}/raw/webtoon_insight/*.parquet"],
        },
    },
    dag = dag
)

delete_dataset_csv_task = BashOperator(
    task_id="delete_dataset_csv_task",
    bash_command="rm -rf scrape_webtoon_officials_{{ ds }}.csv",
    dag = dag
)

delete_dataset_parquet_task = BashOperator(
    task_id="delete_dataset_parquet_task",
    bash_command="rm -rf scrape_webtoon_officials_{{ ds }}.parquet",
    dag = dag
)

# Dependencies

scrape_webtoon_officials >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task

local_to_gcs_task >> [delete_dataset_csv_task, delete_dataset_parquet_task]