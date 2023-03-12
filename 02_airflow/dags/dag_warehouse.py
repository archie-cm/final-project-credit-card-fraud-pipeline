from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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
    dag_id='dag_warehouse',
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

# Create a partitioned table from external table
bq_create_partitioned_table_job = BigQueryInsertJobOperator(
    task_id=f"bq_create_webtoon_partitioned_table_task",
    configuration={
        "query": {
            "query": f"""CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.validated_webtoon_insight
                PARTITION BY date
                AS
                SELECT *
                FROM
                (
                    select *, row_number() over(partition by title, date) as row_seq
                    from `{PROJECT_ID}.{BIGQUERY_DATASET}.raw-webtoon-insight`
                )
                WHERE row_seq = 1;
                """,
            "useLegacySql": False,
        }
    },
    dag = dag
)