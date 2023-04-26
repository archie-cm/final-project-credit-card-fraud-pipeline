import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'final_project')

dataset_file = "application_record.csv"
dataset_url = f"https://drive.google.com/uc?export=download&id=1H-2tQsYXkGUWp_Ybaad4zEnsxM_mZtn9"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_local_home = "/opt/airflow"
parquet_file = dataset_file.replace('.csv', '.parquet')

#dbt_loc = "/opt/airflow/dbt"
#spark_master = "spark://spark:7077"

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    return pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

#def cleansing_spark(src_file):
        
    #df = pd.read_csv(src_file)
    #df=df.assign(YEARS_BIRTH = lambda x: (np.floor(np.absolute(x["DAYS_BIRTH"] / 365.25))), \
    #                          YEARS_EMPLOYED=lambda x: np.floor(np.abs(x["DAYS_EMPLOYED"] / 365.25)))\
    #                            .drop(["DAYS_BIRTH", "DAYS_EMPLOYED"], axis=1)
    #df['CODE_GENDER'] = df['CODE_GENDER'].apply(lambda x: 1 if x == 'F' else 0)
    #df['FLAG_OWN_CAR'] = df['FLAG_OWN_CAR'].apply(lambda x: 1 if x == 'Y' else 0)
    #df['FLAG_OWN_REALTY'] = df['FLAG_OWN_REALTY'].apply(lambda x: 1 if x == 'Y' else 0)
    #df.dropna(how='all', inplace=True)

    #df.to_csv("/opt/airflow/application_record.csv", index=False) 

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_final_project",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command="gdrive_connect.sh"
        #bash_command=f"curl -sSL {dataset_url} > '{path_to_local_home}/{dataset_file}'"           # for smaller files
    )

    spark_cleansing_task = BashOperator(
        task_id="spark_cleansing_task",
        bash_command="cd /opt/airflow/spark && python3 spark-cleansing.py"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )
    

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",                       # for parquet
            # "object_name": f"raw/{dataset_file}",                     # for csv
            "local_file": f"{path_to_local_home}/{parquet_file}",       # for parquet file
            # "local_file": f"{path_to_local_home}/{dataset_file}"      # for csv
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                "autodetect": True
            },
        },
    )
    dbt_init_task = BashOperator(
        task_id="dbt_init_task",
        bash_command= "cd /opt/airflow/dbt/credit_card_dwh && dbt deps && dbt seed --profiles-dir ."
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt_task",
        bash_command= "cd /opt/airflow/dbt/credit_card_dwh && dbt deps && dbt run --profiles-dir ."
    )

    download_dataset_task >> spark_cleansing_task >> \
    format_to_parquet_task >> local_to_gcs_task >> \
    bigquery_external_table_task >> dbt_init_task >> run_dbt_task 
