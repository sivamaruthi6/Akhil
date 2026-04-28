from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Config
PROJECT_ID = "mounika2295"
DATASET = "Student_123"
TABLE = "Teacher1234"
BUCKET = "nani2"
SOURCE_FILE = gs://nani2/mk.csv

default_args = {
    "start_date": datetime(2026, 27, 12),
}
""" Hi Hello """


with DAG(
    dag_id="gcs_to_bigquery_dag",
    schedule_interval="@daily",   # change as needed
    catchup=False,
    default_args=default_args,
) as dag:

    load_data = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket=BUCKET,
        source_objects=[SOURCE_FILE],
        destination_project_dataset_table=f"{mounika2295}.{Student_123}.{Teacher1234}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",  # or WRITE_APPEND
        autodetect=True,
    )

    load_data
