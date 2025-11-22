import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# Load configuration from environment variables
# ---------------------------------------------------------------------------
GCP_BUCKET = os.getenv("GCP_BUCKET_NAME", "your-gcs-bucket")
GCP_SOURCE_FILE = os.getenv("GCP_SOURCE_FILE", "path/to/file.csv")
BQ_TABLE = os.getenv(
    "BQ_DESTINATION_TABLE",
    "your-project.your_dataset.your_table"
)
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
BQ_CONN_ID = os.getenv("BQ_CONN_ID", "google_cloud_default")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="check_load_csv_to_bigquery",
    default_args=default_args,
    description="Checks for file in GCS then loads CSV into BigQuery",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "gcs", "etl"],
) as dag:


    # Sensor: Check if the file exists in GCS
    # -----------------------------------------------------------------------
    check_file_exists = GCSObjectExistenceSensor(
        task_id="check_file_exists",
        bucket=GCP_BUCKET,
        object=GCP_SOURCE_FILE,
        timeout=300,
        poke_interval=30,
        mode="poke",
        google_cloud_conn_id=GCP_CONN_ID,
    )


    # Load CSV from GCS → BigQuery
    # -----------------------------------------------------------------------
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket=GCP_BUCKET,
        source_objects=[GCP_SOURCE_FILE],
        destination_project_dataset_table=BQ_TABLE,
        source_format="CSV",
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        field_delimiter=",",
        autodetect=True,
        google_cloud_storage_conn_id=GCP_CONN_ID,
        bigquery_conn_id=BQ_CONN_ID,
    )

    check_file_exists >> load_csv_to_bigquery
