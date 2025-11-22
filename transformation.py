import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Load configuration from environment variables
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
STAGING_DATASET = os.getenv("BQ_STAGING_DATASET", "staging_dataset")
TRANSFORM_DATASET = os.getenv("BQ_TRANSFORM_DATASET", "transform_dataset")

GCP_BUCKET = os.getenv("GCP_BUCKET_NAME", "your-gcs-bucket")
GCP_SOURCE_FILE = os.getenv("GCP_SOURCE_FILE", "path/to/your.csv")

SOURCE_TABLE = f"{PROJECT_ID}.{STAGING_DATASET}.global_data"

COUNTRIES = os.getenv(
    "COUNTRY_LIST",
    "USA,India,Germany,Japan,France,Canada,Italy"
).split(",")

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
    dag_id="load_and_transform",
    default_args=default_args,
    description="Load CSV file from GCS → BigQuery and build country-wise tables",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "gcs", "etl"],
) as dag:


    # Sensor: Check if file exists in GCS
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


    # Task: Load CSV → BigQuery
    # -----------------------------------------------------------------------
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket=GCP_BUCKET,
        source_objects=[GCP_SOURCE_FILE],
        destination_project_dataset_table=SOURCE_TABLE,
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


    # Create country-specific tables dynamically
    # -----------------------------------------------------------------------
    for country in COUNTRIES:
        BigQueryInsertJobOperator(
            task_id=f"create_table_{country.lower()}",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{PROJECT_ID}.{TRANSFORM_DATASET}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{SOURCE_TABLE}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=BQ_CONN_ID,
        ).set_upstream(load_csv_to_bigquery)

    check_file_exists >> load_csv_to_bigquery
