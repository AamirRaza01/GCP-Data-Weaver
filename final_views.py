import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator


# Load configuration via environment variables 
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
STAGING_DATASET = os.getenv("BQ_STAGING_DATASET", "staging_dataset")
TRANSFORM_DATASET = os.getenv("BQ_TRANSFORM_DATASET", "transform_dataset")
REPORTING_DATASET = os.getenv("BQ_REPORTING_DATASET", "reporting_dataset")

GCP_BUCKET = os.getenv("GCP_BUCKET_NAME", "your-gcs-bucket")
GCP_SOURCE_FILE = os.getenv("GCP_SOURCE_FILE", "path/to/file.csv")

COUNTRIES = os.getenv(
    "COUNTRY_LIST",
    "USA,India,Germany,Japan,France,Canada,Italy"
).split(",")

SOURCE_TABLE = f"{PROJECT_ID}.{STAGING_DATASET}.global_data"

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
    dag_id="load_and_transform_view",
    default_args=default_args,
    description="Load CSV → BigQuery, create country tables & reporting views",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "gcs", "views", "etl"],
) as dag:


    # Check if the file exists in GCS
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


    # Load CSV into BigQuery
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


    # Dynamic creation of tables + views per country
    # -----------------------------------------------------------------------
    create_table_tasks = []
    create_view_tasks = []

    for country in COUNTRIES:

        # Task: Create country-specific table
        create_table_task = BigQueryInsertJobOperator(
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
        )

        # Task: Create reporting VIEW for each country table
        create_view_task = BigQueryInsertJobOperator(
            task_id=f"create_view_{country.lower()}",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{PROJECT_ID}.{REPORTING_DATASET}.{country.lower()}_view` AS
                        SELECT
                            `Year` AS year,
                            `Disease Name` AS disease_name,
                            `Disease Category` AS disease_category,
                            `Prevalence Rate` AS prevalence_rate,
                            `Incidence Rate` AS incidence_rate
                        FROM `{PROJECT_ID}.{TRANSFORM_DATASET}.{country.lower()}_table`
                        WHERE `Availability of Vaccines Treatment` = False
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=BQ_CONN_ID,
        )

        # Set task dependencies
        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)

        create_table_tasks.append(create_table_task)
        create_view_tasks.append(create_view_task)


    # Final success task
    # -----------------------------------------------------------------------
    success_task = DummyOperator(task_id="success_task")

    # DAG dependencies
    check_file_exists >> load_csv_to_bigquery
    for table_task, view_task in zip(create_table_tasks, create_view_tasks):
        table_task >> view_task >> success_task
