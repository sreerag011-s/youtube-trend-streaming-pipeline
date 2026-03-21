from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "youtube-pipeline",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="youtube_trending_pipeline",
    default_args=default_args,
    description="End-to-end YouTube streaming pipeline",
    schedule_interval="*/10 * * * *",  # every 10 mins
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    bronze = DatabricksSubmitRunOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": "<your-cluster-id>",
            "notebook_task": {
                "notebook_path": "/Workspace/bronze_layer"
            }
        }
    )

    validation = DatabricksSubmitRunOperator(
        task_id="data_validation",
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": "<your-cluster-id>",
            "notebook_task": {
                "notebook_path": "/Workspace/Datavalidation"
            }
        }
    )

    silver = DatabricksSubmitRunOperator(
        task_id="silver_transformation",
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": "<your-cluster-id>",
            "notebook_task": {
                "notebook_path": "/Workspace/silver_layer"
            }
        }
    )

    gold = DatabricksSubmitRunOperator(
        task_id="gold_analytics",
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": "<your-cluster-id>",
            "notebook_task": {
                "notebook_path": "/Workspace/Gold_Layer"
            }
        }
    )

    bronze >> validation >> silver >> gold