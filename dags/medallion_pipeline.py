from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

SPARK_JOBS_DIR = "/opt/airflow/spark/jobs"
SPARK_JARS_DIR = "/opt/airflow/spark"

# Wszystkie JARy na driver classpath — wymagane w local[*] dla JDBC, S3A i Delta Lake.
# Delta JARy są kopiowane z pip package do /opt/airflow/spark/ podczas budowania obrazu
# (patrz Dockerfile.airflow) — bez tego DeltaCatalog nie jest widoczny dla JVM przy starcie.
DRIVER_CLASSPATH = ":".join([
    f"{SPARK_JARS_DIR}/mssql-jdbc-13.2.1.jre11.jar",
    f"{SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{SPARK_JARS_DIR}/delta-spark_2.12-3.0.0.jar",
    f"{SPARK_JARS_DIR}/delta-storage-3.0.0.jar",
])
default_args = {
    "owner": "data_engineer",
    "retries": 2,                        # ile razy retry po błędzie?
    "retry_delay": timedelta(minutes=5), # ile czekać między retry?
}
@dag(
    dag_id="medallion_pipeline",
    description="Medallion ETL pipeline (Bronze → Silver → Gold) orchestrating Spark jobs via Airflow",         
    schedule="@daily",                          # jak często? (hint: "@daily")
    start_date=datetime(2026, 1, 1),
    catchup=False,                         # ważne! wytłumaczę zaraz
    default_args=default_args,
    tags=["bronze", "silver", "gold"],
)
def medallion_pipeline():

    run_bronze = BashOperator(
        task_id="bronze_layer",
        bash_command=f"spark-submit --master local[*] --driver-class-path {DRIVER_CLASSPATH} {SPARK_JOBS_DIR}/bronze_layer.py",
    )

    run_silver = BashOperator(
        task_id="silver_layer",
        bash_command=f"spark-submit --master local[*] --driver-class-path {DRIVER_CLASSPATH} {SPARK_JOBS_DIR}/silver_layer.py",
    )

    run_gold = BashOperator(
        task_id="gold_layer",
        bash_command=f"spark-submit --master local[*] --driver-class-path {DRIVER_CLASSPATH} {SPARK_JOBS_DIR}/gold_layer.py",
    )
    # Kolejność wykonania tasków
    run_bronze >> run_silver >> run_gold
medallion_pipeline()  # ważne! bez tego Airflow nie zarejestruje DAGa