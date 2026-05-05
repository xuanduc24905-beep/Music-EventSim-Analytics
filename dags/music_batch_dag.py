from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "music_batch_pipeline",
    default_args=default_args,
    description="Lambda Architecture - Batch Layer - Music Streaming Analytics",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    tags=["music", "batch", "eventsim", "lambda"],
    catchup=False,
) as dag:

    SPARK        = "docker exec spark-master bash -c"
    SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077"

    # ── Wait for HDFS /music/raw/ to exist ───────────────────
    wait_for_hdfs = BashOperator(
        task_id="wait_for_hdfs_data",
        bash_command=(
            'docker exec namenode bash -c "'
            "for i in \$(seq 1 30); do "
            "  hdfs dfs -test -e /music/raw/ && echo OK && break; "
            "  echo Waiting for /music/raw/...; sleep 10; "
            "done"
            '"'
        ),
    )

    # ── Task 1: Spark EDA ─────────────────────────────────────
    spark_eda = BashOperator(
        task_id="spark_eda",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/01_eda.py"',
    )

    # ── Task 2: Analytics ─────────────────────────────────────
    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/02_analytics.py"',
    )

    # ── Task 3: Hive load (Serving Layer) ────────────────────
    hive_load = BashOperator(
        task_id="hive_load",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/04_hive_load.py"',
    )

    # ── Task 4: Export parquet → /data/ for Streamlit ────────
    export_parquet = BashOperator(
        task_id="export_parquet",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/05_export.py"',
    )

    wait_for_hdfs >> spark_eda >> spark_analytics >> hive_load >> export_parquet
