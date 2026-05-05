from airflow import DAG
from airflow.operators.bash import BashOperator
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

SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077"
)

with DAG(
    "music_batch_pipeline",
    default_args=default_args,
    description="Lambda Architecture — Batch Layer scheduled pipeline",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    tags=["music", "batch", "lambda"],
    catchup=False,
) as dag:

    wait_for_hdfs = BashOperator(
        task_id="wait_for_hdfs_data",
        bash_command=(
            'docker exec namenode bash -c "'
            "for i in \\$(seq 1 30); do "
            "  hdfs dfs -ls /music/raw/ 2>/dev/null | grep -q parquet && echo OK && break; "
            "  echo Waiting...; sleep 10; "
            "done"
            '"'
        ),
    )

    spark_eda = BashOperator(
        task_id="spark_eda",
        bash_command=f"{SPARK_SUBMIT} /spark-jobs/01_eda.py",
    )

    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=f"{SPARK_SUBMIT} /spark-jobs/02_analytics.py",
    )

    hive_load = BashOperator(
        task_id="hive_load",
        bash_command=f"{SPARK_SUBMIT} /spark-jobs/04_hive_load.py",
    )

    export_parquet = BashOperator(
        task_id="export_parquet",
        bash_command=f"{SPARK_SUBMIT} /spark-jobs/05_export.py",
    )

    wait_for_hdfs >> spark_eda >> spark_analytics >> hive_load >> export_parquet
