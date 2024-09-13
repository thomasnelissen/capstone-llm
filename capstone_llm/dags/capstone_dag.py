from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import os

default_args = {
    "owner": "airflow",
    "description": "Capstone ingest and clean",
    "depend_on_past": False,
    "start_date": datetime(2024, 9, 12),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "Capstone",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    dag.doc_md = __doc__
    # ingest_task = DockerOperator(
    #     task_id="capstone_ingest",
    #     image="capstone",
    #     container_name="capstone_ingest",
    #     api_version="auto",
    #     auto_remove=True,
    #     command="python3 -m capstonellm.tasks.ingest -t pyspark",
    #     environment={
    #         "AWS_ACCESS_KEY_ID": "{{ env['AWS_ACCESS_KEY_ID'] }}",
    #         "AWS_SECRET_ACCESS_KEY": "{{ env['AWS_SECRET_ACCESS_KEY']' }}",
    #         "AWS_SESSION_TOKEN": "{{ env['AWS_SESSION_TOKEN'] }}"
    #     },
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="bridge",
    # )

    tags = ["airflow", "apache-spark", "dbt", "docker", "pyspark", "python-polars", "sql"]

    tasks = [
        DockerOperator(
        task_id="capstone_clean_" + tag,
        trigger_rule=TriggerRule.ALL_DONE,
        image="capstone",
        container_name="capstone_clean_" + tag,
        api_version="auto",
        auto_remove=True,
        command="python3 -m capstonellm.tasks.clean -e production -t " + tag,
        environment={
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN")
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge") for tag in tags
    ]

    empty = EmptyOperator(
        task_id="dummy"
    )

    tasks[0] >> tasks[1] >> empty >> tasks[2] >> tasks[3] >> tasks[4] >> tasks[5] >> tasks[6]