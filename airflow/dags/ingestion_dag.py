"""
Weather Ingestion DAG
Schedules daily DockerOperator runs of de-ingest container.
Parameterized by {{ ds }} (logical run date) for backfill support.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH")
if not HOST_PROJECT_PATH:
    raise ValueError("HOST_PROJECT_PATH not set in .env") 

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_ingestion_pipeline',
    default_args=default_args,
    description='Scheduled API ingestion to Bronze/Silver lake',
    schedule_interval='@daily',
    catchup=False, 
    max_active_runs=1,
    tags=['portfolio', 'ingestion'],
) as dag:

    run_ingestion_container = DockerOperator(
        task_id='run_ingestion_container',
        image='de-ingest:latest',
        api_version='auto',

        # Remove container after run to avoid dead container buildup
        auto_remove='force',
        
        command='--run-date {{ ds }} --location "Boston" --write-s3',
        docker_url='unix://var/run/docker.sock',

        # Compose network - allows de-ingest to reach de_postgres
        network_mode='e2e-de_default',

        env_file=f"{HOST_PROJECT_PATH}/.env",
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/data", target="/app/data", type="bind")
        ]
    )