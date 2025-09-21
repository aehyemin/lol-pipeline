from airflow import DAG
from airflow.operators.python import PythonOperator

import pendulum
from datetime import timedelta
import os
from ingest.collect_match import (
    get_summoners_puuid,
    get_matchid_by_puuid,
    run_riot_pipeline,  
    request_riot_api
)
default_args = {
    "retries": 1, 
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}
with DAG(

    dag_id="riot_ingest_daily",
    start_date=pendulum.datetime(2025, 4, 21, tz="UTC"),
    schedule="0 17 * * *",
    catchup=False, 
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args=default_args,
    tags=["ingest", "riot-api", "toS3"],
) as dag:



    task_get_puuids = PythonOperator(
        task_id="get_league_puuid",
        python_callable=get_summoners_puuid,
        op_kwargs={"tier": "challengerleagues"},
        # pool="riot-api-pool",  
    )

    task_get_matchid_by_puuid = PythonOperator(
        task_id="get_matchid_by_puuid",
        python_callable=get_matchid_by_puuid,  
        op_kwargs={
            "puuids": "{{ ti.xcom_pull(task_ids='get_league_puuid') }}",
            "ds": "{{ ds }}",
            "count": "{{ (dag_run.conf.get('count', 5) | int) }}",
            "limit_users": "{{ (dag_run.conf.get('limit_users', 10) | int) }}",
        }
    )

    task_run_riot_pipeline = PythonOperator(
        task_id="run_riot_pipeline",
        python_callable=run_riot_pipeline,
        op_kwargs={"match_ids": "{{ ti.xcom_pull(task_ids='get_matchid_by_puuid') }}"},
        pool="riot-api-pool",
    )

    

    task_get_puuids >> task_get_matchid_by_puuid >> task_run_riot_pipeline