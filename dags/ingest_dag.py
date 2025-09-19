from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta

from ingest.collect_match import (
    get_matchid_by_puuid,
    get_summoners_puuid,
    get_match_by_matchid,
    run_riot_pipeline,
)

# default_args = {
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }


with DAG(
    dag_id="riot_ingest_daily",
    start_date= pendulum.datetime(2025,9,16, tz="UTC"),
    schedule="0 17 * * *",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["ingest", "riot-api", "toS3"],
    # default_args = default_args,


) as dag:
    task_get_puuids = PythonOperator(
        task_id='get_league_puuid',
        python_callable=get_summoners_puuid,
        op_kwargs={'tier': 'challengerleagues'}
    )
    task_get_matchid_by_puuid = PythonOperator(
        task_id='get_matchid_by_puuid',
        python_callable=get_matchid_by_puuid,
        op_kwargs={
            'puuids': "{{ ti.xcom_pull(task_ids='get_league_puuid') }}",
            'ds': '{{ ds }}'
        }
    )
    task_run_riot_pipeline = PythonOperator(
        task_id='run_riot_pipeline',
        python_callable=run_riot_pipeline,
        op_kwargs={'match_ids': "{{ ti.xcom_pull(task_ids='get_matchid_by_puuid') }}"}
    )   


    

    task_get_puuids >> task_get_matchid_by_puuid >> task_run_riot_pipeline