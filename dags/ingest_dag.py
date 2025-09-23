from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import timedelta
import os
from ingest.collect_match import (
    get_summoners_puuid,
    get_matchid_by_puuid,
    run_riot_pipeline,  

)
default_args = {
    "retries": 1, 
    "retry_delay": timedelta(minutes=1),

}
with DAG(

    dag_id="riot_ingest_daily_and_json_to_parquet",
    start_date=pendulum.datetime(2025, 4, 23, tz="UTC"),
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


    task_spark_transform_json_to_parquet = SparkSubmitOperator(
        task_id="spark_transform_json_to_parquet",
        conn_id="spark_conn",
        application="/opt/airflow/jobs/spark_transform_player.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        application_args=[

            "--input", "s3a://my-riot-ml-pipeline-project/raw/match/ds={{ds}}/*.json",
            "--output", "s3a://my-riot-ml-pipeline-project/staging/match_player_stats",
            "--ds", "{{ ds }}"
        ],
         conf={
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }
)



(
    task_get_puuids >>
    task_get_matchid_by_puuid >>
    task_run_riot_pipeline >> 
    task_spark_transform_json_to_parquet
)