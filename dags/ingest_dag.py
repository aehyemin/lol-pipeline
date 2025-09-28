from airflow import DAG
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import timedelta
from role_influence_contrib import compute_role_contrib

from package_sm_code1 import package_and_upload

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


    task_copy_into_snowflake_raw = SnowflakeOperator(
        task_id="copy_into_snowflake_raw",
        snowflake_conn_id="snowflake_conn",
        sql="""
            DELETE FROM RIOT_DB.RAW.MPS_RAW WHERE ds = TO_DATE('{{ ds }}', 'YYYY-MM-DD');

            COPY INTO RIOT_DB.RAW.MPS_RAW (v, ds)
            FROM (
                SELECT 
                $1,
                TO_DATE(
                        SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'ds=', 2), '/', 1),
                        'YYYY-MM-DD'
                    ) AS ds
                FROM @RIOT_DB.STAGING.STG_RIOT/ds={{ ds }}/
            )
            FILE_FORMAT=(FORMAT_NAME='RIOT_DB.STAGING.FF_PARQUET')
            ON_ERROR='ABORT_STATEMENT';
        """
        )
    
    task_package_sm_code = PythonOperator(
        task_id="package_sm_code",
        python_callable=package_and_upload,
        )
    

    SKLEARN_IMAGE = "366743142698.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker-scikit-learn:1.2-1"


    ROLE_ARN      = "arn:aws:iam::174919262157:role/riot-pipeline-role"
    S3_ARTIFACTS  = "s3://my-riot-ml-pipeline-project/sm-artifacts/"
    
    task_sm_train_model = SageMakerTrainingOperator(
        task_id ="sm_train_model",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        check_interval=30,
        config={
            "TrainingJobName": "riot-train-{{ ds_nodash }}",
            "RoleArn": ROLE_ARN,
            "AlgorithmSpecification": {
                "TrainingImage": SKLEARN_IMAGE,
                "TrainingInputMode": "File",
            },
            "OutputDataConfig": {"S3OutputPath": S3_ARTIFACTS},
            "ResourceConfig": {"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 30},
            "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
            "HyperParameters": {
                "sagemaker_program": "sm_train.py",
                "sagemaker_submit_directory": "{{ ti.xcom_pull(task_ids='package_sm_code') or var.value.SM_CODE_URI }}",
                "secret-name": "riot/snowflake/train",
        },
    },
)
    

    task_role_contrib = PythonOperator(
        task_id="compute_role_contrib",
        python_callable=compute_role_contrib,
        op_kwargs={
            "ds": "{{ ds }}",
            
            "model_artifact_s3": "{{ (ti.xcom_pull(task_ids='sm_train_model', include_prior_dates=True) or {}).get('ModelArtifacts', {}).get('S3ModelArtifacts') or (var.value.SM_MODEL_URI | default('', true)) }}",
    
    },
    )



(
    task_get_puuids >>
    task_get_matchid_by_puuid >>
    task_run_riot_pipeline >> 
    task_spark_transform_json_to_parquet >>
    task_copy_into_snowflake_raw >>
    task_package_sm_code >>
    task_sm_train_model >>
    task_role_contrib
    

)