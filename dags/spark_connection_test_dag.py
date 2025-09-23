# /opt/airflow/dags/spark_connection_test_dag.py
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

with DAG(
    dag_id="spark_connection_test_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test", "spark"],
) as dag:
    spark_test_task = SparkSubmitOperator(
        task_id="spark_connection_test_task",
        conn_id="spark_conn",  # 테스트하려는 Connection ID
        application="/opt/airflow/jobs/spark_test_script.py", # 방금 만든 Spark 스크립트 경로
        verbose=True, # 로그를 자세히 보기 위해 True로 설정
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }
    )