# /opt/airflow/jobs/spark_test_script.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AirflowSparkTest").getOrCreate()

    # 간단한 데이터프레임 생성 및 출력
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)

    print("Spark Session is running successfully!")
    df.show()

    spark.stop()