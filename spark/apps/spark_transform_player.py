from pyspark.sql import SparkSession
import argparse
import pyspark.sql.functions as F
#json데이터를 읽어 플레이어별 행으로 펼친 후 parquet으로 저장
def transform_match_data(spark, input_path, output_path, ds):
    print(f"reading json {input_path}")
    
    df = spark.read.option("multiLine", "true").json(input_path)

    select_df = df.select(
        F.col("metadata.matchId").alias("matchId"),
        F.col("info.gameDuration").alias("gameDuration"),
        F.explode(F.col("info.participants")).alias("player_stat"),
        ).withColumn("ds", F.lit(ds)) 

    final_df = select_df.select(
        "matchId", "gameDuration", "player_stat.*", "ds")

    print(f"export to parquet {output_path}")

    final_df.write.partitionBy("ds").mode("overwrite").parquet(output_path)

    print("json to parquet complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True, help="input json path on s3")
    parser.add_argument("--output", required=True, help="output parquet on s3")
    parser.add_argument("--ds", required=True, help="execution date")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("RiotDataTransform").getOrCreate()

    transform_match_data(spark, args.input, args.output, args.ds)

    spark.stop()
