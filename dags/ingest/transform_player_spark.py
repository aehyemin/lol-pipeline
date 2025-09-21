from pyspark.sql import SparkSession
import argparse
import pyspark.sql.functions as F
#json데이터를 읽어 플레이어별 행으로 펼친 후 parquet으로 저장
def transform_match_data(spark, input_path, output_path):
    print(f"reading json {input_path}")
    
    df = spark.read.option("multiLine", "true").json(input_path)

    select_df = df.select(
        F.col("metadata.matchId").alias("matchId"),
        F.col("info.gameDuration").alias("gameDuration"),
        F.explode(F.col("info.participants")).alias("player_stat"),

    )
    final_df = select_df.select("matchId", "gameDuration", "player_stat.*")

    print(f"export to parquet {output_path}")

    final_df.write.mode("overwrite").parquet(output_path)

    print("json to parquet complete")


if __name__ == "__main__":
    parse = argparse.ArgumentParser()

    parse.add_argument("--input", required=True, help="input json path on s3")
    parse.add_argument("--output", required=True, help="output parquet on s3")

    args = parse.parse_args()

    spark = SparkSession.builder.appName("RiotDataTransform").getOrCreate()

    transform_match_data(spark, args.input, args.output)



