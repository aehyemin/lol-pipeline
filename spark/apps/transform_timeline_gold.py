import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import operator
from functools import reduce

def transform_gold_time(spark, input_path, output_path):
    print(f"reading timeline json data from{input_path}")
    df = spark.read.option("multiLine", "true").json(input_path)

    df_frames = df.select(
        F.col("metadata.matchId").alias("matchId"),
        F.explode("info.frames").alias("frame")
    )
    df_gold = df_frames.select(
        "matchId",
        (F.col("frame.timestamp") / 1000).cast("timestamp").alias("event_time"),
        (F.col("frame.timestamp") / 60000).cast("int").alias("frame_minute"),
        F.col("frame.participantFrames").alias("participantFrames")
    )


    def sum_cols(cols):
    
        return reduce(operator.add, cols)


    blue_list = [F.col("participantFrames")[str(i)]["totalGold"] for i in range(1, 6)]
    red_list  = [F.col("participantFrames")[str(i)]["totalGold"] for i in range(6, 11)]
    blue_gold = sum_cols(blue_list)
    red_gold  = sum_cols(red_list)

    df_final = df_gold.select(
        "matchId",
        "event_time",
        "frame_minute",
        blue_gold.alias("blue_gold_total"),
        red_gold.alias("red_gold_total"),
        (blue_gold - red_gold).alias("gold_diff")
    ).withColumn("match_id", F.col("matchId")) 



    print(f"writing gold diffs to {output_path}")
    (df_final
        .repartition("matchId")
        .write
        .mode("append")
        .partitionBy("matchId")   # 경로 파티션
        .parquet(output_path))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input path")
    parser.add_argument("--output", required=True, help="Output path")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("GoldTimeseriesBatch").getOrCreate()
    transform_gold_time(spark, args.input, args.output)
    spark.stop()