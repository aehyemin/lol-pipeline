from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

SINK_PATH = "/opt/bitnami/spark/data/sink/gold_time"
CHECKPOINT_PATH = "/opt/bitnami/spark/data/checkpoints/gold_stream"

schema = StructType([
    StructField("matchId", StringType(), True),
    StructField("frameTimestamp", LongType(),    True),
    StructField("frameMinute", LongType(), True),
    StructField("blueTotalGold", LongType(), True),
    StructField("redTotalGold", LongType() ,True),
    StructField("goldDiff", LongType(), True),
    StructField("eventTs", LongType(), True),

])

def gold_stream():
    spark = SparkSession.builder \
        .appName("lolGoldDiffStream") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:19092") \
        .option("subscribe", "lol.timeline.frames") \
        .option("startingOffsets", "earliest") \
        .load()
    
    parsed_df = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
                        .select("data.*") \
                        .withColumn("event_time", (F.col("eventTs")/1000).cast("timestamp"))
    

    query = parsed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", SINK_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("matchId") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("Spark Streaming start. Waiting data Kafka")
    query.awaitTermination()


if __name__ == "__main__":
    gold_stream()