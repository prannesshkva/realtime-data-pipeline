from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count

spark = SparkSession.builder \
    .appName("GitHubGold") \
    .getOrCreate()

silver_df = spark.readStream \
    .schema("topic STRING, event_id STRING, repo_name STRING, user_handle STRING, event_time TIMESTAMP") \
    .parquet("data_lake/silver/")

gold_df = silver_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("repo_name")
    ) \
    .agg(count("event_id").alias("total_events"))

query = gold_df.writeStream \
    .format("parquet") \
    .option("path", "data_lake/gold/velocity/") \
    .option("checkpointLocation", "checkpoints/gold/") \
    .outputMode("append") \
    .start()

query.awaitTermination()