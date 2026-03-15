from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp

spark = SparkSession.builder \
    .appName("GitHubSilver") \
    .getOrCreate()

schema = "id STRING, repo STRING, user STRING, action STRING, ts STRING"

bronze_df = spark.readStream \
    .format("parquet") \
    .schema("topic STRING, payload STRING") \
    .load("data_lake/bronze/")

silver_df = bronze_df \
    .withColumn("data", from_json(col("payload"), schema)) \
    .select(
        "topic",
        col("data.id").alias("event_id"),
        col("data.repo").alias("repo_name"),
        col("data.user").alias("user_handle"),
        to_timestamp(col("data.ts")).alias("event_time")
    )

query = silver_df.writeStream \
    .format("parquet") \
    .option("path", "data_lake/silver/") \
    .option("checkpointLocation", "checkpoints/silver/") \
    .start()

query.awaitTermination()