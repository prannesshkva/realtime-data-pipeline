from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder \
    .appName("GitHubBronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "gh_.*") \
    .load()

bronze_df = raw_df.selectExpr(
    "CAST(topic AS STRING)",
    "CAST(value AS STRING) as payload",
    "timestamp"
)

query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", "data_lake/bronze/") \
    .option("checkpointLocation", "checkpoints/bronze/") \
    .partitionBy("topic") \
    .start()

query.awaitTermination()