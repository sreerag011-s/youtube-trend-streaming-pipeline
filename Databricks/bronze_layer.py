#Spark Configs

spark.conf.set(
  "fs.azure.account.auth.type.youtubedatalake12.dfs.core.windows.net",
  "OAuth"
)

spark.conf.set(
  "fs.azure.account.oauth.provider.type.youtubedatalake12.dfs.core.windows.net",
  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
  "fs.azure.account.oauth2.client.id.youtubedatalake12.dfs.core.windows.net",
  "<CLIENT_ID>"
)

spark.conf.set(
  "fs.azure.account.oauth2.client.secret.youtubedatalake12.dfs.core.windows.net",
  "<CLIENT_SECRET>"
)

spark.conf.set(
  "fs.azure.account.oauth2.client.endpoint.youtubedatalake12.dfs.core.windows.net",
  "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token"
)
#EventHub conneciton configs

eventhub_namespace = "youtube-streaming-namespace"
eventhub_name = "youtube-streaming-videos"

bootstrap_servers = f"{eventhub_namespace}.servicebus.windows.net:9093"

connection_string = "example_string"

# Kafka configurations

kafka_options = {
 "kafka.bootstrap.servers": bootstrap_servers,
 "subscribe": "youtube-streaming-videos",

 "kafka.security.protocol": "SASL_SSL",
 "kafka.sasl.mechanism": "PLAIN",

 "kafka.sasl.jaas.config":
 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="' + connection_string + '";',

 "kafka.request.timeout.ms": "60000",
 "kafka.session.timeout.ms": "30000",

 "startingOffsets": "earliest"
}

#Read stream from EventHub
raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)
raw_stream.printSchema()

from pyspark.sql.types import *

youtube_schema = StructType([
    StructField("video_id", StringType()),
    StructField("title", StringType()),
    StructField("channel", StringType()),
    StructField("published_at", StringType()),
    StructField("views", IntegerType()),
    StructField("likes", IntegerType()),
    StructField("niche", StringType()),
    StructField("ingestion_time", StringType())
])

# Parsing Json Messages

from pyspark.sql.functions import col, from_json

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), youtube_schema).alias("data")
).select("data.*")

bronze_path = "abfss://bronze@youtubedatalake12.dfs.core.windows.net/youtube_stream/bronze"
checkpoint_path = "abfss://bronze@youtubedatalake12.dfs.core.windows.net/youtube_stream/checkpoints/bronze"

bronze_query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_path)
)
bronze_query.awaitTermination()

# Just for chekccing bronze_table
bronze_df = spark.read.format("delta").load(
"abfss://bronze@youtubedatalake12.dfs.core.windows.net/youtube_stream/bronze"
)

display(bronze_df)

# Creating a reference database to store the data, doesn't create Azure databricks service - Databse, just a logical folder organization in Metadata store - still in ADLS no cost concerns
spark.sql("""
CREATE DATABASE IF NOT EXISTS youtube_analytics
LOCATION 'abfss://bronze@youtubedatalake12.dfs.core.windows.net/youtube_stream/db/'
""")
#Creating the table inside the db to simply query using SQL
spark.sql("""
CREATE TABLE IF NOT EXISTS youtube_analytics.bronze_videos
USING DELTA
LOCATION 'abfss://bronze@youtubedatalake12.dfs.core.windows.net/youtube_stream/bronze'
""")