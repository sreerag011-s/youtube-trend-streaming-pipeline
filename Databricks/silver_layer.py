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

#=====================================
#Storage configuration
#=====================================

storage_account = "youtubedatalake12"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/youtube_stream/bronze"

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/youtube_stream/silver"

silver_checkpoint = f"abfss://silver@{storage_account}.dfs.core.windows.net/youtube_stream/checkpoints/silver"

# ====================================
# Read Bronze stream
# ====================================

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# ==================================
# Cleaning and deduplicaiton
# ==================================

from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    lower,
    trim
)

silver_df = (
    bronze_stream
    
    # Remove records without video_id
    .filter(col("video_id").isNotNull())
    
    # Clean title text
    .withColumn(
        "title",
        trim(lower(col("title")))
    )
    
    # Clean channel name
    .withColumn(
        "channel",
        trim(col("channel"))
    )
    
    # Convert timestamps
    .withColumn(
        "published_at",
        to_timestamp("published_at")
    )
    
    .withColumn(
        "ingestion_time",
        to_timestamp("ingestion_time")
    )
    
    # Ensure numeric fields
    .withColumn(
        "views",
        col("views").cast("long")
    )
    
    .withColumn(
        "likes",
        col("likes").cast("long")
    )
    
    # Remove impossible values
    .filter(col("views") >= 0)
    .filter(col("likes") >= 0)
    
    # Add processing timestamp
    .withColumn(
        "processed_time",
        current_timestamp()
    )
    
    # Watermark for streaming dedupe
    .withWatermark("published_at", "1 hour")
    
    # Remove duplicates
    .dropDuplicates(["video_id"])
)

# ==================================
# Write to silver Delta tables
# ==================================
silver_query = (
    silver_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .start(silver_path)
)

silver_query.awaitTermination()

#Registering Silver_videos table
%sql
CREATE TABLE IF NOT EXISTS youtube_analytics.silver_videos
USING DELTA
LOCATION 'abfss://silver@youtubedatalake12.dfs.core.windows.net/youtube_stream/silver';