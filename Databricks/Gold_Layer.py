# Spark_config

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


#Storage Patha for Gold tables

storage_account = "youtubedatalake12"

gold_trending_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/youtube_stream/gold_trending"

gold_keyword_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/youtube_stream/gold_keywords"

gold_hashtag_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/youtube_stream/gold_hashtags"

#Import nessecary functions

from pyspark.sql.functions import (
    col,
    current_timestamp,
    unix_timestamp,
    lower,
    explode,
    concat,
    lit,
    log,
    when
)

from keybert import KeyBERT

# Loading silver_df

silver_df = spark.read.table("youtube_analytics.valid_silver_videos")

# Metrics to start with

engagement_rate = col("likes") / col("views")

hours_since_publish = (
    unix_timestamp(current_timestamp()) -
    unix_timestamp(col("published_at"))
) / 3600

view_velocity = col("views") / hours_since_publish

trend_score = (view_velocity * 0.6) + (engagement_rate * 0.4)

#Compute Time Differences
hours_since_publish = (
    unix_timestamp(current_timestamp()) -
    unix_timestamp(col("published_at"))
) / 3600

#Time buckets
time_bucket = when(hours_since_publish <= 1, "last_1h") \
    .when(hours_since_publish <= 6, "last_6h") \
    .when(hours_since_publish <= 24, "last_24h") \
    .otherwise("older")

# Table 1
gold_trending_df = (
    silver_df
    .withColumn("engagement_rate", col("likes") / col("views"))
    .withColumn("hours_since_publish", hours_since_publish)
    .withColumn("view_velocity", col("views") / hours_since_publish)
    .withColumn("log_views", log(col("views") + 1))
    .withColumn("time_bucket", time_bucket)
    .withColumn(
        "trend_score",
        (col("view_velocity") * 0.5) +
        (col("engagement_rate") * 0.3) +
        (col("log_views") * 0.2)
    )
)

# Key word extractions

kw_model = KeyBERT()

titles = silver_df.select("title").toPandas()

# Extract keywords
keyword_list = []

for title in titles["title"]:
    keywords = kw_model.extract_keywords(title, top_n=3)
    for kw, score in keywords:
        keyword_list.append(kw)

keywords_df = spark.createDataFrame(
    [(k,) for k in keyword_list],
    ["keyword"]
)

from pyspark.sql.functions import monotonically_increasing_id

silver_with_id = silver_df.withColumn("id", monotonically_increasing_id())

titles = silver_df.select("video_id", "title").toPandas()

keyword_rows = []

for _, row in titles.iterrows():
    kws = kw_model.extract_keywords(row["title"], top_n=3)
    for kw, _ in kws:
        keyword_rows.append((row["video_id"], kw))

keyword_df = spark.createDataFrame(keyword_rows, ["video_id", "keyword"])

keyword_metrics_df = keyword_df.join(gold_trending_df, "video_id")

#Table 2

gold_keywords_df = (
    keyword_metrics_df
    .groupBy("keyword")
    .agg(
        {"views": "avg", "engagement_rate": "avg"}
    )
    .withColumnRenamed("avg(views)", "avg_views")
    .withColumnRenamed("avg(engagement_rate)", "avg_engagement")
    .orderBy(col("avg_views").desc())
)

#Table 3
gold_hashtags_df = gold_keywords_df.withColumn(
    "hashtag",
    concat(lit("#"), col("keyword"))
)

#Writing to ADLS GEN2

#Trending Videos
gold_trending_df.write.format("delta").mode("overwrite").saveAsTable(
    "youtube_analytics.gold_trending_videos"
)

#Keywords table
gold_trending_df.write.format("delta").mode("overwrite").saveAsTable(
    "youtube_analytics.gold_trending_videos"
)
#Hashtags
gold_hashtags_df.write.format("delta").mode("overwrite").saveAsTable(
    "youtube_analytics.gold_hashtags"
)