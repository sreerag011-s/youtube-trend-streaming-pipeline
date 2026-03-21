# Install Great expectation for validation
%pip install great-expectations

# Load silver table
silver_df = spark.read.table("youtube_analytics.silver_videos")

pandas_df = silver_df.toPandas()

# Initialize Great expectaiton

import great_expectations as gx

validator = gx.from_pandas(pandas_df)

# Validation checks
validation_results = []

validation_results.append(
    validator.expect_column_values_to_not_be_null("video_id")
)

validation_results.append(
    validator.expect_column_values_to_not_be_null("title")
)

validation_results.append(
    validator.expect_column_values_to_be_between(
        "views",
        min_value=0
    )
)

validation_results.append(
    validator.expect_column_values_to_be_between(
        "likes",
        min_value=0
    )
)

validation_results.append(
    validator.expect_column_values_to_not_be_null("published_at")
)
# Results of Validation checks

for check in validation_results:
    print(check)

# Creating path for Qurantine table
quarantine_path = "abfss://silver@youtubedatalake12.dfs.core.windows.net/youtube_stream/quarantine"

#Registering Quarantine table
%sql
CREATE TABLE IF NOT EXISTS youtube_analytics.quarantine_videos
USING DELTA
LOCATION 'abfss://silver@youtubedatalake12.dfs.core.windows.net/youtube_stream/quarantine';

# Validaiton conditions
from pyspark.sql.functions import col

validation_condition = (
    col("video_id").isNotNull() &
    col("title").isNotNull() &
    col("views") >= 0 &
    col("likes") >= 0 &
    col("published_at").isNotNull()
)

# Valid_records
valid_df = silver_df.filter(validation_condition)

# Invalid records
invalid_df = from pyspark.sql.functions import when

invalid_df = silver_df.withColumn(
    "validation_error",
    when(col("video_id").isNull(), "missing_video_id")
    .when(col("title").isNull(), "missing_title")
    .when(col("views") < 0, "negative_views")
    .when(col("likes") < 0, "negative_likes")
    .when(col("published_at").isNull(), "missing_published_at")
)
.filter(~validation_condition)


# Writing invalid records to Quarantine tables

invalid_df.write.format("delta").mode("append").save(quarantine_path)

# Writing valid records to Gold layer
valid_df.write.format("delta").mode("overwrite").saveAsTable(
    "youtube_analytics.valid_silver_videos"
)

# Validaiton conditions
from pyspark.sql.functions import col

validation_condition = (
    col("video_id").isNotNull() &
    col("title").isNotNull() &
    col("views") >= 0 &
    col("likes") >= 0 &
    col("published_at").isNotNull()
)

# Valid_records
valid_df = silver_df.filter(validation_condition)

# Invalid records
invalid_df = from pyspark.sql.functions import when

invalid_df = silver_df.withColumn(
    "validation_error",
    when(col("video_id").isNull(), "missing_video_id")
    .when(col("title").isNull(), "missing_title")
    .when(col("views") < 0, "negative_views")
    .when(col("likes") < 0, "negative_likes")
    .when(col("published_at").isNull(), "missing_published_at")
)
.filter(~validation_condition)


# Writing invalid records to Quarantine tables

invalid_df.write.format("delta").mode("append").save(quarantine_path)

# Writing valid records to Gold layer
valid_df.write.format("delta").mode("overwrite").saveAsTable(
    "youtube_analytics.valid_silver_videos"
)