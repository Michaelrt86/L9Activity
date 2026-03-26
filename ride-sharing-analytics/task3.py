# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics-3").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
df = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

# Parse JSON data into columns using the defined schema
parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
from pyspark.sql.functions import to_timestamp
parsed_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))
parsed_df = parsed_df.withWatermark("timestamp", "10 minutes")

from pyspark.sql.functions import window

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_df = parsed_df.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("window_total_fare")
)

# Extract window start and end times as separate columns
result_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("window_total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included
    batch_df.write.csv(f"output/task-3/batch_{batch_id}", header=True, mode="overwrite")

# Use foreachBatch to apply the function to each micro-batch
query = result_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoint/task-3") \
    .start()

query.awaitTermination()