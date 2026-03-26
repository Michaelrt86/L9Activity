from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics-2").getOrCreate()

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

# Convert timestamp column to TimestampType
from pyspark.sql.functions import to_timestamp
parsed_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))

# Add watermark for late data handling (e.g., 10 minutes delay)
parsed_df = parsed_df.withWatermark("timestamp", "10 minutes")

# Compute aggregations: total fare and average distance grouped by driver_id
agg_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file
def write_batch(batch_df, batch_id):

# Save the batch DataFrame as a CSV file with the batch ID in the filename
    batch_df.write.csv(f"output/task-2/batch_{batch_id}", header=True, mode="overwrite")

# Use foreachBatch to apply the function to each micro-batch
query = agg_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoint/task-2") \
    .start()

query.awaitTermination()