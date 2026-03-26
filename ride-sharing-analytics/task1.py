from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics-1").getOrCreate()

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

# Print parsed data to the CSV files
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("header", "true") \
    .option("path", "output/task-1") \
    .option("checkpointLocation", "checkpoint/task-1") \
    .start()

query.awaitTermination()