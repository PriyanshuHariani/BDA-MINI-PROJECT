from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, sum as sql_sum, stddev

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("AggregationSpark") \
    .getOrCreate()

# Dummy input data
input_data = [
    ('key1', 10),
    ('key2', 20),
    ('key1', 30),
    ('key2', 40),
    ('key1', 50),
    ('key2', 60)
]

# Create DataFrame from input data
df = spark.createDataFrame(input_data, ["key", "value"])

# Perform aggregation using DataFrame API
result_df = df.groupBy("key") \
    .agg(
        mean("value").alias("mean"),
        sql_sum("value").alias("sum"),
        stddev("value").alias("std_dev")
    )

# Show the result
result_df.show()

# Stop SparkSession
spark.stop()
