from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("SortData") \
    .getOrCreate()

# Define dummy input data
dummy_data = [
    ("3", "Wolf"),
    ("1", "Elephant"),
    ("2", "Lion"),
    ("4", "Tiger")
]

# Create DataFrame from dummy data
df = spark.createDataFrame(dummy_data, ["id", "animal"])

# Sort the DataFrame
sorted_df = df.orderBy(col("id"))

# Show the sorted DataFrame
sorted_df.show()

# Stop the Spark session
spark.stop()
