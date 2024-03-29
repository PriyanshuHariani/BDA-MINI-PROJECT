from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("JoinExample") \
    .getOrCreate()

# Create DataFrames for left and right datasets
left_data = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value"])
right_data = spark.createDataFrame([(1, "X"), (2, "Y"), (4, "Z")], ["id", "value"])

# Perform map-side join
map_join = left_data.join(right_data, on="id", how="inner")

# Perform reduce-side join
reduce_join = left_data.union(right_data).groupBy("id").agg({"value": "collect_list"})

# Show the results
print("Map Side Join:")
map_join.show()

print("Reduce Side Join:")
reduce_join.show()

# Stop the Spark session
spark.stop()
